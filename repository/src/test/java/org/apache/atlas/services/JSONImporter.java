/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes.ArrayType;
import org.apache.atlas.typesystem.types.DataTypes.MapType;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;

//imports all instances in a JSON file into graph.  Reassigns guid.  Assumes that there
//are no circular dependencies.
public class JSONImporter {

    private TypeSystem typeSystem_;

    private Map<String,DependencyTreeNode> instanceNodes_ = new HashMap<String, DependencyTreeNode>();

    //unimported nodes with no unresolved dependencies.  Updated in
    //waves
    private List<DependencyTreeNode> leafNodes_ = new ArrayList<DependencyTreeNode>();



    public JSONImporter(TypeSystem typeSystem, String json) throws AtlasException {
        typeSystem_ = typeSystem;
        ITypedReferenceableInstance[] instancesFromJson = deserializeClassInstances(json);

        //create dependency tree nodes (with no children) for all instances
        for(ITypedReferenceableInstance instance : instancesFromJson) {
            DependencyTreeNode node = new DependencyTreeNode(instance);
            instanceNodes_.put(instance.getId().id, node);
        }

        //determine the dependencies between the dependency tree nodes
        //based on the references in the underlying instance.
        //For each node, adds the DependendencyTreeNodes for the referenced objects
        //as children.  Any nodes with no referenced objects become part
        //of the initial set of leaf nodes.
        for(DependencyTreeNode node : instanceNodes_.values()) {
            findAndAddChildren(typeSystem, node);
            if(node.isLeaf()) {
                leafNodes_.add(node);
            }
        }

        //unassign the guids so that Atlas will create new instances
        //rather than trying to update instances.
        for(DependencyTreeNode node : instanceNodes_.values()) {
            IReferenceableInstance inst = node.getInstance();
            ((ReferenceableInstance)inst).replaceWithNewId(new Id(inst.getTypeName()));
        }
    }


    private void findAndAddChildren(TypeSystem typeSystem, DependencyTreeNode node) throws AtlasException {
        IReferenceableInstance inst = node.getInstance();
        ClassType entityType = typeSystem.getDataType(ClassType.class, inst.getTypeName());

        for(Map.Entry<String,Object> valueMapEntry: inst.getValuesMap().entrySet()) {
            String key = valueMapEntry.getKey();
            Object value = valueMapEntry.getValue();
            boolean isReference = isReference(entityType, key);
            if(! isReference) {
                continue;
            }
            Collection<Object> referencedValues;
            //TBD: map handling
            if(value instanceof Collection) {
                referencedValues = (Collection<Object>)value;

            }
            else {
                referencedValues = Collections.singleton(value);
            }

            for(Object referencedValue : referencedValues) {
                if(referencedValue instanceof IReferenceableInstance) {
                    IReferenceableInstance refdInst = (IReferenceableInstance)referencedValue;
                    DependencyTreeNode refdNode = instanceNodes_.get(refdInst.getId().id);
                    if(refdNode == null) {
                        throw new AtlasException(inst.getId() + " refers to the non existent entity " + refdInst.getId());
                    }
                    node.addChild(key, refdNode);
                }

            }
        }
    }


    private boolean isReference(ClassType entityType, String key) {
        AttributeInfo info =  entityType.fieldMapping().fields.get(key);
        return isReference(info.dataType());
    }

    private boolean isReference(IDataType< ?> dt) {

        //structs don't have guids.  Struct instances are always inlined in the object they
        //belong to
        if( dt.getTypeCategory() == TypeCategory.CLASS) {
            return true;
        }
        if(dt.getTypeCategory() == TypeCategory.ARRAY) {
            ArrayType at = (ArrayType)dt;
            return isReference(at.getElemType());
        }
        if(dt.getTypeCategory() == TypeCategory.MAP) {
            //map keys must be strings
            MapType mt = (MapType)dt;
            return isReference(mt.getValueType());
        }
        return false;
    }


    public void doImport(MetadataRepository repo) throws AtlasException {

        //do the import in passes.  In each pass, we save the objects in
        //Atlas that only have dependencies on objects that were previously
        //saved in Atlas by the import.  We start with the objects that
        //have no dependencies at all.  Then we check the objects
        //that depend on those objects and see if they are now able
        //to be imported.  In pass two, we save all of those objects.
        //We repeat this process until all of the objects have been imported.

        //Note that this was designed to be able to import the json corresponding
        //to the HiveTitanSample.  It is not able to handle graphs
        //that have cycles.  It could be extended to do this if we
        //want to make use of this class in other scenarios.  To do this,
        //we would need to to use some algorithm (perhaps minimum spanning tree, or ideally
        //some algorithm that only removes the minumum number of references to turn the graph into a tree)
        //to turn the graph into a tree.  We would need to record the references
        //that were removed in order to do this somewhere else.  We would first
        //save the objects with only the references that are in the tree
        //we computed.  Then, as a second step, we would go though and
        //update the objects with the remaining references.

        while(! leafNodes_.isEmpty()) {

            importLeafNodes(repo);

            leafNodes_ = determineNewLeafNodes();
        }

    }


    private  List<DependencyTreeNode> determineNewLeafNodes() {

        Set<DependencyTreeNode> newLeafNodes = new HashSet<DependencyTreeNode>();
        for(DependencyTreeNode node : leafNodes_) {
            for(DependencyTreeNode leafParent : node.getParents()) {
                if(leafParent.isLeaf() && ! leafParent.wasImported()) {
                    newLeafNodes.add(leafParent);
                }
            }
        }
        return new ArrayList<DependencyTreeNode>(newLeafNodes);
    }


    private void importLeafNodes(MetadataRepository repo)
            throws AtlasException, RepositoryException, EntityExistsException {

        ITypedReferenceableInstance[] toImport = new ITypedReferenceableInstance[leafNodes_.size()];
        int idx = 0;
        for(DependencyTreeNode node : leafNodes_) {
            node.updateReferencedGuids();
            toImport[idx++] = node.getInstance();
        }

        //each call to createEntities adds stuff to the context, and as a result calling
        //it multiple times without resetting the context causes it to produce incorrect
        //results.  To avoid this, all calls need to take place within a fresh context.
        //unfortunatly there is currently not a way to save and restore the current
        //context.  Since this is just test code, we'll just destroy the one
        //that's there.  In production we should save/restore the context
        //or call a different api that does not modify the context
        RequestContext.createContext();
        List<String> guids = repo.createEntities(toImport);


        //set the new guids in the DependencyTreeNodes
        //using the values we got back from Atlas.
        for(int i = 0; i < guids.size(); i++) {
            leafNodes_.get(i).setNewGuid(guids.get(i));
        }
    }

    private ITypedReferenceableInstance[] deserializeClassInstances(String entityInstanceDefinition)
            throws AtlasException {
        return GraphHelper.deserializeClassInstances(typeSystem_, entityInstanceDefinition);

    }

}
