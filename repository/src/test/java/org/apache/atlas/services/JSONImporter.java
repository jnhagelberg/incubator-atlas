
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
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.exception.EntityExistsException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.utils.ParamChecker;
import org.codehaus.jettison.json.JSONArray;

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
                if(value instanceof Collection) {
                    referencedValues = (Collection<Object>)value;
                    
                }
                else {
                    referencedValues = Collections.singleton(value);
                }
                
                boolean hasReferences = false;
                for(Object referencedValue : referencedValues) {
                    if(referencedValue instanceof IReferenceableInstance) {
                        hasReferences=true;
                        IReferenceableInstance refdInst = (IReferenceableInstance)referencedValue;
                        DependencyTreeNode refdNode = instanceNodes_.get(refdInst.getId().id);
                        if(refdNode == null) {      
                            throw new AtlasException(inst.getId() + " refers to the non existent entity " + refdInst.getId());
                        }
                        node.addChild(key, refdNode);
                    }
                    
                }
                if(! hasReferences) {
                    leafNodes_.add(node);
                }
            }
        }

        //unassign the guids so that Atlas will create new instances
        //rather than trying to update instances.
        for(DependencyTreeNode node : instanceNodes_.values()) {
            IReferenceableInstance inst = node.getInstance();
            ((ReferenceableInstance)inst).replaceWithNewId(new Id(inst.getTypeName()));
        }
    }


    private boolean isReference(ClassType entityType, String key) {
        AttributeInfo info =  entityType.fieldMapping().fields.get(key);
        IDataType<?> dt = info.dataType();
        //structs don't have guids.  Struct instances are always inlined in the object they
        //belong to
        return dt.getTypeCategory() == TypeCategory.CLASS;
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
            
            leafNodes_ = determinNewLeafNodes();
        }
        
    }


    private  List<DependencyTreeNode> determinNewLeafNodes() {
        
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
        List<String> guids = repo.createEntities(toImport);
        
        //set the new guids in the DependencyTreeNodes
        //using the values we got back from Atlas.
        for(int i = 0; i < guids.size(); i++) {
            leafNodes_.get(i).setNewGuid(guids.get(i));
        }
    }
    
    //"borrowed" from ClassType.  TODO: move method there to a utiltiy class, delete this
    private ITypedReferenceableInstance[] deserializeClassInstances(String entityInstanceDefinition)
            throws AtlasException {
        try {
            JSONArray referableInstances = new JSONArray(entityInstanceDefinition);
            ITypedReferenceableInstance[] instances = new ITypedReferenceableInstance[referableInstances.length()];
            for (int index = 0; index < referableInstances.length(); index++) {
                Referenceable entityInstance =
                        InstanceSerialization.fromJsonReferenceable(referableInstances.getString(index), true);
                final String entityTypeName = entityInstance.getTypeName();
                ParamChecker.notEmpty(entityTypeName, "Entity type cannot be null");

                ClassType entityType = typeSystem_.getDataType(ClassType.class, entityTypeName);

                //Both assigned id and values are required for full update
                //classtype.convert() will remove values if id is assigned. So, set temp id, convert and
                // then replace with original id
                Id origId = entityInstance.getId();
                entityInstance.replaceWithNewId(new Id(entityInstance.getTypeName()));
                ITypedReferenceableInstance typedInstrance = entityType.convert(entityInstance, Multiplicity.REQUIRED);
                ((ReferenceableInstance)typedInstrance).replaceWithNewId(origId);
                instances[index] = typedInstrance;
            }
            return instances;
        } catch(ValueConversionException | TypeNotFoundException  e) {
            throw e;
        } catch (Exception e) {  // exception from deserializer
            System.err.println("Unable to deserialize json=" + entityInstanceDefinition);
            e.printStackTrace();
            throw new IllegalArgumentException("Unable to deserialize json", e);
        }
    }
    


}
