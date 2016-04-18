
package org.apache.atlas.services;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.persistence.Id;
import org.elasticsearch.common.collect.ImmutableList;

/**
 * Utility class used to by the JSON Imported.  Represents a node in a dependency
 * tree created from the objects that were deserialized from json.  
 * 
 * @see JSONImporter
 * 
 * @author jeff
 *
 */
public class DependencyTreeNode {
    
    //instance deserialized from json.  
    private ITypedReferenceableInstance instance_;
    
    //new guid assigned when the object was saved into Atlas.
    private String newGuid_;
    
    //guid from the loaded json
    private Id oldId_;
    
    //nodes for objects that reference the object in this node
    private Collection<DependencyTreeNode> parents_ = new ArrayList<DependencyTreeNode>();    

    
    //records the nodes objects referenced by this object, organized by
    //the name of the reference.  The key is the name of the reference.
    private Map<String,Collection<DependencyTreeNode>> referenceValues_ =
            new HashMap<String,Collection<DependencyTreeNode>>();
    
    
    public DependencyTreeNode(ITypedReferenceableInstance instance) {
        instance_ = instance;
        //save the original id, since it will get reset before
        //the import starts.
        oldId_ = instance.getId();
    }
    
    public void setNewGuid(String guid) {
        newGuid_ = guid;
        
    }
    
    /**
     * Returns true if the instance for this node was added into Atlas (and therefore has
     * a new guid assigned).
     * @return
     */
    public boolean wasImported() {
        return newGuid_ != null;
    }

    /**
     * Returns true if this node has no unresolved dependencies.
     * 
     * @return
     */
    public boolean isLeaf() {
        for(Collection<DependencyTreeNode> referenceChildren : referenceValues_.values()) {
            for(DependencyTreeNode referenceChild : referenceChildren) {
                if(! referenceChild.wasImported()) {
                    return false;
                }
            }
        }
        return true;
    }

    public void addChild(String referenceName, DependencyTreeNode refdNode) {
        refdNode.addParent(this);
        Collection<DependencyTreeNode> referencedNodes = referenceValues_.get(referenceName);
        if(referencedNodes == null) {
            referencedNodes = new ArrayList<DependencyTreeNode>();
            referenceValues_.put(referenceName, referencedNodes);
        }
        referencedNodes.add(refdNode);
    }
    
    public Id getNewId() {
        return new Id(newGuid_, oldId_.getVersion(), oldId_.getClassName());
    }
    
    /**
     * TBD - should this be moved to JSONImporter?
     * 
     * Updates the reference attributes in the underlying ITypedReferenceableInstance
     * to use the guids that were assigned by Atlas.  This throws an exception if
     * any of the referenced objects do not have a new guid.
     * 
     * @throws AtlasException
     */
    public void updateReferencedGuids() throws AtlasException {
        
        //call set with new ImmutableList for each reference
        //using resolved rids
        if(! isLeaf()) {
            throw new AtlasException("Not all the referenced objects have been saved!");
        }
        
        for(Map.Entry<String, Collection<DependencyTreeNode>> entry : referenceValues_.entrySet()) {
            String refName = entry.getKey();
            Collection<DependencyTreeNode> values = entry.getValue();               
            Object newReferenceValue = determineNewReferenceValue(refName, values);                
            instance_.set(refName, newReferenceValue);
        }
    }

    /**
     * Before adding objects to Atlas, we need to update the references to use
     * the GUIDs created by Atlas rather than the GUIDs from the json.  We do
     * this by calling IInstance.set() with an updated value of the field.
     * This method determines what that updated value should be for the
     * field (reference) with the given name.
     * 
     * @param refName the name of the field to update
     * @param values dependency tree nodes of objects that are referenced
     * @return
     * @throws AtlasException
     */
    private Object determineNewReferenceValue(String refName, Collection<DependencyTreeNode> values)
            throws AtlasException {
        
        boolean collectionNeeded = instance_.get(refName) instanceof Collection;
        Object newReferenceValue = null;
        if(collectionNeeded) {
            ImmutableList.Builder<Id> builder =  ImmutableList.builder();
            for(DependencyTreeNode referencedValue : values) {
                builder.add(referencedValue.getNewId());                        
            }                    
            
            newReferenceValue = builder.build();                                        
        }
        else {
            if(values.isEmpty()) {
                newReferenceValue = null;
            }
            else {
                DependencyTreeNode node = values.iterator().next();
                newReferenceValue = node.getNewId();
            }                     
        }
        return newReferenceValue;
    }

    private void addParent(DependencyTreeNode dependencyTreeNode) {
       parents_.add(dependencyTreeNode);            
    }
    
    public Collection<DependencyTreeNode> getParents() {
        return Collections.unmodifiableCollection(parents_);
    }

    public ITypedReferenceableInstance getInstance() {

        return instance_;
    }
}