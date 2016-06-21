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
package org.apache.atlas.repository.graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.persistence.Id.EntityState;

/**
 * Accumulates the changes that are needed to perform a delete, applies them all
 * at once, so that the underlying graph system can batch the updates. It also
 * prevents any actual updates of the graph from happening until all of the
 * processing has taken place. This makes the processing more robust in the face
 * of non-ACID transactions. We won't even attempt any graph updates if
 * processing errors occur.
 *
 */
public class DeleteContext {

    private GraphHelper graphHelper_;

    // maintain a list of the actions so that the operations get applied
    // in the same order as they came in at
    private List<DeleteAction> deleteActions_ = new ArrayList<DeleteAction>(); 
    private Set<AtlasVertex> processedVertices_ = new HashSet<AtlasVertex>();
    private Map<AtlasElement, UpdatedElement> updateElements_ = new HashMap<>();
    
    public DeleteContext(GraphHelper helper) {
        graphHelper_ = helper;
    }

    /**
     * Records that the given element has been soft deleted so
     * that is is treated as deleted by the delete context.
     *
     * @param element
     */
    public void softDeleteElement(AtlasElement element) {
        getOrCreateUpdatedElement(element, true).delete();
    }

    /**
     * Records that the specified Vertex should be deleted.  It will be deleted
     * when commitDelete() is called.
     *
     * @param vertex The vertex to delete.
     */
    public void removeVertex(AtlasVertex vertex) {

        if (isDeleted(vertex)) {
            throw new IllegalStateException("Cannot delete a vertex that has already been deleted");
        }
        deleteActions_.add(new VertexRemoval(vertex));
        getOrCreateUpdatedElement(vertex).delete();
    }
    
    /**
     * Gets the value of the specified property on the given element, taking into
     * account change that have been applied to the DeleteContext but have not
     * yet been committed into the graph.
     * 
     * @param element
     * @param property
     * @param clazz
     * @return
     */
    public <T> T getProperty(AtlasElement element, String property, Class<T> clazz) {
        return getUpdatedElement(element).getProperty(property, clazz);
    }
    
    /**
     * Gets the value of the specified property on the given element, taking into
     * account change that have been applied to the DeleteContext but have not
     * yet been committed into the graph.
     * 
     * @param element
     * @param property
     * @return
     */
    public List<String> getListProperty(AtlasElement element, String property) throws AtlasException {
        return getUpdatedElement(element).getListProperty(property);
    }
    
    /**
    * Records that the specified Edge should be deleted.  It will be deleted
    * when commitDelete() is called.
    *
    * @param vertex The vertex to delete.
    */
    public void removeEdge(AtlasEdge edge) {
        if (isDeleted(edge)) {
            throw new IllegalStateException("Cannot delete an edge that has already been deleted");
        }
        deleteActions_.add(new EdgeRemoval(edge));
        getOrCreateUpdatedElement(edge).delete();
    }

    /**
     * Records that a property needs to be set in an Element.  The change will take place
     * when commitDelete() is called.
     *
     * @param element the element to update
     * @param name the name of the property to set
     * @param value the value to set the property to
     */
    public void setProperty(AtlasElement element, String name, Object value) {
        if (isDeleted(element)) {
            throw new IllegalStateException("Cannot update an element that has been deleted.");
        }
        deleteActions_.add(new PropertyUpdate(element, name, value));
        getOrCreateUpdatedElement(element).setProperty(name, value);
    }

    /**
     * Applies all of the acccumulated changes to the graph.
     *
     */
    public void commitDelete() {
        for (DeleteAction action : deleteActions_) {
            action.perform(graphHelper_);
        }
        deleteActions_.clear();
        updateElements_.clear();
        processedVertices_.clear();
    }

    /**
    * Returns true if either:
    *
    *  1) the given vertex has been previously processed by the delete algoritm or
    *  2) the given element has been deleted, either through the DeleteContext or previously through the soft
    * delete mechanism.
    *
    * @param element
    * @return
    */
    public boolean isProcessedOrDeleted(AtlasVertex vertex) {
        return isProcessed(vertex) || ! isActive(vertex);
    }

    /**
     * Returns true if the given element has not been deleted, either
     * through the DeleteContext or previously through the soft
     * delete mechanism.
     *
     * @param element
     * @return
     */
    public boolean isActive(AtlasElement element) {
        
        EntityState state = GraphHelper.getState(element);
        return state == EntityState.ACTIVE && !isDeleted(element);
    }
    /**
     * Returns true if the given Vertex has been previsouly processed
     * by the delete algorithm.
     *
     * @param vertex
     * @return
     */
    public boolean isProcessed(AtlasVertex vertex) {
        return processedVertices_.contains(vertex);
    }
    /**
     * Records that a given Vertex has been processed by the delete algorithm.
     *
     * @param vertex
     */
    public void addProcessedVertex(AtlasVertex vertex) {
        processedVertices_.add(vertex);
    }

    private boolean isDeleted(AtlasElement instanceVertex) {
        return getUpdatedElement(instanceVertex).isDeleted();
    }
    
    private UpdatedElement getOrCreateUpdatedElement(AtlasElement element, boolean updateCache) {
        
        UpdatedElement result = updateElements_.get(element);
        if(result == null) {
            result = new UpdatedElement(element);
            if(updateCache) {
                updateElements_.put(element, result);
            }
        }
        return result;
    }
    
    private UpdatedElement getUpdatedElement(AtlasElement element) {
       return getOrCreateUpdatedElement(element, false);        
    }

    private UpdatedElement getOrCreateUpdatedElement(AtlasElement element) {
        return getOrCreateUpdatedElement(element, true);        
    }

    /**
     * Interface for delete actions that are accumulated by this
     * class to be executed later.
     *
     */
    private static interface DeleteAction {
        void perform(GraphHelper helper);
    }

    private static class PropertyUpdate implements DeleteAction {

        private AtlasElement element_;
        private String property_;
        private Object newValue_;

        public PropertyUpdate(AtlasElement element_, String property_, Object newValue_) {
            super();
            this.element_ = element_;
            this.property_ = property_;
            this.newValue_ = newValue_;
        }

        @Override
        public void perform(GraphHelper helper) {
            GraphHelper.setProperty(element_, property_, newValue_);
        }
    }

    private static class VertexRemoval implements DeleteAction {
        private AtlasVertex toDelete_;

        public VertexRemoval(AtlasVertex toDelete) {
            super();
            this.toDelete_ = toDelete;
        }

        @Override
        public void perform(GraphHelper helper) {
            helper.removeVertex(toDelete_);
        }
    }

    private static class EdgeRemoval implements DeleteAction {

        private AtlasEdge toDelete_;

        public EdgeRemoval(AtlasEdge toDelete) {
            super();
            this.toDelete_ = toDelete;
        }

        @Override
        public void perform(GraphHelper helper) {
            helper.removeEdge(toDelete_);
        }
    }
    
    private static class UpdatedElement {
        
        private AtlasElement wrapped_;
        private boolean deleted_ = false;
        private Map<String,Object> propertyChanges_ = new HashMap<String,Object>();
        
        public UpdatedElement(AtlasElement element) {
            wrapped_ = element;
        }
        
        public void setProperty(String key, Object value) {
            propertyChanges_.put(key, value);
        }

        public <T> T getProperty(String key, Class<T> clazz) {
            if(propertyChanges_.containsKey(key)) {
                return (T)propertyChanges_.get(key);
            }
            return wrapped_.getProperty(key, clazz);
        }
        
        public void delete() {
            deleted_ = true;
            propertyChanges_.clear();
        }
        
        public boolean isDeleted() {
            return deleted_;
        }
        
        public List<String> getListProperty(String key) throws AtlasException {
            
            if(propertyChanges_.containsKey(key)) {
                return (List<String>)propertyChanges_.get(key);
            }
            return wrapped_.getListProperty(key);
        }
    }   

}
