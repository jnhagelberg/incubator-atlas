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
    private Map<AtlasElement, UpdatedAtlasElement> updatedAtlasElements_ = new HashMap<>();

    public DeleteContext(GraphHelper helper) {
        graphHelper_ = helper;
    }

    /**
     * Records that the given AtlasElement has been soft deleted so
     * that is is treated as deleted by the delete context.
     *
     * @param AtlasElement
     */
    public void softDeleteAtlasElement(AtlasElement AtlasElement) {
        getUpdatedElement(AtlasElement).delete();
    }

    /**
     * Records that the specified AtlasVertex should be deleted.  It will be deleted
     * when commitDelete() is called.
     *
     * @param AtlasVertex The AtlasVertex to delete.
     */
    public void removeAtlasVertex(AtlasVertex AtlasVertex) {

        if (isDeleted(AtlasVertex)) {
            throw new IllegalStateException("Cannot delete a AtlasVertex that has already been deleted");
        }
        deleteActions_.add(new AtlasVertexRemoval(AtlasVertex));
        getUpdatedElement(AtlasVertex).delete();
    }

    /**
    * Records that the specified AtlasEdge should be deleted.  It will be deleted
    * when commitDelete() is called.
    *
    * @param AtlasVertex The AtlasVertex to delete.
    */
    public void removeAtlasEdge(AtlasEdge AtlasEdge) {
        if (isDeleted(AtlasEdge)) {
            throw new IllegalStateException("Cannot delete an AtlasEdge that has already been deleted");
        }
        deleteActions_.add(new AtlasEdgeRemoval(AtlasEdge));
        getUpdatedElement(AtlasEdge).delete();
    }

    /**
     * Records that a property needs to be set in an AtlasElement.  The change will take place
     * when commitDelete() is called.
     *
     * @param AtlasElement the AtlasElement to update
     * @param name the name of the property to set
     * @param value the value to set the property to
     */
    public void setProperty(AtlasElement AtlasElement, String name, Object value) {
        if (isDeleted(AtlasElement)) {
            throw new IllegalStateException("Cannot update an AtlasElement that has been deleted.");
        }
        deleteActions_.add(new PropertyUpdate(AtlasElement, name, value));
        getUpdatedElement(AtlasElement).setProperty(name, value);
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
        updatedAtlasElements_.clear();
        processedVertices_.clear();
    }

    /**
     * Gets the value of the specified property on the given AtlasElement, taking into
     * account change that have been applied to the DeleteContext but have not
     * yet been committed into the graph.
     *
     * @param AtlasElement
     * @param property
     * @param clazz
     * @return
     */
    public <T> T getProperty(AtlasElement AtlasElement, String property, Class<T> clazz) {
        return getReadOnlyUpdatedElement(AtlasElement).getProperty(property, clazz);
    }

    /**
     * Gets the value of the specified property on the given AtlasElement, taking into
     * account change that have been applied to the DeleteContext but have not
     * yet been committed into the graph.
     *
     * @param AtlasElement
     * @param property
     * @param clazz
     * @return
     * @throws AtlasException 
     */
    public List<String> getListProperty(AtlasElement AtlasElement, String property) throws AtlasException {
        return getReadOnlyUpdatedElement(AtlasElement).getListProperty(property);
    }
    
    
    /**
    * Returns true if either:
    *
    *  1) the given AtlasVertex has been previously processed by the delete algoritm or
    *  2) the given AtlasElement has been deleted, either through the DeleteContext or previously through the soft
    * delete mechanism.
    *
    * @param AtlasElement
    * @return
    */
    public boolean isProcessedOrDeleted(AtlasVertex AtlasVertex) {
        return isProcessed(AtlasVertex) || ! isActive(AtlasVertex);
    }

    /**
     * Returns true if the given AtlasElement has not been deleted, either
     * through the DeleteContext or previously through the soft
     * delete mechanism.
     *
     * @param AtlasElement
     * @return
     */
    public boolean isActive(AtlasElement AtlasElement) {

        EntityState state = GraphHelper.getState(AtlasElement);
        return state == EntityState.ACTIVE && !isDeleted(AtlasElement);
    }

    private boolean isDeleted(AtlasElement instanceAtlasVertex) {
        return getReadOnlyUpdatedElement(instanceAtlasVertex).isDeleted();
    }

    /**
     * Returns true if the given AtlasVertex has been previously processed
     * by the delete algorithm.
     *
     * @param AtlasVertex
     * @return
     */
    public boolean isProcessed(AtlasVertex AtlasVertex) {
        return processedVertices_.contains(AtlasVertex);
    }
    /**
     * Records that a given AtlasVertex has been processed by the delete algorithm.
     *
     * @param AtlasVertex
     */
    public void addProcessedAtlasVertex(AtlasVertex AtlasVertex) {
        processedVertices_.add(AtlasVertex);
    }


    /**
     * This returns an UpdatedAtlasElement that corresponds to the given AtlasElement.  If there are no changes
     * to the given AtlasElement, a temporary UpdatedAtlasElement is created (but not cached).  No changes should be applied
     * to UpdatedAtlasElements returned here, since they may not be saved.
     */
    private UpdatedAtlasElement getReadOnlyUpdatedElement(AtlasElement AtlasElement) {
       return getOrCreateUpdatedElement(AtlasElement, false);
    }


    /**
     * This returns an UpdatedAtlasElement that corresponds to the given AtlasElement.  If there are no changes
     * to the given AtlasElement, an UpdatedAtlasElement is created and added to the cache.
     */
    private UpdatedAtlasElement getUpdatedElement(AtlasElement AtlasElement) {
        return getOrCreateUpdatedElement(AtlasElement, true);
    }

    private UpdatedAtlasElement getOrCreateUpdatedElement(AtlasElement AtlasElement, boolean updateCache) {

        UpdatedAtlasElement result = updatedAtlasElements_.get(AtlasElement);
        if(result == null) {
            result = new UpdatedAtlasElement(AtlasElement);
            if(updateCache) {
                updatedAtlasElements_.put(AtlasElement, result);
            }
        }
        return result;
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

        private AtlasElement AtlasElement_;
        private String property_;
        private Object newValue_;

        public PropertyUpdate(AtlasElement AtlasElement_, String property_, Object newValue_) {
            super();
            this.AtlasElement_ = AtlasElement_;
            this.property_ = property_;
            this.newValue_ = newValue_;
        }

        @Override
        public void perform(GraphHelper helper) {
            GraphHelper.setProperty(AtlasElement_, property_, newValue_);
        }
    }

    private static class AtlasVertexRemoval implements DeleteAction {
        private AtlasVertex toDelete_;

        public AtlasVertexRemoval(AtlasVertex toDelete) {
            super();
            this.toDelete_ = toDelete;
        }

        @Override
        public void perform(GraphHelper helper) {
            helper.removeVertex(toDelete_);
        }
    }

    private static class AtlasEdgeRemoval implements DeleteAction {

        private AtlasEdge toDelete_;

        public AtlasEdgeRemoval(AtlasEdge toDelete) {
            super();
            this.toDelete_ = toDelete;
        }

        @Override
        public void perform(GraphHelper helper) {
            helper.removeEdge(toDelete_);
        }
    }

    /**
     * Represents the updated state of an AtlasAtlasElement, with
     * the property changes through the DeleteContext applied.
     *
     */
    private static class UpdatedAtlasElement {

        private AtlasElement wrapped_;
        private boolean deleted_ = false;
        private Map<String,Object> updatedPropertyValues_ = new HashMap<String,Object>();

        public UpdatedAtlasElement(AtlasElement AtlasElement) {
            wrapped_ = AtlasElement;
        }

        /**
         * Records a property value change.
         */
        public void setProperty(String key, Object value) {
            updatedPropertyValues_.put(key, value);
        }

        /**
         * Gets the value of the given property, taking into account uncommitted
         * changes made through the delete context.
         *
         * @param key the property name
         * @return the value of the property.
         */
        public <T> T getProperty(String key, Class<T> clazz) {
            if(updatedPropertyValues_.containsKey(key)) {
                return (T)updatedPropertyValues_.get(key);
            }
            return wrapped_.getProperty(key, clazz);
        }
        
        /**
         * Gets the value of the given property, taking into account uncommitted
         * changes made through the delete context.
         *
         * @param key the property name
         * @return the value of the property.
         */
        public List<String> getListProperty(String key) throws AtlasException {
            
            if(updatedPropertyValues_.containsKey(key)) {
                return (List<String>)updatedPropertyValues_.get(key);
            }
            return wrapped_.getListProperty(key);
        }
        /**
         * Records that this AtlasElement has been deleted.
         */
        public void delete() {
            deleted_ = true;
            updatedPropertyValues_.clear();
        }

        /**
         * Whether or not this AtlasElement has been deleted.
         *
         */
        public boolean isDeleted() {
            return deleted_;
        }
    }

}
