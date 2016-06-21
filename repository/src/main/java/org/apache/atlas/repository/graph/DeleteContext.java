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

import org.apache.atlas.typesystem.persistence.Id.EntityState;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

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
    private Set<Vertex> processedVertices_ = new HashSet<Vertex>();
    private Map<Element, UpdatedElement> updateElements_ = new HashMap<>();

    public DeleteContext(GraphHelper helper) {
        graphHelper_ = helper;
    }

    /**
     * Records that the given element has been soft deleted so
     * that is is treated as deleted by the delete context.
     *
     * @param element
     */
    public void softDeleteElement(Element element) {
        getUpdatedElement(element).delete();
    }

    /**
     * Records that the specified Vertex should be deleted.  It will be deleted
     * when commitDelete() is called.
     *
     * @param vertex The vertex to delete.
     */
    public void removeVertex(Vertex vertex) {

        if (isDeleted(vertex)) {
            throw new IllegalStateException("Cannot delete a vertex that has already been deleted");
        }
        deleteActions_.add(new VertexRemoval(vertex));
        getUpdatedElement(vertex).delete();
    }

    /**
    * Records that the specified Edge should be deleted.  It will be deleted
    * when commitDelete() is called.
    *
    * @param vertex The vertex to delete.
    */
    public void removeEdge(Edge edge) {
        if (isDeleted(edge)) {
            throw new IllegalStateException("Cannot delete an edge that has already been deleted");
        }
        deleteActions_.add(new EdgeRemoval(edge));
        getUpdatedElement(edge).delete();
    }

    /**
     * Records that a property needs to be set in an Element.  The change will take place
     * when commitDelete() is called.
     *
     * @param element the element to update
     * @param name the name of the property to set
     * @param value the value to set the property to
     */
    public void setProperty(Element element, String name, Object value) {
        if (isDeleted(element)) {
            throw new IllegalStateException("Cannot update an element that has been deleted.");
        }
        deleteActions_.add(new PropertyUpdate(element, name, value));
        getUpdatedElement(element).setProperty(name, value);
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
     * Gets the value of the specified property on the given element, taking into
     * account change that have been applied to the DeleteContext but have not
     * yet been committed into the graph.
     *
     * @param element
     * @param property
     * @param clazz
     * @return
     */
    public <T> T getProperty(Element element, String property) {
        return getReadOnlyUpdatedElement(element).getProperty(property);
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
    public boolean isProcessedOrDeleted(Vertex vertex) {
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
    public boolean isActive(Element element) {

        EntityState state = GraphHelper.getState(element);
        return state == EntityState.ACTIVE && !isDeleted(element);
    }

    private boolean isDeleted(Element instanceVertex) {
        return getReadOnlyUpdatedElement(instanceVertex).isDeleted();
    }

    /**
     * Returns true if the given Vertex has been previously processed
     * by the delete algorithm.
     *
     * @param vertex
     * @return
     */
    public boolean isProcessed(Vertex vertex) {
        return processedVertices_.contains(vertex);
    }
    /**
     * Records that a given Vertex has been processed by the delete algorithm.
     *
     * @param vertex
     */
    public void addProcessedVertex(Vertex vertex) {
        processedVertices_.add(vertex);
    }


    /**
     * This returns an UpdatedElement that corresponds to the given element.  If there are no changes
     * to the given element, a temporary UpdatedElement is created (but not cached).  No changes should be applied
     * to UpdatedElements returned here, since they may not be saved.
     */
    private UpdatedElement getReadOnlyUpdatedElement(Element element) {
       return getOrCreateUpdatedElement(element, false);
    }


    /**
     * This returns an UpdatedElement that corresponds to the given element.  If there are no changes
     * to the given element, an UpdatedElement is created and added to the cache.
     */
    private UpdatedElement getUpdatedElement(Element element) {
        return getOrCreateUpdatedElement(element, true);
    }

    private UpdatedElement getOrCreateUpdatedElement(Element element, boolean updateCache) {

        UpdatedElement result = updateElements_.get(element);
        if(result == null) {
            result = new UpdatedElement(element);
            if(updateCache) {
                updateElements_.put(element, result);
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

        private Element element_;
        private String property_;
        private Object newValue_;

        public PropertyUpdate(Element element_, String property_, Object newValue_) {
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
        private Vertex toDelete_;

        public VertexRemoval(Vertex toDelete) {
            super();
            this.toDelete_ = toDelete;
        }

        @Override
        public void perform(GraphHelper helper) {
            helper.removeVertex(toDelete_);
        }
    }

    private static class EdgeRemoval implements DeleteAction {

        private Edge toDelete_;

        public EdgeRemoval(Edge toDelete) {
            super();
            this.toDelete_ = toDelete;
        }

        @Override
        public void perform(GraphHelper helper) {
            helper.removeEdge(toDelete_);
        }
    }

    /**
     * Represents the updated state of an AtlasElement, with
     * the property changes through the DeleteContext applied.
     *
     */
    private static class UpdatedElement {

        private Element wrapped_;
        private boolean deleted_ = false;
        private Map<String,Object> propertyChanges_ = new HashMap<String,Object>();

        public UpdatedElement(Element element) {
            wrapped_ = element;
        }

        /**
         * Records a property value change.
         */
        public void setProperty(String key, Object value) {
            propertyChanges_.put(key, value);
        }

        /**
         * Gets the value of the given property, taking into account uncommitted
         * changes made through the delete context.
         *
         * @param key the property name
         * @return the value of the property.
         */
        public <T> T getProperty(String key) {
            if(propertyChanges_.containsKey(key)) {
                return (T)propertyChanges_.get(key);
            }
            return wrapped_.getProperty(key);
        }

        /**
         * Records that this element has been deleted.
         */
        public void delete() {
            deleted_ = true;
            propertyChanges_.clear();
        }

        /**
         * Whether or not this element has been deleted.
         *
         */
        public boolean isDeleted() {
            return deleted_;
        }
    }

}
