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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.atlas.typesystem.persistence.Id.EntityState;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;

/**
 * Accumulates the changes that are needed to perform a delete,
 * applies them all at once, so that the underlying graph
 * system can batch the updates.  It also prevents any actual
 * updates of the graph from happening until all of the processing
 * has taken place.  This makes the processing more robust in the
 * face of non-ACID transactions.  We won't even attempt any
 * graph updates if processing errors occur. 
 *
 */
public class DeleteContext {
    
    private GraphHelper graphHelper_;
    
    //maintain a list of the actions so that the operations get applied
    //in the same order as they came in at
    private List<DeleteAction> deleteActions_ = new ArrayList<DeleteAction>();
    private Set<Element> elementsMarkedForDelete_ = new HashSet<Element>();
    private Set<Vertex> processedVertices_ = new HashSet<Vertex>();
    
    
    public DeleteContext(GraphHelper helper) {
        graphHelper_ = helper;
    }
    
    public void registerSoftDeletedElement(Element element) {
        
        elementsMarkedForDelete_.add(element);
    }
    
    public void removeVertex(Vertex vertex) {
        
        if(isDeleted(vertex)) {
            throw new IllegalStateException("Cannot delete a vertex that has already been deleted");
        }
        deleteActions_.add(new VertexRemoval(vertex));
        elementsMarkedForDelete_.add(vertex);
    }

    
    public void removeEdge(Edge edge) {
        if(isDeleted(edge)) {
            throw new IllegalStateException("Cannot delete an edge that has already been deleted");
        }
        deleteActions_.add(new EdgeRemoval(edge));
        elementsMarkedForDelete_.add(edge);
    }

    public void setProperty(Element element, String name, Object value) {
        if(isDeleted(element)) {
            throw new IllegalStateException("Cannot update an element that has been deleted.");
        }      
        deleteActions_.add(new PropertyUpdate(element, name, value));
    }
    
    public void commitDelete() {
        for(DeleteAction action : deleteActions_) {
            action.perform(graphHelper_);
        }
        elementsMarkedForDelete_.clear();
        processedVertices_.clear();
    }
    
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
    
    public boolean isProcessedOrDeleted(Vertex vertex) {
        return isProcessed(vertex) || ! isActive(vertex);
    }

    public boolean isActive(Element element) {
        EntityState state = GraphHelper.getState(element);
        return state == EntityState.ACTIVE && ! isDeleted(element);
    }
    
    public boolean isProcessed(Vertex vertex) {
        return processedVertices_.contains(vertex);
    }
    
    public void addProcessedVertex(Vertex vertex) {
        processedVertices_.add(vertex);
    }
    
    private boolean isDeleted(Element instanceVertex) {
        return elementsMarkedForDelete_.contains(instanceVertex);
    }

}
