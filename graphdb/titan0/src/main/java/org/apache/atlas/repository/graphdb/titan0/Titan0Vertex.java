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
package org.apache.atlas.repository.graphdb.titan0;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.utils.adapters.IterableAdapter;
import org.apache.atlas.utils.adapters.impl.EdgeMapper;

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Titan0Vertex extends Titan0Element<Vertex> implements AtlasVertex<Titan0Vertex, Titan0Edge> {

    public Titan0Vertex(Vertex source) {
        super(source);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> getEdges(AtlasEdgeDirection dir, String edgeLabel) {
        Iterable<Edge> titanEdges = element_.getEdges(
                TitanObjectFactory.createDirection(dir), edgeLabel);
        return new IterableAdapter<>(titanEdges, EdgeMapper.INSTANCE);
    } 
    
    private TitanVertex getAsTitanVertex() {
        return (TitanVertex)element_;
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> getEdges(AtlasEdgeDirection in) {
        Iterable<Edge> titanResult = element_.getEdges(TitanObjectFactory.createDirection(in));
        return new IterableAdapter<Edge, AtlasEdge<Titan0Vertex, Titan0Edge>>(titanResult, EdgeMapper.INSTANCE);
    }

    @Override
    public <T> void addProperty(String propertyName, T value) {
        try {
            getAsTitanVertex().addProperty(propertyName, value);
        }
        catch(SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public <T> Collection<T> getPropertyValues(String key) {
        
        TitanVertex tv = getAsTitanVertex();
        Collection<T> result = new ArrayList<T>();
        for (TitanProperty property : tv.getProperties(key)) {
            result.add((T) property.getValue());
        }
        return result;        
    }   

    @Override
    public AtlasVertexQuery<Titan0Vertex, Titan0Edge> query() {
       return new Titan0VertexQuery(element_.query());
    }

    @Override
    public Titan0Vertex getV() {
        
        return this;
    }  

}
