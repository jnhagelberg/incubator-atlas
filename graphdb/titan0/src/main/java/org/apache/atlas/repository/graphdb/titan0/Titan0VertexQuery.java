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

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.utils.EdgeToAtlasEdgeFunction;
import org.apache.atlas.utils.VertexToAtlasVertexFuncion;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

/**
 * Titan 0.5.4 implementation of AtlasVertexQuery
 */
public class Titan0VertexQuery implements AtlasVertexQuery<Titan0Vertex, Titan0Edge> {

    private VertexQuery query_;

    public Titan0VertexQuery(VertexQuery vertexQuery) {
        query_ = vertexQuery;
    }

    @Override
    public AtlasVertexQuery<Titan0Vertex, Titan0Edge> direction(AtlasEdgeDirection queryDirection) {
        query_.direction(TitanObjectFactory.createDirection(queryDirection));
        return this;
        
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> vertices() {
        Iterable<Vertex> vertices = query_.vertices();
        return Iterables.transform(vertices, VertexToAtlasVertexFuncion.INSTANCE);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> edges() {
        Iterable<Edge> vertices = query_.edges();
        return Iterables.transform(vertices, EdgeToAtlasEdgeFunction.INSTANCE);
   
    }

    @Override
    public long count() {
        return query_.count();
    }
    
    
}