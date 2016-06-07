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
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.EdgeToAtlasEdgeFunction;
import org.apache.atlas.utils.VertexToAtlasVertexFunction;

import com.google.common.collect.Iterables;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

/**
 * Titan 0.5.4 implementation of AtlasGraphQuery.
 */
public class Titan0GraphQuery implements AtlasGraphQuery<Titan0Vertex, Titan0Edge> {

    private GraphQuery wrappedQuery;

    public Titan0GraphQuery(GraphQuery query) {
        wrappedQuery = query;
    }

    @Override
    public AtlasGraphQuery<Titan0Vertex, Titan0Edge> has(String propertyKey, Object value) {
        wrappedQuery = wrappedQuery.has(propertyKey, value);
        return this;
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> vertices() {
        Iterable<Vertex> result = wrappedQuery.vertices();
        return Iterables.transform(result, VertexToAtlasVertexFunction.INSTANCE);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> edges() {
        Iterable<Edge> result = wrappedQuery.edges();
        return Iterables.transform(result, EdgeToAtlasEdgeFunction.INSTANCE);
    }

    @Override
    public AtlasGraphQuery<Titan0Vertex, Titan0Edge> has(String propertyKey, ComparisionOperator operator,
            Object value) {
        Compare c = getGremlinPredicate(operator);
        wrappedQuery = wrappedQuery.has(propertyKey, c, value);
        return this;
    }

    private Compare getGremlinPredicate(ComparisionOperator op) {
        switch (op) {
        case EQUAL:
            return Compare.EQUAL;
        case GREATER_THAN_EQUAL:
            return Compare.GREATER_THAN_EQUAL;
        case LESS_THAN_EQUAL:
            return Compare.LESS_THAN_EQUAL;
        default:
            throw new RuntimeException("Unsupported comparison operator:" + op);
        }
    }

}
