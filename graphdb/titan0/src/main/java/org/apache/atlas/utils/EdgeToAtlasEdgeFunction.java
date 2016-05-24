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

package org.apache.atlas.utils;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.titan0.GraphDbObjectFactory;
import org.apache.atlas.repository.graphdb.titan0.Titan0Edge;
import org.apache.atlas.repository.graphdb.titan0.Titan0Vertex;

import com.google.common.base.Function;
import com.tinkerpop.blueprints.Edge;

/**
 * Google Guava function that converts an Edge to an AtlasEdge
 *
 * @see org.apache.atlas.repository.graphdb.titan0.Titan0Graph#getEdges() org.apache.atlas.repository.graphdb.titan0.Titan0Graph#getEdges() for an example of how this is used.
 */
public class EdgeToAtlasEdgeFunction implements Function<Edge, AtlasEdge<Titan0Vertex, Titan0Edge>> {

    public static final EdgeToAtlasEdgeFunction INSTANCE = new EdgeToAtlasEdgeFunction();

    private EdgeToAtlasEdgeFunction() {

    }

    /* (non-Javadoc)
     * @see com.google.common.base.Function#apply(java.lang.Object)
     */
    @Override
    public AtlasEdge<Titan0Vertex, Titan0Edge> apply(Edge edge) {

        return GraphDbObjectFactory.createEdge(edge);
    }


}
