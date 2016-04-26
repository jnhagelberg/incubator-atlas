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
package org.apache.atlas.utils.adapters.impl;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.titan0.Titan0Edge;
import org.apache.atlas.repository.graphdb.titan0.Titan0Vertex;
import org.apache.atlas.repository.graphdb.titan0.TitanObjectFactory;
import org.apache.atlas.utils.adapters.Mapper;

import com.tinkerpop.blueprints.Edge;

/** 
 * Mapper that converts Titan edges to Atlas edges. 
 */
public class EdgeMapper implements Mapper<Edge, AtlasEdge<Titan0Vertex, Titan0Edge>> {
    
    public static EdgeMapper INSTANCE = new EdgeMapper();
    
    private EdgeMapper() {
        
    }
    @Override
    public AtlasEdge<Titan0Vertex, Titan0Edge> map(Edge source) {
       return  TitanObjectFactory.createEdge(source);
    }
}