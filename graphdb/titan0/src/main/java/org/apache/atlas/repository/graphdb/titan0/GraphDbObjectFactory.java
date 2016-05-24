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

import org.apache.atlas.repository.graphdb.AtlasGraphIndex;

import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

/**
 * Factory that serves up instances of graph database abstraction
 * layer classes that correspond to Titan/Tinkerpop classes.
 */
public class GraphDbObjectFactory {

    private GraphDbObjectFactory() {

    }

    /**
     * Creates a Titan0Edge that corresponds to the given
     * Gremlin Edge.
     *
     * @param source
     * @return
     */
    public static Titan0Edge createEdge(Edge source) {

        if(source == null) {
            return null;
        }
        return new Titan0Edge(source);
    }

    /**
     * Creates a Titan0GraphQuery that corresponds to the given
     * GraphQuery.
     *
     * @param source
     * @return
     */
    public static Titan0GraphQuery createQuery(GraphQuery query) {

       return new Titan0GraphQuery(query);
    }

    /**
     * Creates a Titan0Vertex that corresponds to the given
     * Gremlin Vertex.
     *
     * @param source
     * @return
     */
    public static Titan0Vertex createVertex(Vertex source) {

        if(source == null) {
            return null;
        }
        return new Titan0Vertex(source);
    }

    /**
     * @param propertyKey
     * @return
     */
    public static Titan0PropertyKey createPropertyKey(PropertyKey propertyKey) {
        if(propertyKey == null) {
            return null;
        }
        return new Titan0PropertyKey(propertyKey);
    }

    /**
     * @param index
     * @return
     */
    public static AtlasGraphIndex createGraphIndex(TitanGraphIndex index) {
        if(index == null) {
            return null;
        }
        return new Titan0GraphIndex(index);
    }

}
