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

package org.apache.atlas.repository.graphdb.titan1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

/**
 *
 */
public class GraphDatabaseTest {

    /**
     *
     */
    private static final String TRAIT_NAMES = "__traitNames";
    private AtlasGraph<?,?> graph;

    private <V,E> AtlasGraph<V, E> getGraph() {
        if(graph == null) {
            Titan1Database db = new Titan1Database();
            graph = db.getGraph();
        }
        return (AtlasGraph<V,E>)graph;
    }

     @Test
    public <V,E> void testPropertyDataTypes() {

        AtlasGraph<V,E> graph = getGraph();
        testProperty(graph, "booleanProperty", Boolean.TRUE);
        testProperty(graph, "booleanProperty", Boolean.FALSE);

        testProperty(graph, "intProperty", 5);
        testProperty(graph, "intProperty", Integer.MAX_VALUE);
        testProperty(graph, "intProperty", Integer.MIN_VALUE);

        testProperty(graph, "longProperty", 5l);
        testProperty(graph, "longProperty", Long.MIN_VALUE);
        testProperty(graph, "longProperty", Long.MAX_VALUE);


    }

    private <V,E> void testProperty(AtlasGraph<V,E> graph, String name, Object value) {

        AtlasVertex<V,E> vertex = graph.addVertex();
        vertex.setProperty(name, value);
        assertEquals(value, vertex.getProperty(name));
        AtlasVertex<V,E> loaded = graph.getVertex(vertex.getId().toString());
        assertEquals(value, loaded.getProperty(name));


    }

    @Test
    public <V, E> void testMultiplicityOnePropertySupport() {

        AtlasGraph<V, E> graph = (AtlasGraph<V, E>) getGraph();

        AtlasVertex<V, E> vertex = graph.addVertex();
        vertex.setProperty("name", "Jeff");
        vertex.setProperty("location", "Littleton");
        assertEquals("Jeff", vertex.getProperty("name"));
        assertEquals("Littleton", vertex.getProperty("location"));

        AtlasVertex<V, E> vertexCopy = graph.getVertex(vertex.getId().toString());

        assertEquals("Jeff", vertexCopy.getProperty("name"));
        assertEquals("Littleton", vertexCopy.getProperty("location"));

        assertTrue(vertexCopy.getPropertyKeys().contains("name"));
        assertTrue(vertexCopy.getPropertyKeys().contains("location"));

        assertTrue(vertexCopy.getPropertyValues("name", String.class).contains("Jeff"));
        assertTrue(vertexCopy.getPropertyValues("location", String.class).contains("Littleton"));
        assertTrue(vertexCopy.getPropertyValues("test", String.class).isEmpty());
        assertNull(vertexCopy.getProperty("test"));

        vertex.removeProperty("name");
        assertFalse(vertex.getPropertyKeys().contains("name"));
        assertNull(vertex.getProperty("name"));
        assertTrue(vertex.getPropertyValues("name", String.class).isEmpty());

        vertexCopy = graph.getVertex(vertex.getId().toString());
        assertFalse(vertexCopy.getPropertyKeys().contains("name"));
        assertNull(vertexCopy.getProperty("name"));
        assertTrue(vertexCopy.getPropertyValues("name", String.class).isEmpty());

    }

    @Test
    public <V,E> void testRemoveEdge() {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasVertex<V,E> v1 = graph.addVertex();
        AtlasVertex<V,E> v2 = graph.addVertex();

        AtlasEdge<V,E> edge = graph.addEdge(v1, v2, "knows");

        //make sure the edge exists
        AtlasEdge<V, E> edgeCopy = graph.getEdge(edge.getId().toString());
        assertNotNull(edgeCopy);
        assertEquals(edgeCopy, edge);


        graph.removeEdge(edge);

        edgeCopy = graph.getEdge(edge.getId().toString());
        //should return null now, since edge was deleted
        assertNull(edgeCopy);

    }

    @Test
    public <V,E> void testRemoveVertex() {

        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();

        AtlasVertex<V,E> v1 = graph.addVertex();

        assertNotNull(graph.getVertex(v1.getId().toString()));

        graph.removeVertex(v1);

        assertNull(graph.getVertex(v1.getId().toString()));
    }


     @Test
    public <V,E> void testGetEdges() {


        AtlasGraph<V,E> graph = (AtlasGraph<V,E>)getGraph();
        AtlasVertex<V,E> v1 = graph.addVertex();
        AtlasVertex<V,E> v2 = graph.addVertex();
        AtlasVertex<V,E> v3 = graph.addVertex();

        AtlasEdge<V,E> knows = graph.addEdge(v2, v1, "knows");
        AtlasEdge<V,E> eats =  graph.addEdge(v3, v1, "eats");
        AtlasEdge<V,E> drives = graph.addEdge(v3, v2, "drives");
        AtlasEdge<V,E> sleeps = graph.addEdge(v2, v3, "sleeps");


        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.IN));
            assertEquals(2, edges.size());
            assertTrue(edges.contains(knows));
            assertTrue(edges.contains(eats));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.OUT));
            assertTrue(edges.isEmpty());
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.BOTH));
            assertEquals(2, edges.size());
            assertTrue(edges.contains(knows));
            assertTrue(edges.contains(eats));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.IN, "knows"));
            assertEquals(1, edges.size());
            assertTrue(edges.contains(knows));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v1.getEdges(AtlasEdgeDirection.BOTH, "knows"));
            assertEquals(1, edges.size());
            assertTrue(edges.contains(knows));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v2.getEdges(AtlasEdgeDirection.IN));
            assertEquals(1, edges.size());
            assertTrue(edges.contains(drives));
        }


        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v2.getEdges(AtlasEdgeDirection.OUT));
            assertEquals(2, edges.size());
            assertTrue(edges.contains(knows));
            assertTrue(edges.contains(sleeps));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v2.getEdges(AtlasEdgeDirection.BOTH));
            assertEquals(3, edges.size());
            assertTrue(edges.contains(knows));
            assertTrue(edges.contains(sleeps));
            assertTrue(edges.contains(drives));
        }

        {
            List<AtlasEdge<V,E>> edges =  IteratorUtils.asList(v2.getEdges(AtlasEdgeDirection.BOTH,"delivers"));
            assertEquals(0, edges.size());
        }


    }


    @Test
    public <V,E >void testMultiplictyManyPropertySupport() {

        AtlasGraph<V,E> graph = getGraph();
        String vertexId;
        {
            AtlasVertex<V, E> vertex = graph.addVertex();
            vertexId = vertex.getId().toString();
            vertex.setProperty(TRAIT_NAMES, "trait1");
            vertex.setProperty(TRAIT_NAMES, "trait2");
            assertEquals(2, vertex.getPropertyValues(TRAIT_NAMES, String.class).size());
            vertex.addProperty(TRAIT_NAMES, "trait3");
            vertex.addProperty(TRAIT_NAMES, "trait4");
            assertTrue(vertex.getPropertyKeys().contains(TRAIT_NAMES));
            Collection<String> traitNames = vertex.getPropertyValues(TRAIT_NAMES, String.class);
            assertTrue(traitNames.contains("trait1"));
            assertTrue(traitNames.contains("trait2"));
            assertTrue(traitNames.contains("trait3"));
            assertTrue(traitNames.contains("trait4"));

            try {
                vertex.getProperty(TRAIT_NAMES);        }
            catch(IllegalStateException expected) {
                //multiple property values exist
            }
        }

        {
            AtlasVertex<V,E> vertexCopy = graph.getVertex(vertexId);
            assertTrue(vertexCopy.getPropertyKeys().contains(TRAIT_NAMES));
            Collection<String> traitNames = vertexCopy.getPropertyValues(TRAIT_NAMES, String.class);
            assertTrue(traitNames.contains("trait1"));
            assertTrue(traitNames.contains("trait2"));
            assertTrue(traitNames.contains("trait3"));
            assertTrue(traitNames.contains("trait4"));

            try {
                vertexCopy.getProperty(TRAIT_NAMES);        }
            catch(IllegalStateException expected) {
                //multiple property values exist
            }
        }
    }

    @Test
    public <V,E >void testAddMultManyPropertyValueTwice() {

        AtlasGraph<V,E> graph = getGraph();
        String vertexId;
        {
            AtlasVertex<V, E> vertex = graph.addVertex();
            vertexId = vertex.getId().toString();
            vertex.setProperty(TRAIT_NAMES, "trait1");
            vertex.setProperty(TRAIT_NAMES, "trait1");
            vertex.addProperty(TRAIT_NAMES, "trait2");
            vertex.addProperty(TRAIT_NAMES, "trait2");
            assertEquals(2, vertex.getPropertyValues(TRAIT_NAMES, String.class).size());
            assertTrue(vertex.getPropertyKeys().contains(TRAIT_NAMES));
            Collection<String> traitNames = vertex.getPropertyValues(TRAIT_NAMES, String.class);
            assertTrue(traitNames.contains("trait1"));
            assertTrue(traitNames.contains("trait2"));

        }

        {
            AtlasVertex<V,E> vertexCopy = graph.getVertex(vertexId);
            assertTrue(vertexCopy.getPropertyKeys().contains(TRAIT_NAMES));
            Collection<String> traitNames = vertexCopy.getPropertyValues(TRAIT_NAMES, String.class);
            assertTrue(traitNames.contains("trait1"));
            assertTrue(traitNames.contains("trait2"));
        }
    }
}
