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
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.utils.EdgeToAtlasEdgeFunction;
import org.apache.atlas.utils.VertexToAtlasVertexFuncion;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONWriter;
import com.tinkerpop.pipes.util.structures.Row;

public class Titan0Graph implements AtlasGraph<Titan0Vertex, Titan0Edge> {

    
    public Titan0Graph() {

    }

    @Override
    public AtlasEdge<Titan0Vertex, Titan0Edge> addEdge(AtlasVertex<Titan0Vertex, Titan0Edge> outVertex, AtlasVertex<Titan0Vertex, Titan0Edge> inVertex, String edgeLabel) {
        try {
            Edge edge = getGraph().addEdge(null, 
                    outVertex.getV().getWrappedElement(), 
                    inVertex.getV().getWrappedElement(), 
                    edgeLabel);
            return TitanObjectFactory.createEdge(edge);
        }
        catch(SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public AtlasGraphQuery<Titan0Vertex, Titan0Edge> query() {
        GraphQuery query = getGraph().query();
        return TitanObjectFactory.createQuery(query);
    }

    @Override
    public AtlasEdge<Titan0Vertex, Titan0Edge> getEdge(String edgeId) {
        Edge edge = getGraph().getEdge(edgeId);
        return TitanObjectFactory.createEdge(edge);
    }

    @Override
    public void removeEdge(AtlasEdge<Titan0Vertex, Titan0Edge> edge) {
        getGraph().removeEdge(edge.getE().getWrappedElement());
        
    }

    @Override
    public void removeVertex(AtlasVertex<Titan0Vertex, Titan0Edge> vertex) {
        getGraph().removeVertex(vertex.getV().getWrappedElement());
        
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> getEdges() {
        Iterable<Edge> edges = getGraph().getEdges();
        return Iterables.transform(edges, EdgeToAtlasEdgeFunction.INSTANCE);
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> getVertices() {
        Iterable<Vertex> vertices = getGraph().getVertices();
        return Iterables.transform(vertices, VertexToAtlasVertexFuncion.INSTANCE);
    }

    @Override
    public AtlasVertex<Titan0Vertex, Titan0Edge> addVertex() {
        Vertex result = getGraph().addVertex(null);
        return TitanObjectFactory.createVertex(result);
    }

    @Override
    public void commit() {
        getGraph().commit();        
    }

    @Override
    public void rollback() {
        getGraph().rollback();
    }

    @Override
    public AtlasIndexQuery<Titan0Vertex, Titan0Edge> indexQuery(String fulltextIndex, String graphQuery) {
        TitanIndexQuery query = getGraph().indexQuery(fulltextIndex, graphQuery);
        return new Titan0IndexQuery(query);
    }

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new Titan0DatabaseManager(getGraph().getManagementSystem());
    }

    @Override
    public void shutdown() {
       getGraph().shutdown();        
    }

    public Set<String> getVertexIndexKeys() {
        return getIndexKeys(Vertex.class);
    }
    public Set<String> getEdgeIndexKeys() {
        return getIndexKeys(Edge.class);
    }
    
    
    private Set<String> getIndexKeys(Class<? extends Element> titanClass) {
        
       return getGraph().getIndexedKeys(titanClass);
    }

    @Override
    public AtlasVertex<Titan0Vertex, Titan0Edge> getVertex(String vertexId) {
        Vertex v = getGraph().getVertex(vertexId);
        return TitanObjectFactory.createVertex(v);
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> getVertices(String key, Object value) {
        
        Iterable<Vertex> result = getGraph().getVertices(key, value);
        return Iterables.transform(result, VertexToAtlasVertexFuncion.INSTANCE);
    }

   
    @Override
    public Object getGremlinColumnValue(Object rowValue, String colName, int idx) {
        Row<List> rV = (Row<List>)rowValue;
        Object value = rV.getColumn(colName).get(idx);
        return convertGremlinValue(value);
    }

    @Override
    public Object convertGremlinValue(Object rawValue) {
        if(rawValue instanceof Vertex) {
            return TitanObjectFactory.createVertex((Vertex)rawValue);
        }
        if(rawValue instanceof Edge) {
            return TitanObjectFactory.createEdge((Edge)rawValue);
        }
        return rawValue;
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {

        return GremlinVersion.TWO;
    }

    @Override
    public List<Object> convertPathQueryResultToList(Object rawValue) {
        return (List<Object>)rawValue;
    }

    @Override
    public void clear() {       
        TitanGraph graph = getGraph();
		if(graph.isOpen()) {
            //only a shut down graph can be cleared
            graph.shutdown();
        }
        TitanCleanup.clear(graph);
    }
    
    private TitanGraph getGraph() {
    	//return the singleton instance of the graph in the plugin
    	return Titan0Database.getGraphInstance();
    }
    
    @Override
    public void exportToGson(OutputStream os) throws IOException {
        GraphSONWriter.outputGraph(getGraph(), os);        
    }
    
        /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#executeGremlinScript(java.lang.String)
     */
    @Override
    public Object executeGremlinScript(String gremlinQuery) throws ScriptException {
        
        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("gremlin-groovy");
        Bindings bindings = engine.createBindings();
        bindings.put("g", getGraph());
        Object result = engine.eval(gremlinQuery, bindings);
        return result;
    }
}
