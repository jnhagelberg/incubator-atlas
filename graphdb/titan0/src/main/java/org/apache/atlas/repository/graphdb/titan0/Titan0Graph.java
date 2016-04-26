
package org.apache.atlas.repository.graphdb.titan0;
import java.util.List;
import java.util.Set;

import javax.script.Bindings;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.utils.adapters.IterableAdapter;
import org.apache.atlas.utils.adapters.impl.EdgeMapper;
import org.apache.atlas.utils.adapters.impl.VertexMapper;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.util.structures.Row;

public class Titan0Graph implements AtlasGraph<Titan0Vertex, Titan0Edge> {

    
    public Titan0Graph() {

    }

    @Override
    public AtlasEdge<Titan0Vertex, Titan0Edge> addEdge(AtlasVertex<Titan0Vertex, Titan0Edge> outVertex, AtlasVertex<Titan0Vertex, Titan0Edge> inVertex, String edgeLabel) {
        Edge edge = getGraph().addEdge(null, 
                outVertex.getV().getWrappedElement(), 
                inVertex.getV().getWrappedElement(), 
                edgeLabel);
        return TitanObjectFactory.createEdge(edge);
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
        return new IterableAdapter<Edge, AtlasEdge<Titan0Vertex, Titan0Edge>>(edges, EdgeMapper.INSTANCE);
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> getVertices() {
        Iterable<Vertex> vertices = getGraph().getVertices();
        return new IterableAdapter<Vertex, AtlasVertex<Titan0Vertex, Titan0Edge>>(vertices, VertexMapper.INSTANCE);
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
        return new IterableAdapter<>(result, VertexMapper.INSTANCE);

    }

    @Override
    public void injectBinding(Bindings bindings, String key) {
        bindings.put(key, getGraph());
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
}
