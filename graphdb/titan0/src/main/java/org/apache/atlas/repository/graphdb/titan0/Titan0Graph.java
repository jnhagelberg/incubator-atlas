
package org.apache.atlas.repository.graphdb.titan0;
import java.util.List;
import java.util.Set;

import javax.script.Bindings;

import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAGraph;
import org.apache.atlas.repository.graphdb.AAGraphQuery;
import org.apache.atlas.repository.graphdb.AAIndexQuery;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.repository.graphdb.ElementType;
import org.apache.atlas.repository.graphdb.GraphDatabaseManager;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.utils.IterableAdapter;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.util.structures.Row;

public class Titan0Graph implements AAGraph<Vertex,Edge> {

    
    public Titan0Graph() {

    }

    @Override
    public AAEdge<Vertex,Edge> addEdge(Object id, AAVertex<Vertex,Edge> outVertex, AAVertex<Vertex,Edge> inVertex, String edgeLabel) {
        Edge edge = getGraph().addEdge(id, 
                outVertex.getWrappedVertex(), 
                inVertex.getWrappedVertex(), 
                edgeLabel);
        return TitanObjectFactory.createEdge(edge);
    }

    @Override
    public AAGraphQuery<Vertex,Edge> query() {
        GraphQuery query = getGraph().query();
        return TitanObjectFactory.createQuery(query);
    }

    @Override
    public AAEdge<Vertex,Edge> getEdge(String edgeId) {
        Edge edge = getGraph().getEdge(edgeId);
        return TitanObjectFactory.createEdge(edge);
    }

    @Override
    public void removeEdge(AAEdge<Vertex,Edge> edge) {
        getGraph().removeEdge(edge.getWrappedEdge());
        
    }

    @Override
    public void removeVertex(AAVertex<Vertex,Edge> vertex) {
        getGraph().removeVertex(vertex.getWrappedVertex());
        
    }

    @Override
    public Iterable<AAEdge<Vertex,Edge>> getEdges() {
        Iterable<Edge> edges = getGraph().getEdges();
        return new IterableAdapter<Edge, AAEdge<Vertex, Edge>>(edges, EdgeMapper.INSTANCE);
    }

    @Override
    public Iterable<AAVertex<Vertex,Edge>> getVertices() {
        Iterable<Vertex> vertices = getGraph().getVertices();
        return new IterableAdapter<Vertex, AAVertex<Vertex, Edge>>(vertices, VertexMapper.INSTANCE);
    }

    @Override
    public AAVertex<Vertex,Edge> addVertex(Object id) {
        Vertex result = getGraph().addVertex(id);
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
    public AAIndexQuery<Vertex,Edge> indexQuery(String fulltextIndex, String graphQuery) {
        TitanIndexQuery query = getGraph().indexQuery(fulltextIndex, graphQuery);
        return new Titan0IndexQuery(query);
    }

    @Override
    public GraphDatabaseManager getManagementSystem() {
        return new Titan0DatabaseManager(getGraph().getManagementSystem());
    }

    @Override
    public void shutdown() {
       getGraph().shutdown();        
    }

    @Override
    public Set<String> getIndexedKeys(ElementType type) {
        Class<? extends Element> titanClass = type == ElementType.VERTEX ? Vertex.class : Edge.class;
       return getGraph().getIndexedKeys(titanClass);
    }

    @Override
    public AAVertex<Vertex, Edge> getVertex(String vertexId) {
        Vertex v = getGraph().getVertex(vertexId);
        return TitanObjectFactory.createVertex(v);
    }

    @Override
    public Iterable<AAVertex<Vertex, Edge>> getVertices(String key, Object value) {
        
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
    	return Titan0GraphPlugin.getGraphInstance();
    }
}
