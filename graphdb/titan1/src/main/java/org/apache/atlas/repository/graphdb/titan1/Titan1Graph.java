
package org.apache.atlas.repository.graphdb.titan1;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import org.apache.atlas.utils.IteratorAdapter;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;

public class Titan1Graph implements AAGraph<Vertex,Edge> {

    
    public Titan1Graph() {

    }

    @Override
    public AAEdge<Vertex,Edge> addEdge(Object id, AAVertex<Vertex,Edge> outVertex, AAVertex<Vertex,Edge> inVertex, String edgeLabel) {
        
        Vertex oV = outVertex.getWrappedVertex();
        Vertex iV = inVertex.getWrappedVertex();
        Edge edge;
        if(id == null) {
            edge = oV.addEdge(edgeLabel, iV);
        }
        else {
            edge = oV.addEdge(edgeLabel, iV, T.id, id);
        }
        return TitanObjectFactory.createEdge(edge);
    }

    @Override
    public AAGraphQuery<Vertex,Edge> query() {
              
        TitanGraphQuery<?> query = getGraph().query();
        return TitanObjectFactory.createQuery(query);
    }

    @Override
    public AAEdge<Vertex,Edge> getEdge(String edgeId) {
        Iterator<Edge> it = getGraph().edges(edgeId);
        Edge e = getSingleElement(it, edgeId);
        return TitanObjectFactory.createEdge(e);
    }

    @Override
    public void removeEdge(AAEdge<Vertex,Edge> edge) {
        
        Edge wrapped = edge.getWrappedEdge();
        wrapped.remove();
        
    }

    @Override
    public void removeVertex(AAVertex<Vertex,Edge> vertex) {
        Vertex wrapped = vertex.getWrappedVertex();
        wrapped.remove();        
    }

    @Override
    public Iterable<AAEdge<Vertex,Edge>> getEdges() {
        
        Iterator<Edge> edges = getGraph().edges();
        final Iterator<AAEdge<Vertex,Edge>> resultIt = new IteratorAdapter<>(edges, EdgeMapper.INSTANCE);
        
        return new Iterable<AAEdge<Vertex,Edge>>() {

            @Override
            public Iterator<AAEdge<Vertex, Edge>> iterator() {
                return resultIt;
            }            
        };
        
    }

    @Override
    public Iterable<AAVertex<Vertex,Edge>> getVertices() {
        
        Iterator<Vertex> vertices = getGraph().vertices();
        final Iterator<AAVertex<Vertex,Edge>> resultIt = new IteratorAdapter<>(vertices, VertexMapper.INSTANCE);
        
        return new Iterable<AAVertex<Vertex,Edge>>() {

            @Override
            public Iterator<AAVertex<Vertex, Edge>> iterator() {
                return resultIt;
            }            
        };
    }

    @Override
    public AAVertex<Vertex,Edge> addVertex(Object id) {
        Vertex result;
        if(id == null) {
            result = getGraph().addVertex();
        }
        else {
            result = getGraph().addVertex(T.id, id);
        }
        return TitanObjectFactory.createVertex(result);
    }

    @Override
    public void commit() {
        getGraph().tx().commit();        
    }

    @Override
    public void rollback() {
        getGraph().tx().rollback();
    }

    @Override
    public AAIndexQuery<Vertex,Edge> indexQuery(String fulltextIndex, String graphQuery) {
        TitanIndexQuery query = getGraph().indexQuery(fulltextIndex, graphQuery);
        return new Titan1IndexQuery(query);
    }

    @Override
    public GraphDatabaseManager getManagementSystem() {
        return new Titan1DatabaseManager(getGraph().openManagement());
    }

    @Override
    public void shutdown() {
        getGraph().close();
    }

    @Override
    public Set<String> getIndexedKeys(ElementType type) {
        Class<? extends Element> titanClass = type == ElementType.VERTEX ? Vertex.class : Edge.class;
        
        TitanManagement mgmt = getGraph().openManagement();
        Iterable<TitanGraphIndex> indices = mgmt.getGraphIndexes(titanClass);
        Set<String> result = new HashSet<String>(); 
        for(TitanGraphIndex index : indices) {
            result.add(index.name());
        }
        mgmt.commit();
        return result;
       
    }

    @Override
    public AAVertex<Vertex, Edge> getVertex(String vertexId) {
        Iterator<Vertex> it = getGraph().vertices(vertexId);
        Vertex v = getSingleElement(it, vertexId);
        return TitanObjectFactory.createVertex(v);
    }

    public static <T> T getSingleElement(Iterator<T> it, String id) {
        if(! it.hasNext()) {
            return null;
        }
        T element = it.next();
        if(it.hasNext()) {
            throw new RuntimeException("Multiple items were found with the id " + id);
        }
        return element;
    }
    
    @Override
    public Iterable<AAVertex<Vertex, Edge>> getVertices(String key, Object value) {
        AAGraphQuery<Vertex, Edge> query = query();        
        query.has(key, value);
        return query.vertices();
    }
    

    @Override
    public void injectBinding(Bindings bindings, String key) {
        bindings.put(key, getGraph().traversal());
    }

    
    @Override
    public Object getGremlinColumnValue(Object rowValue, String colName, int idx) {
                
        Object rawColumnValue = null;        
        if(rowValue instanceof Map) { 
           rawColumnValue = ((Map<?,?>)rowValue).get(colName);
        }
        else {
            //when there is only one column, result does not come back as a map
            rawColumnValue = rowValue;
        }
        
        Object value = null;
        if(rawColumnValue instanceof List && idx >= 0) {
            value = ((List<?>)rawColumnValue).get(idx);
        }
        else {
            value = rawColumnValue;
        }
        
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

        return GremlinVersion.THREE;
    }
 
    @Override
    public List<Object> convertPathQueryResultToList(Object rawValue) {
        ImmutablePath path =  (ImmutablePath)rawValue;
        return path.objects();
        
    }
    
    @Override
    public void clear() {
        TitanCleanup.clear(getGraph());
        
    }
    
    private TitanGraph getGraph() {
        return Titan1GraphPlugin.getGraphInstance();
    }
}
