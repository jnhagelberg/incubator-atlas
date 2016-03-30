
package org.apache.atlas.repository.graphdb.titan1;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
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
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource.Builder;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.engine.StandardTraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class Titan1Graph implements AAGraph<Vertex,Edge> {

    private TitanGraph titanGraph_;
    
    public Titan1Graph(TitanGraph graph) {
        titanGraph_ = graph;
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
              
        TitanGraphQuery<?> query = titanGraph_.query();
        return TitanObjectFactory.createQuery(query);
    }

    @Override
    public AAEdge<Vertex,Edge> getEdge(String edgeId) {
        Iterator<Edge> it = titanGraph_.edges(edgeId);
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
        
        Iterator<Edge> edges = titanGraph_.edges();
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
        
        Iterator<Vertex> vertices = titanGraph_.vertices();
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
            result = titanGraph_.addVertex();
        }
        else {
            result = titanGraph_.addVertex(T.id, id);
        }
        return TitanObjectFactory.createVertex(result);
    }

    @Override
    public void commit() {
        titanGraph_.tx().commit();        
    }

    @Override
    public void rollback() {
        titanGraph_.tx().rollback();
    }

    @Override
    public AAIndexQuery<Vertex,Edge> indexQuery(String fulltextIndex, String graphQuery) {
        TitanIndexQuery query = titanGraph_.indexQuery(fulltextIndex, graphQuery);
        return new Titan1IndexQuery(query);
    }

    @Override
    public GraphDatabaseManager getManagementSystem() {
        return new Titan1DatabaseManager(titanGraph_.openManagement());
    }

    @Override
    public void shutdown() {
        titanGraph_.close();
    }

    @Override
    public Set<String> getIndexedKeys(ElementType type) {
        Class<? extends Element> titanClass = type == ElementType.VERTEX ? Vertex.class : Edge.class;
        
        TitanManagement mgmt = titanGraph_.openManagement();
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
        Iterator<Vertex> it = titanGraph_.vertices(vertexId);
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
        bindings.put(key, titanGraph_.traversal());
    }

    
    @Override
    public Object getGremlinColumnValue(Object rowValue, String colName, int idx) {
        
        //TBD
        //Row<List> rV = (Row<List>)rowValue;
        //Object value = rV.getColumn(colName).get(idx);
        
        Object value = rowValue;
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
    
    static class Foo implements TraversalSource.Builder<GraphTraversalSource> {
        TraversalSource.Builder<GraphTraversalSource> delegate = GraphTraversalSource.build().engine(StandardTraversalEngine.build())

        @Override
        public Builder engine(org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine.Builder engine) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Builder with(TraversalStrategy strategy) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Builder without(Class<? extends TraversalStrategy> strategyClass) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public GraphTraversalSource create(Graph graph) {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    static class CustomTraversalBuilder implements TraversalEngine.Builder {

        TraversalEngine.Builder delegate = GraphTraversalSource.build().engine(StandardTraversalEngine.build())
        
        @Override
        public TraversalEngine create(Graph graph) {
            // TODO Auto-generated method stub
            return null;
        }
        
    }
    
    static class DelegatingTraversalEngine implements TraversalEngine {

        @Override
        public Type getType() {
            // TODO Auto-generated method stub
            return Type.COMPUTER;
        }

        @Override
        public Optional<GraphComputer> getGraphComputer() {
            // TODO Auto-generated method stub
            return null;
        }
        
        
    }
}
