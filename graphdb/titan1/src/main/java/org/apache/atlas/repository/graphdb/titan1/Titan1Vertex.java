
package org.apache.atlas.repository.graphdb.titan1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.repository.graphdb.AAVertexQuery;
import org.apache.atlas.utils.IterableAdapter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.thinkaurelius.titan.core.TitanVertex;

public class Titan1Vertex extends Titan1Element<Vertex> implements AAVertex<Vertex, Edge> {

    private static final class EdgesIterable implements Iterable<Edge> {
        private final Iterator<Edge> edges;

        private EdgesIterable(Iterator<Edge> edges) {
            this.edges = edges;
        }

        @Override
        public Iterator<Edge> iterator() {
            return edges;
        }
    }
    
    public Titan1Vertex(Vertex source) {
        super(source);
    }

  
    @Override
    public void removeProperty(String propertyName) {  
        Iterator<VertexProperty<String>> it = element_.properties(propertyName);
        while(it.hasNext()) {
            VertexProperty<String> property = it.next();
            property.remove();
        }        
    }


    @Override
    public void setProperty(String propertyName, Object value) {
        element_.property(VertexProperty.Cardinality.single, propertyName, value);        
    }

    @Override
    public Iterable<AAEdge<Vertex, Edge>> getEdges(AADirection dir, String edgeLabel) {
        
        Direction d = TitanObjectFactory.createDirection(dir);
        Iterator<Edge> edges = element_.edges(d, edgeLabel);        
        Iterable<Edge> result = new EdgesIterable(edges);
        return new IterableAdapter<Edge, AAEdge<Vertex,Edge>>(result, EdgeMapper.INSTANCE);
        
    }

    @Override
    public Vertex getWrappedVertex() {

        return element_;
    }
    
    private TitanVertex getAsTitanVertex() {
        return (TitanVertex)element_;
    }

    @Override
    public Iterable<AAEdge<Vertex, Edge>> getEdges(AADirection in) {
        Direction d = TitanObjectFactory.createDirection(in);
        Iterator<Edge> edges = element_.edges(d);        
        Iterable<Edge> result = new EdgesIterable(edges);
        return new IterableAdapter<Edge, AAEdge<Vertex,Edge>>(result, EdgeMapper.INSTANCE);
    }

    @Override
    public<T> void addProperty(String propertyName, T value) {
       
       element_.property(VertexProperty.Cardinality.set, propertyName, value);
    }

    @Override
    public Collection<String> getPropertyValues(String propertyName) {
       
        Collection<String> result = new ArrayList<String>();
        Iterator<VertexProperty<String>> it = element_.properties(propertyName);
        while(it.hasNext()) {
            result.add(it.next().value());
        }
              
        return result;        
    }
    
    @Override
    public AAVertexQuery<Vertex, Edge> query() {
        
       return new Titan1VertexQuery(getAsTitanVertex().query());
    }

}
