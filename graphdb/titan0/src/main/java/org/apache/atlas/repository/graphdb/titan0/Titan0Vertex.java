
package org.apache.atlas.repository.graphdb.titan0;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.repository.graphdb.AAVertexQuery;
import org.apache.atlas.utils.IterableAdapter;

import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Titan0Vertex extends Titan0Element<Vertex> implements AAVertex<Vertex, Edge> {

    public Titan0Vertex(Vertex source) {
        super(source);
    }

    @Override
    public void removeProperty(String propertyName) {
        element_.removeProperty(propertyName);
        
    }
    
    @Override
    public <T> void setProperty(String propertyName, T value) {
        element_.setProperty(propertyName, value);
        
    }

    @Override
    public Iterable<AAEdge<Vertex, Edge>> getEdges(AADirection dir, String edgeLabel) {
        Iterable<Edge> titanEdges = element_.getEdges(
                TitanObjectFactory.createDirection(dir), edgeLabel);
        return new IterableAdapter<>(titanEdges, EdgeMapper.INSTANCE);
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
        Iterable<Edge> titanResult = element_.getEdges(TitanObjectFactory.createDirection(in));
        return new IterableAdapter<Edge, AAEdge<Vertex,Edge>>(titanResult, EdgeMapper.INSTANCE);
    }

    @Override
    public <T> void addProperty(String propertyName, T value) {
       getAsTitanVertex().addProperty(propertyName, value);        
    }

    @Override
    public Collection<String> getPropertyValues(String traitNamesPropertyKey) {
        
        TitanVertex tv = getAsTitanVertex();
        Collection<String> result = new ArrayList<String>();
        for (TitanProperty property : tv.getProperties(traitNamesPropertyKey)) {
            result.add((String) property.getValue());
        }
        return result;        
    }   

    @Override
    public AAVertexQuery<Vertex, Edge> query() {
       return new Titan0VertexQuery(element_.query());
    }  

}
