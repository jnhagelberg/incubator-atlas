
package org.apache.atlas.repository.graphdb.titan0;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.utils.adapters.IterableAdapter;
import org.apache.atlas.utils.adapters.impl.EdgeMapper;

import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Titan0Vertex extends Titan0Element<Vertex> implements AtlasVertex<Titan0Vertex, Titan0Edge> {

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
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> getEdges(AtlasEdgeDirection dir, String edgeLabel) {
        Iterable<Edge> titanEdges = element_.getEdges(
                TitanObjectFactory.createDirection(dir), edgeLabel);
        return new IterableAdapter<>(titanEdges, EdgeMapper.INSTANCE);
    } 
    
    private TitanVertex getAsTitanVertex() {
        return (TitanVertex)element_;
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> getEdges(AtlasEdgeDirection in) {
        Iterable<Edge> titanResult = element_.getEdges(TitanObjectFactory.createDirection(in));
        return new IterableAdapter<Edge, AtlasEdge<Titan0Vertex, Titan0Edge>>(titanResult, EdgeMapper.INSTANCE);
    }

    @Override
    public <T> void addProperty(String propertyName, T value) {
       getAsTitanVertex().addProperty(propertyName, value);        
    }

    @Override
    public <T> Collection<T> getPropertyValues(String key) {
        
        TitanVertex tv = getAsTitanVertex();
        Collection<T> result = new ArrayList<T>();
        for (TitanProperty property : tv.getProperties(key)) {
            result.add((T) property.getValue());
        }
        return result;        
    }   

    @Override
    public AtlasVertexQuery<Titan0Vertex, Titan0Edge> query() {
       return new Titan0VertexQuery(element_.query());
    }

    @Override
    public Titan0Vertex getV() {
        
        return this;
    }  

}
