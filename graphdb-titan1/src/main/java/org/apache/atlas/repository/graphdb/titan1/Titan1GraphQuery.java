
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAGraphQuery;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.utils.IterableAdapter;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanGraphQuery;

public class Titan1GraphQuery implements AAGraphQuery<Vertex,Edge> {

    private TitanGraphQuery<?> wrapped_;
    
    public Titan1GraphQuery(TitanGraphQuery<?> query) {
        wrapped_ = query;
    }

    @Override
    public AAGraphQuery<Vertex,Edge> has(String propertyKey, Object value) {
       
        TitanGraphQuery<?> result = wrapped_.has(propertyKey, value);
        if(result == wrapped_) {
            return this;
        }
        return TitanObjectFactory.createQuery(result);
        
    }

    @Override
    public Iterable<AAVertex<Vertex,Edge>> vertices() {
        Iterable it = wrapped_.vertices();        
        Iterable<Vertex> result = (Iterable<Vertex>)it;
        return new IterableAdapter<Vertex,AAVertex<Vertex,Edge>>(result, VertexMapper.INSTANCE);
    }
    
    @Override
    public Iterable<AAEdge<Vertex,Edge>> edges() {
        Iterable it = wrapped_.edges();        
        Iterable<Edge> result = (Iterable<Edge>)it;
        return new IterableAdapter<Edge,AAEdge<Vertex,Edge>>(result, EdgeMapper.INSTANCE);
    }

}
