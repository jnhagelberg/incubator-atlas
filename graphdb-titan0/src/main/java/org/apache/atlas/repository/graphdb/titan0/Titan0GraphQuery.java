
package org.apache.atlas.repository.graphdb.titan0;

import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAGraphQuery;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.utils.IterableAdapter;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class Titan0GraphQuery implements AAGraphQuery<Vertex,Edge> {

    private GraphQuery wrapped_;
    
    public Titan0GraphQuery(GraphQuery query) {
        wrapped_ = query;
    }

    @Override
    public AAGraphQuery<Vertex,Edge> has(String propertyKey, Object value) {
        GraphQuery result = wrapped_.has(propertyKey, value);
        if(result == wrapped_) {
            return this;
        }
        return TitanObjectFactory.createQuery(result);
        
    }

    @Override
    public Iterable<AAVertex<Vertex,Edge>> vertices() {
        Iterable<Vertex> result = wrapped_.vertices();
        return new IterableAdapter<Vertex,AAVertex<Vertex,Edge>>(result, VertexMapper.INSTANCE);
    }

    @Override
    public Iterable<AAEdge<Vertex, Edge>> edges() {
        Iterable<Edge> result = wrapped_.edges();
        return new IterableAdapter<Edge,AAEdge<Vertex,Edge>>(result, EdgeMapper.INSTANCE);
    }

}
