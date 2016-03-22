
package org.apache.atlas.repository.graphdb.titan0;

import org.apache.atlas.repository.graph.util.IterableAdapter;
import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.repository.graphdb.AAVertexQuery;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class Titan0VertexQuery implements AAVertexQuery<Vertex, Edge> {

    private VertexQuery query_;

    public Titan0VertexQuery(VertexQuery query) {
        query_ = query;
    }

    @Override
    public AAVertexQuery<Vertex, Edge> direction(AADirection queryDirection) {
        query_.direction(TitanObjectFactory.createDirection(queryDirection));
        return this;
        
    }

    @Override
    public Iterable<AAVertex<Vertex, Edge>> vertices() {
        Iterable<Vertex> vertices = query_.vertices();
        return new IterableAdapter<Vertex,AAVertex<Vertex, Edge>>(vertices, VertexMapper.INSTANCE);
    }

    @Override
    public Iterable<AAEdge<Vertex, Edge>> edges() {
        Iterable<Edge> vertices = query_.edges();
        return new IterableAdapter<Edge,AAEdge<Vertex, Edge>>(vertices, EdgeMapper.INSTANCE);
   
    }

    @Override
    public long count() {
        return query_.count();
    }
    
    
}