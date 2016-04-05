
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.repository.graphdb.AAVertexQuery;
import org.apache.atlas.utils.IterableAdapter;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanVertexQuery;

public class Titan1VertexQuery implements AAVertexQuery<Vertex, Edge> {

    private TitanVertexQuery<?> query_;

    public Titan1VertexQuery(TitanVertexQuery<?> query) {       
        query_ = query;
    }

    @Override
    public AAVertexQuery<Vertex, Edge> direction(AADirection queryDirection) {
        query_.direction(TitanObjectFactory.createDirection(queryDirection));
        return this;
        
    }

    @Override
    public Iterable<AAVertex<Vertex, Edge>> vertices() {
        Iterable vertices = query_.vertices();
        return new IterableAdapter<Vertex,AAVertex<Vertex, Edge>>(vertices, VertexMapper.INSTANCE);
    }

    @Override
    public Iterable<AAEdge<Vertex, Edge>> edges() {
        Iterable vertices = query_.edges();
        return new IterableAdapter<Edge,AAEdge<Vertex, Edge>>(vertices, EdgeMapper.INSTANCE);
   
    }

    @Override
    public long count() {
        return query_.count();
    }
    
    
}