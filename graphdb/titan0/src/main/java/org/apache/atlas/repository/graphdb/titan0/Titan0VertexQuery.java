
package org.apache.atlas.repository.graphdb.titan0;

import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.utils.adapters.IterableAdapter;
import org.apache.atlas.utils.adapters.impl.EdgeMapper;
import org.apache.atlas.utils.adapters.impl.VertexMapper;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;

public class Titan0VertexQuery implements AtlasVertexQuery<Titan0Vertex, Titan0Edge> {

    private VertexQuery query_;

    public Titan0VertexQuery(VertexQuery vertexQuery) {
        query_ = vertexQuery;
    }

    @Override
    public AtlasVertexQuery<Titan0Vertex, Titan0Edge> direction(AtlasEdgeDirection queryDirection) {
        query_.direction(TitanObjectFactory.createDirection(queryDirection));
        return this;
        
    }

    @Override
    public Iterable<AtlasVertex<Titan0Vertex, Titan0Edge>> vertices() {
        Iterable<Vertex> vertices = query_.vertices();
        return new IterableAdapter<Vertex,AtlasVertex<Titan0Vertex, Titan0Edge>>(vertices, VertexMapper.INSTANCE);
    }

    @Override
    public Iterable<AtlasEdge<Titan0Vertex, Titan0Edge>> edges() {
        Iterable<Edge> vertices = query_.edges();
        return new IterableAdapter<Edge,AtlasEdge<Titan0Vertex, Titan0Edge>>(vertices, EdgeMapper.INSTANCE);
   
    }

    @Override
    public long count() {
        return query_.count();
    }
    
    
}