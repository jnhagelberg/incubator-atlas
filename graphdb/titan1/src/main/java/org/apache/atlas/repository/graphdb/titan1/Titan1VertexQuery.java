
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.utils.adapters.IterableAdapter;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanVertexQuery;

public class Titan1VertexQuery implements AtlasVertexQuery<Titan1Vertex, Titan1Edge> {

    private TitanVertexQuery<?> query_;

    public Titan1VertexQuery(TitanVertexQuery<?> query) {
        query_ = query;
    }

    @Override
    public AtlasVertexQuery<Titan1Vertex, Titan1Edge> direction(AtlasEdgeDirection queryDirection) {
        query_.direction(TitanObjectFactory.createDirection(queryDirection));
        return this;

    }

    @Override
    public Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>> vertices() {
        Iterable vertices = query_.vertices();
        return new IterableAdapter<Vertex,AtlasVertex<Titan1Vertex, Titan1Edge>>(vertices, VertexMapper.INSTANCE);
    }

    @Override
    public Iterable<AtlasEdge<Titan1Vertex, Titan1Edge>> edges() {
        Iterable vertices = query_.edges();
        return new IterableAdapter<Edge,AtlasEdge<Titan1Vertex, Titan1Edge>>(vertices, EdgeMapper.INSTANCE);

    }

    @Override
    public long count() {
        return query_.count();
    }


}