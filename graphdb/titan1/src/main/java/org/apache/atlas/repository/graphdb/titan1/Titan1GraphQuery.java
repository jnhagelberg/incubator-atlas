
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.adapters.IterableAdapter;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;

public class Titan1GraphQuery implements AtlasGraphQuery<Titan1Vertex, Titan1Edge> {

    private TitanGraphQuery<?> wrapped_;

    public Titan1GraphQuery(TitanGraphQuery<?> query) {
        wrapped_ = query;
    }

    @Override
    public AtlasGraphQuery<Titan1Vertex, Titan1Edge> has(String propertyKey, Object value) {

        TitanGraphQuery<?> result = wrapped_.has(propertyKey, value);
        return wrapResult(result);

    }

    private AtlasGraphQuery<Titan1Vertex, Titan1Edge> wrapResult(TitanGraphQuery<?> result) {
        if(result == wrapped_) {
            return this;
        }
        return TitanObjectFactory.createQuery(result);
    }

    @Override
    public Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>> vertices() {
        Iterable it = wrapped_.vertices();
        Iterable<Vertex> result = (Iterable<Vertex>)it;
        return new IterableAdapter<Vertex,AtlasVertex<Titan1Vertex, Titan1Edge>>(result, VertexMapper.INSTANCE);
    }

    @Override
    public Iterable<AtlasEdge<Titan1Vertex, Titan1Edge>> edges() {
        Iterable it = wrapped_.edges();
        Iterable<Edge> result = (Iterable<Edge>)it;
        return new IterableAdapter<Edge,AtlasEdge<Titan1Vertex, Titan1Edge>>(result, EdgeMapper.INSTANCE);
    }


    @Override
    public AtlasGraphQuery<Titan1Vertex, Titan1Edge> has(String propertyKey, ComparisionOperator operator, Object value) {
        Compare c = getGremlinPredicate(operator);
        TitanPredicate pred = TitanPredicate.Converter.convert(c);
        TitanGraphQuery<?> result = wrapped_.has(propertyKey, pred, value);
        return wrapResult(result);
    }

    private Compare getGremlinPredicate(ComparisionOperator op) {
        switch(op) {
            case EQUAL:
                return Compare.eq;
            case GREATER_THAN_EQUAL:
                return Compare.gte;
            case LESS_THAN_EQUAL:
                return Compare.lte;
            default:
            throw new RuntimeException("Unsupported comparison operator:" + op);
        }
    }

}
