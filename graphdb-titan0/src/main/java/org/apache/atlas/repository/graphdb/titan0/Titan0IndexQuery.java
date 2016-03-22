
package org.apache.atlas.repository.graphdb.titan0;

import java.util.Iterator;

import org.apache.atlas.repository.graph.util.IteratorAdapter;
import org.apache.atlas.repository.graph.util.Mapper;
import org.apache.atlas.repository.graphdb.AAIndexQuery;
import org.apache.atlas.repository.graphdb.AAVertex;

import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Titan0IndexQuery implements AAIndexQuery<Vertex, Edge> {

    private TitanIndexQuery query_;
    private static final IndexQueryResultMapper QUERY_RESULT_MAPPER = new IndexQueryResultMapper();

    public Titan0IndexQuery(TitanIndexQuery query) {
        query_ = query;
    }

    @Override
    public Iterator<AAIndexQuery.Result<Vertex, Edge>> vertices() {
        Iterator<TitanIndexQuery.Result<Vertex>> results = query_.vertices().iterator();

        return new IteratorAdapter<TitanIndexQuery.Result<Vertex>, AAIndexQuery.Result<Vertex, Edge>>(results,
                QUERY_RESULT_MAPPER);
    }

    private static final class IndexQueryResultMapper
            implements Mapper<TitanIndexQuery.Result<Vertex>, AAIndexQuery.Result<Vertex, Edge>> {
        @Override
        public AAIndexQuery.Result<Vertex, Edge> map(TitanIndexQuery.Result<Vertex> source) {
            return new ResultImpl(source);
        }
    }
    
    static class ResultImpl implements AAIndexQuery.Result<Vertex, Edge> {
        TitanIndexQuery.Result<Vertex> source_;

        public ResultImpl(TitanIndexQuery.Result<Vertex> source) {
            source_ = source;
        }

        @Override
        public AAVertex<Vertex, Edge> getVertex() {
            return TitanObjectFactory.createVertex(source_.getElement());
        }

        @Override
        public double getScore() {
            return source_.getScore();
        }
    }
}
