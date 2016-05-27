
package org.apache.atlas.repository.graphdb.titan1;

import java.util.Iterator;

import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.adapters.IteratorAdapter;
import org.apache.atlas.utils.adapters.Mapper;

import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.TitanVertex;

public class Titan1IndexQuery implements AtlasIndexQuery<Titan1Vertex, Titan1Edge> {

    private TitanIndexQuery query_;
    private static final IndexQueryResultMapper QUERY_RESULT_MAPPER = new IndexQueryResultMapper();

    public Titan1IndexQuery(TitanIndexQuery query) {
        query_ = query;
    }

    @Override
    public Iterator<AtlasIndexQuery.Result<Titan1Vertex, Titan1Edge>> vertices() {
        Iterator<TitanIndexQuery.Result<TitanVertex>> results = query_.vertices().iterator();

        return new IteratorAdapter<TitanIndexQuery.Result<TitanVertex>, AtlasIndexQuery.Result<Titan1Vertex, Titan1Edge>>(results,
                QUERY_RESULT_MAPPER);
    }

    private static final class IndexQueryResultMapper
            implements Mapper<TitanIndexQuery.Result<TitanVertex>, AtlasIndexQuery.Result<Titan1Vertex, Titan1Edge>> {

        @Override
        public AtlasIndexQuery.Result<Titan1Vertex, Titan1Edge> map(TitanIndexQuery.Result<TitanVertex> source) {
            return new ResultImpl(source);
        }
    }

    static class ResultImpl implements AtlasIndexQuery.Result<Titan1Vertex, Titan1Edge> {
        TitanIndexQuery.Result<TitanVertex> source_;

        public ResultImpl(TitanIndexQuery.Result<TitanVertex> source) {
            source_ = source;
        }

        @Override
        public AtlasVertex<Titan1Vertex, Titan1Edge> getVertex() {
            return TitanObjectFactory.createVertex(source_.getElement());
        }

        @Override
        public double getScore() {
            return source_.getScore();
        }
    }
}
