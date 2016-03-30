
package org.apache.atlas.repository.graphdb.titan1;

import java.util.Iterator;

import org.apache.atlas.repository.graphdb.AAIndexQuery;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.utils.IteratorAdapter;
import org.apache.atlas.utils.Mapper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.TitanVertex;

public class Titan1IndexQuery implements AAIndexQuery<Vertex, Edge> {

    private TitanIndexQuery query_;
    private static final IndexQueryResultMapper QUERY_RESULT_MAPPER = new IndexQueryResultMapper();

    public Titan1IndexQuery(TitanIndexQuery query) {
        query_ = query;
    }

    @Override
    public Iterator<AAIndexQuery.Result<Vertex, Edge>> vertices() {
        Iterator<TitanIndexQuery.Result<TitanVertex>> results = query_.vertices().iterator();

        return new IteratorAdapter<TitanIndexQuery.Result<TitanVertex>, AAIndexQuery.Result<Vertex, Edge>>(results,
                QUERY_RESULT_MAPPER);
    }

    private static final class IndexQueryResultMapper
            implements Mapper<TitanIndexQuery.Result<TitanVertex>, AAIndexQuery.Result<Vertex, Edge>> {
        
        @Override
        public AAIndexQuery.Result<Vertex, Edge> map(TitanIndexQuery.Result<TitanVertex> source) {
            return new ResultImpl(source);
        }
    }
    
    static class ResultImpl implements AAIndexQuery.Result<Vertex, Edge> {
        TitanIndexQuery.Result<TitanVertex> source_;

        public ResultImpl(TitanIndexQuery.Result<TitanVertex> source) {
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
