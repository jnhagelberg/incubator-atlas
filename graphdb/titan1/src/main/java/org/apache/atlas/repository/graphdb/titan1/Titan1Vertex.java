
package org.apache.atlas.repository.graphdb.titan1;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasVertexQuery;
import org.apache.atlas.utils.adapters.IterableAdapter;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;

public class Titan1Vertex extends Titan1Element<Vertex> implements AtlasVertex<Titan1Vertex, Titan1Edge> {

    public Titan1Vertex(TitanGraph graph, String id) {
        super(graph, id);
    }

    public Titan1Vertex(Vertex source) {
        super(source);
    }

    @Override
    public<T> void addProperty(String propertyName, T value) {
       try {
           getWrappedElement().property(VertexProperty.Cardinality.set, propertyName, value);
       }
       catch(SchemaViolationException e) {
           throw new AtlasSchemaViolationException(e);
       }
    }



    @Override
    public Iterable<AtlasEdge<Titan1Vertex, Titan1Edge>> getEdges(AtlasEdgeDirection dir, String edgeLabel) {

        Direction d = TitanObjectFactory.createDirection(dir);
        Iterator<Edge> edges = getWrappedElement().edges(d, edgeLabel);
        Iterable<Edge> result = new EdgesIterable(edges);
        return new IterableAdapter<Edge, AtlasEdge<Titan1Vertex, Titan1Edge>>(result, EdgeMapper.INSTANCE);

    }

    private TitanVertex getAsTitanVertex() {
        return (TitanVertex)getWrappedElement();
    }

    @Override
    public Iterable<AtlasEdge<Titan1Vertex, Titan1Edge>> getEdges(AtlasEdgeDirection in) {
        Direction d = TitanObjectFactory.createDirection(in);
        Iterator<Edge> edges = getWrappedElement().edges(d);
        Iterable<Edge> result = new EdgesIterable(edges);
        return new IterableAdapter<Edge, AtlasEdge<Titan1Vertex, Titan1Edge>>(result, EdgeMapper.INSTANCE);
    }

    @Override
    public <T> Collection<T> getPropertyValues(String propertyName) {

        Collection<T> result = new ArrayList<T>();
        Iterator<VertexProperty<T>> it = getWrappedElement().properties(propertyName);
        while(it.hasNext()) {
            result.add(it.next().value());
        }

        return result;
    }

    @Override
    public AtlasVertexQuery<Titan1Vertex, Titan1Edge> query() {

       return new Titan1VertexQuery(getAsTitanVertex().query());
    }


    @Override
    public Titan1Vertex getV() {
        return this;
    }

    private static final class EdgesIterable implements Iterable<Edge> {
        private final Iterator<Edge> edges;

        private EdgesIterable(Iterator<Edge> edges) {
            this.edges = edges;
        }

        @Override
        public Iterator<Edge> iterator() {
            return edges;
        }
    }


}
