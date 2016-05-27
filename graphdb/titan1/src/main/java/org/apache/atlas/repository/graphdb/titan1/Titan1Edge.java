
package org.apache.atlas.repository.graphdb.titan1;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class Titan1Edge extends Titan1Element<Edge> implements AtlasEdge<Titan1Vertex, Titan1Edge> {


    public Titan1Edge(Edge edge) {
        super(edge);
    }

    @Override
    public String getLabel() {
        return getWrappedElement().label();
    }

    @Override
    public Titan1Edge getE() {

        return this;
    }

    @Override
    public AtlasVertex<Titan1Vertex, Titan1Edge> getInVertex() {
        return TitanObjectFactory.createVertex(getWrappedElement().inVertex());
    }

    @Override
    public AtlasVertex<Titan1Vertex, Titan1Edge> getOutVertex() {
        return TitanObjectFactory.createVertex(getWrappedElement().outVertex());
    }

}
