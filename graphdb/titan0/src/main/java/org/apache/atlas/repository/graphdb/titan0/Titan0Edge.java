
package org.apache.atlas.repository.graphdb.titan0;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Titan0Edge extends Titan0Element<Edge> implements AtlasEdge<Titan0Vertex, Titan0Edge> {       
    

    public Titan0Edge(Edge edge) {
        super(edge);
    }
    
    @Override
    public String getLabel() {
        return element_.getLabel();
    }      

    @Override
    public Titan0Edge getE() {
        return this;
    }

    @Override
    public AtlasVertex<Titan0Vertex, Titan0Edge> getInVertex() {
        Vertex v = element_.getVertex(Direction.IN);
        return TitanObjectFactory.createVertex(v);
    }

    @Override
    public AtlasVertex<Titan0Vertex, Titan0Edge> getOutVertex() {
        Vertex v = element_.getVertex(Direction.OUT);
        return TitanObjectFactory.createVertex(v);
    }
}
