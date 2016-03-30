
package org.apache.atlas.repository.graphdb.titan0;
import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class Titan0Edge extends Titan0Element<Edge> implements AAEdge<Vertex,Edge> {       

    public Titan0Edge(Edge edge) {
        super(edge);
    }
    
    @Override
    public String getLabel() {
        return element_.getLabel();
    }

    @Override
    public Edge getWrappedEdge() {
        return element_;
    } 
    
    @Override
    public AAVertex<Vertex,Edge> getVertex(AADirection in) {
        Direction dir = TitanObjectFactory.createDirection(in);
        Vertex v = element_.getVertex(dir);
        return TitanObjectFactory.createVertex(v);
    }

    
}
