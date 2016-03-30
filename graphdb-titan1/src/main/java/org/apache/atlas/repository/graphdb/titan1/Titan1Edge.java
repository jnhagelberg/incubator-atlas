
package org.apache.atlas.repository.graphdb.titan1;
import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class Titan1Edge extends Titan1Element<Edge> implements AAEdge<Vertex,Edge> {
    

    public Titan1Edge(Edge edge) {
        super(edge);
    }
    
    @Override
    public String getLabel() {
        return element_.label();
    }

    @Override
    public Edge getWrappedEdge() {
        return element_;
    }

    @Override
    public AAVertex<Vertex,Edge> getVertex(AADirection in) {     
        Vertex v;
        
        if(in == AADirection.IN) {
            v = element_.inVertex();
        }
        else if(in == AADirection.OUT) {
            v = element_.outVertex();
        }
        else {
            throw new RuntimeException("Invalid direction: " + in);
        }
                        
        return TitanObjectFactory.createVertex(v);
    }
    
}
