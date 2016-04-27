
package org.apache.atlas.repository.graphdb.titan0;

import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.types.Multiplicity;

import com.thinkaurelius.titan.core.Cardinality;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class TitanObjectFactory {
    
    private TitanObjectFactory() {
        
    }
    
    public static Titan0Edge createEdge(Edge source) {
        if(source == null) {
            return null;
        }
        return new Titan0Edge(source);
    }
    
    public static Direction createDirection(AtlasEdgeDirection dir) {
        switch(dir) {
        case IN:
            return Direction.IN;      
        case OUT:
            return Direction.OUT;
        default:
            throw new RuntimeException("Unrecognized direction: " + dir);
        }
     }
    
    
    public static Cardinality createCardinality(Multiplicity multiplicity) {
        if (multiplicity == Multiplicity.OPTIONAL || multiplicity == Multiplicity.REQUIRED) {
            return Cardinality.SINGLE;
        } else if (multiplicity == Multiplicity.COLLECTION) {
            return Cardinality.LIST;
        } else if (multiplicity == Multiplicity.SET) {
            return Cardinality.SET;
        }
        // default to LIST as this is the most forgiving
        return Cardinality.LIST;
    }

        

    public static Titan0GraphQuery createQuery(GraphQuery query) {
       return new Titan0GraphQuery(query);
    }

    public static AtlasVertex<Titan0Vertex, Titan0Edge> createVertex(Vertex source) {
        if(source == null) {
            return null;
        }
        return new Titan0Vertex(source);
    }
 
}
