
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.utils.LruMap;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.TitanGraphQuery;

public class TitanObjectFactory {

    private static LruMap<Edge, Titan1Edge> edgeMap = new LruMap<Edge, Titan1Edge>(1000);
    private static LruMap<Vertex, Titan1Vertex> vertexMap = new LruMap<Vertex, Titan1Vertex>(1000);
    
    public static Titan1Edge createEdge(Edge source) {
        Titan1Edge result = edgeMap.get(source);
        if(result == null) {
            result = new Titan1Edge(source);
            edgeMap.put(source, result);
        }
        return result;
    }
    
    public static Direction createDirection(AADirection dir) {
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
        // todo - default to LIST as this is the most forgiving
        return Cardinality.LIST;
    }

        

    public static Titan1GraphQuery createQuery(TitanGraphQuery<?> query) {
       return new Titan1GraphQuery(query);
    }

    public static AAVertex<Vertex, Edge> createVertex(Vertex source) {
        Titan1Vertex result = vertexMap.get(source);
        if(result == null) {
            result = new Titan1Vertex(source);
            vertexMap.put(source, result);
        }
        return result;
    }

   
    
    
}
