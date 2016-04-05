
package org.apache.atlas.repository.graphdb.titan0;

import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.utils.LruMap;

import com.thinkaurelius.titan.core.Cardinality;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;

public class TitanObjectFactory {

    private static LruMap<Edge, Titan0Edge> edgeMap = new LruMap<Edge, Titan0Edge>(1000);
    private static LruMap<Vertex, Titan0Vertex> vertexMap = new LruMap<Vertex, Titan0Vertex>(1000);
    
    public static Titan0Edge createEdge(Edge source) {
        Titan0Edge result = edgeMap.get(source);
        if(result == null) {
            result = new Titan0Edge(source);
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

        

    public static Titan0GraphQuery createQuery(GraphQuery query) {
       return new Titan0GraphQuery(query);
    }

    public static AAVertex<Vertex, Edge> createVertex(Vertex source) {
        Titan0Vertex result = vertexMap.get(source);
        if(result == null) {
            result = new Titan0Vertex(source);
            vertexMap.put(source, result);
        }
        return result;
    }

   
    
    
}
