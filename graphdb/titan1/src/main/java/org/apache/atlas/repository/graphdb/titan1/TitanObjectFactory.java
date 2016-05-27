
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;

public class TitanObjectFactory {

    public static Titan1Edge createEdge(Edge source) {
        if(source == null) {
            return null;
        }
        return new Titan1Edge(source);
    }

    public static Direction createDirection(AtlasEdgeDirection dir) {
        switch(dir) {
        case IN:
            return Direction.IN;
        case OUT:
            return Direction.OUT;
        case BOTH:
            return Direction.BOTH;
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

    public static Titan1Vertex createVertex(Vertex source) {
        if(source == null) {
            return null;
        }
        return new Titan1Vertex(source);
    }

    public static PropertyKey createPropertyKey(AtlasPropertyKey key) {
        return ((Titan1PropertyKey)key).getWrappedPropertyKey();
    }

    /**
     * @param propertyKey
     * @return
     */
    public static Titan1PropertyKey createPropertyKey(PropertyKey propertyKey) {
        if(propertyKey == null) {
            return null;
        }
        return new Titan1PropertyKey(propertyKey);
    }

    /**
     * @param index
     * @return
     */
    public static AtlasGraphIndex createGraphIndex(TitanGraphIndex index) {
        if(index == null) {
            return null;
        }
        return new Titan1GraphIndex(index);
    }
}
