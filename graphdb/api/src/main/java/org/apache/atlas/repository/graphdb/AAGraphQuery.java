
package org.apache.atlas.repository.graphdb;

public interface AAGraphQuery<V,E> {

	AAGraphQuery<V,E> has(String propertyKey, Object value);

	Iterable<AAVertex<V, E>> vertices();

    Iterable<AAEdge<V, E>> edges();

    AAGraphQuery<V,E> has(String propertyKey, ComparisionOperator compMethod, Object value);
    
    public static enum ComparisionOperator {
        GREATER_THAN_EQUAL,
        EQUAL,
        LESS_THAN_EQUAL
    }

}
