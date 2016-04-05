
package org.apache.atlas.repository.graphdb;

public interface AAVertexQuery<V,E> {

    AAVertexQuery<V,E> direction(AADirection queryDirection);

    Iterable<AAVertex<V,E>> vertices();

    Iterable<AAEdge<V,E>> edges();

    long count();

}
