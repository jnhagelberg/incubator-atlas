
package org.apache.atlas.repository.graphdb;

public interface AAEdge<V,E> extends AAElement {

	AAVertex<V,E> getVertex(AADirection in);

	String getLabel();

	E getWrappedEdge();

}
