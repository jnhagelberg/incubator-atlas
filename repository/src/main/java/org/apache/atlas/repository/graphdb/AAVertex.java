
package org.apache.atlas.repository.graphdb;

import java.util.Collection;
import java.util.List;

public interface AAVertex<V,E> extends AAElement {

	void removeProperty(String propertyName);

	<T> void setProperty(String propertyName, T value);		
	
    <T> void addProperty(String propertyName, T value);

	Iterable<AAEdge<V,E>> getEdges(AADirection out, String edgeLabel);
	
	V getWrappedVertex();

    Iterable<AAEdge<V,E>> getEdges(AADirection in);

    Collection<String> getPropertyValues(String propertyName);    

    AAVertexQuery<V,E> query();
}
