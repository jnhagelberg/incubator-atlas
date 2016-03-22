
package org.apache.atlas.repository.graphdb;

import java.util.Collection;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;

public interface AAVertex<V,E> {

	Object getId();

	<T> T getProperty(String propertyName);

	void removeProperty(String propertyName);

	Set<String> getPropertyKeys();

	void setProperty(String propertyName, Object value);

	Iterable<AAEdge<V,E>> getEdges(AADirection out, String edgeLabel);
	
	V getWrappedVertex();

    Iterable<AAEdge<V,E>> getEdges(AADirection in);

    void addProperty(String propertyName, Object value);

    Collection<String> getPropertyValues(String traitNamesPropertyKey);
    
    JSONObject toJson(final Set<String> propertyKeys, final GraphSONMode mode) throws JSONException;

    AAVertexQuery<V,E> query();
}
