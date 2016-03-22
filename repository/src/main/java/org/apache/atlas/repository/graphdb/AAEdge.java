
package org.apache.atlas.repository.graphdb;

import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;

public interface AAEdge<V,E> {

	AAVertex<V,E> getVertex(AADirection in);

	String getLabel();

	Object getId();
	
	JSONObject toJson(final Set<String> propertyKeys, final GraphSONMode mode) throws JSONException;

	E getWrappedEdge();

}
