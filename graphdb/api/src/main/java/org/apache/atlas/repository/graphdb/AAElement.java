
package org.apache.atlas.repository.graphdb;

import java.util.Collection;
import java.util.Set;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public interface AAElement {

    Object getId();

    Collection<? extends String> getPropertyKeys();

    <T> T getProperty(String propertyName);
    
    JSONObject toJson(Set<String> propertyKeys) throws JSONException;
    
    
}
