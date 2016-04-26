
package org.apache.atlas.repository.graphdb.titan0;

import java.util.Set;

import org.apache.atlas.repository.graphdb.AtlasElement;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;

public class Titan0Element<T extends Element> implements AtlasElement {

    protected T element_;
    
    public Titan0Element(T element) {
        element_ = element;
    }
    
    @Override
    public Object getId() {
        return element_.getId();
    }

    @Override
    public Set<String> getPropertyKeys() {
        return element_.getPropertyKeys();
    }

    @Override
    public <T> T getProperty(String propertyName) {
        return element_.getProperty(propertyName);
    }

    @Override
    public JSONObject toJson(Set<String> propertyKeys) throws JSONException {
        return GraphSONUtility.jsonFromElement(element_, propertyKeys, GraphSONMode.NORMAL);
    }

    //not in interface
    public T getWrappedElement() {
        return element_;
    }
    
    @Override
    public int hashCode() {
        int result = 37;
        result = 17*result + getClass().hashCode();
        result = 17*result + getWrappedElement().hashCode();
        return result;
    }    
    
    @Override
    public boolean equals(Object other) {
        if(other.getClass() != getClass()) {
            return false;
        }
        Titan0Element otherElement = (Titan0Element) other;
        return getWrappedElement().equals(otherElement.getWrappedElement());
    }
}
