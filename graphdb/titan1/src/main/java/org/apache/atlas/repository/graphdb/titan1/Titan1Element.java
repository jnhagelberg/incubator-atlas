
package org.apache.atlas.repository.graphdb.titan1;

import java.util.Set;

import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.titan1.graphson.AtlasGraphSONMode;
import org.apache.atlas.repository.graphdb.titan1.graphson.AtlasGraphSONUtility;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class Titan1Element<T extends Element> implements AtlasElement {

    
    protected T element_;
    
    public Titan1Element(T element) {
        element_ = element;
    }
    
    @Override
    public <T> T getProperty(String propertyName) {
        
        
        //add explicit logic to return null if the property does not exist
        //This is the behavior Atlas expects.  Titan 1 throws an exception
        //in this scenario.       
        Property p = element_.property(propertyName);
        if(p.isPresent()) {
            return (T)p.value();
        }
        return null;
    }
    
    @Override
    public Set<String> getPropertyKeys() {
        return element_.keys();
    }

    @Override
    public Object getId() {
        return element_.id();
    }
    

    //not in interface
    public T getWrappedElement() {
        return element_;
    }
    
    @Override
    public JSONObject toJson(Set<String> propertyKeys) throws JSONException {
        
        return AtlasGraphSONUtility.jsonFromElement(this, propertyKeys, AtlasGraphSONMode.NORMAL);
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
        Titan1Element otherElement = (Titan1Element) other;
        return getWrappedElement().equals(otherElement.getWrappedElement());
    }
    
}
