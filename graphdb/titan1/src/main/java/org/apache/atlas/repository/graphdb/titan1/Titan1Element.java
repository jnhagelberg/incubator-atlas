
package org.apache.atlas.repository.graphdb.titan1;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.titan1.graphson.AtlasGraphSONMode;
import org.apache.atlas.repository.graphdb.titan1.graphson.AtlasGraphSONUtility;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanGraph;


public class Titan1Element<T extends Element> implements AtlasElement {


    private T element_;
    protected Object id_;
    private TitanGraph graph_;


    public Titan1Element(TitanGraph graph, String id) {
        id_ = id;
        graph_ = graph;
    }


    public Titan1Element(T element) {
        element_ = element;
        id_ = element.id();
    }

    @Override
    public <T> T getProperty(String propertyName, Class<T> clazz) {


        //add explicit logic to return null if the property does not exist
        //This is the behavior Atlas expects.  Titan 1 throws an exception
        //in this scenario.
        Property p = getWrappedElement().property(propertyName);
        if(p.isPresent()) {
            return (T)p.value();
        }
        return null;
    }

    /**
     * Gets all of the values of the given property.
     * @param propertyName
     * @return
     */
    @Override
    public <T> Collection<T> getPropertyValues(String propertyName, Class<T> type) {
        return Collections.singleton(getProperty(propertyName, type));
    }
    
    @Override
    public Set<String> getPropertyKeys() {
        return getWrappedElement().keys();
    }

    @Override
    public void removeProperty(String propertyName) {
        Iterator<? extends Property<String>> it = getWrappedElement().properties(propertyName);
        while(it.hasNext()) {
            Property<String> property = it.next();
            property.remove();
        }
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        try {
            getWrappedElement().property(propertyName, value);
        }
        catch(SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public Object getId() {
        return id_;
    }


    //not in interface
    public T getWrappedElement() {

        T element = getElement();
        if(element == null) {
            throw new IllegalStateException("The vertex " + id_ + " does not exist!");
        }
        return element;
    }

    private T getElement() {

        if(element_ != null) {
            return element_;
        }

        if(getClass() == Titan1Vertex.class) {
            Iterator<Vertex> it = graph_.vertices(id_);
            if(! it.hasNext()) {
                return null;
            }
            element_ = (T)it.next();
            return element_;
        }
        return null;
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

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasElement#getListProperty(java.lang.String)
     */
    @Override
    public List<String> getListProperty(String propertyName) {
        return getProperty(propertyName, List.class);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasElement#setListProperty(java.lang.String, java.util.List)
     */
    @Override
    public void setListProperty(String propertyName, List<String> values) {
        setProperty(propertyName, values);

    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasElement#exists()
     */
    @Override
    public boolean exists() {
        return getElement() != null;
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasElement#setJsonProperty(java.lang.String, java.lang.Object)
     */
    @Override
    public <T> void setJsonProperty(String propertyName, T value) {
        setProperty(propertyName, value);

    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasElement#getJsonProperty(java.lang.String)
     */
    @Override
    public <T> T getJsonProperty(String propertyName) {
       return (T)getProperty(propertyName, String.class);
    }
    
    @Override
    public String getIdForDisplay() {
        return getId().toString();
    }

}
