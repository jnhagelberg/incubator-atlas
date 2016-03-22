
package org.apache.atlas.repository.graphdb.titan0;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;

import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.util.IterableAdapter;
import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.repository.graphdb.AAVertexQuery;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;

public class Titan0Vertex implements AAVertex<Vertex, Edge> {

    private Vertex vertex_;
    
    public Titan0Vertex(Vertex source) {
        vertex_ = source;
    }

    @Override
    public Object getId() {
        return vertex_.getId();
    }

    @Override
    public <T> T getProperty(String propertyName) {
        return vertex_.getProperty(propertyName);
    }

    @Override
    public void removeProperty(String propertyName) {
        vertex_.removeProperty(propertyName);
        
    }

    @Override
    public Set<String> getPropertyKeys() {
        return vertex_.getPropertyKeys();
    }

    @Override
    public void setProperty(String propertyName, Object value) {
        vertex_.setProperty(propertyName, value);
        
    }

    @Override
    public Iterable<AAEdge<Vertex, Edge>> getEdges(AADirection dir, String edgeLabel) {
        Iterable<Edge> titanEdges = vertex_.getEdges(
                TitanObjectFactory.createDirection(dir), edgeLabel);
        return new IterableAdapter<>(titanEdges, EdgeMapper.INSTANCE);
    }

    @Override
    public Vertex getWrappedVertex() {

        return vertex_;
    }
    
    private TitanVertex getAsTitanVertex() {
        return (TitanVertex)vertex_;
    }

    @Override
    public Iterable<AAEdge<Vertex, Edge>> getEdges(AADirection in) {
        Iterable<Edge> titanResult = vertex_.getEdges(TitanObjectFactory.createDirection(in));
        return new IterableAdapter<Edge, AAEdge<Vertex,Edge>>(titanResult, EdgeMapper.INSTANCE);
    }

    @Override
    public void addProperty(String propertyName, Object value) {
       getAsTitanVertex().addProperty(propertyName, value);        
    }

    @Override
    public Collection<String> getPropertyValues(String traitNamesPropertyKey) {
        
        TitanVertex tv = getAsTitanVertex();
        Collection<String> result = new ArrayList<String>();
        for (TitanProperty property : tv.getProperties(Constants.TRAIT_NAMES_PROPERTY_KEY)) {
            result.add((String) property.getValue());
        }
        return result;        
    }

    @Override
    public JSONObject toJson(Set<String> propertyKeys, GraphSONMode mode) throws JSONException {
        return GraphSONUtility.jsonFromElement(vertex_, propertyKeys, GraphSONMode.NORMAL);
    }

    @Override
    public AAVertexQuery<Vertex, Edge> query() {
       return new Titan0VertexQuery(vertex_.query());
    }

}
