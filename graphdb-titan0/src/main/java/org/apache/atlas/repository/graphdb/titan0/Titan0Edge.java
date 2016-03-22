
package org.apache.atlas.repository.graphdb.titan0;
import java.util.Set;

import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;

public class Titan0Edge implements AAEdge<Vertex,Edge> {
    
    private Edge edge_;

    public Titan0Edge(Edge edge) {
        edge_ = edge;
    }
    

    @Override
    public String getLabel() {
        return edge_.getLabel();
    }

    @Override
    public Object getId() {
        return edge_.getId();
    }

    @Override
    public Edge getWrappedEdge() {
        return edge_;
    }
    @Override
    public JSONObject toJson(Set<String> propertyKeys, GraphSONMode mode) throws JSONException {
        return GraphSONUtility.jsonFromElement(edge_, propertyKeys, GraphSONMode.NORMAL);
    }

    
    @Override
    public AAVertex<Vertex,Edge> getVertex(AADirection in) {
        Direction dir = TitanObjectFactory.createDirection(in);
        Vertex v = edge_.getVertex(dir);
        return TitanObjectFactory.createVertex(v);
    }

    
}
