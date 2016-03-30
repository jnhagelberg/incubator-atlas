
package org.apache.atlas.repository.graphdb.titan0;

import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.utils.Mapper;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class EdgeMapper implements Mapper<Edge, AAEdge<Vertex, Edge>> {
    
    public static EdgeMapper INSTANCE = new EdgeMapper();
    
    private EdgeMapper() {
        
    }
    @Override
    public AAEdge<Vertex, Edge> map(Edge source) {
       return  TitanObjectFactory.createEdge(source);
    }
}