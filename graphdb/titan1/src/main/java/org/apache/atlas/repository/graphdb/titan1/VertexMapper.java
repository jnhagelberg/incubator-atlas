
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.utils.Mapper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class VertexMapper implements Mapper<Vertex, AAVertex<Vertex, Edge>> {
    
    public static VertexMapper INSTANCE = new VertexMapper();
    
    private VertexMapper() {
        
    }
    @Override
    public AAVertex<Vertex, Edge> map(Vertex source) {
       return TitanObjectFactory.createVertex(source);
    }
}