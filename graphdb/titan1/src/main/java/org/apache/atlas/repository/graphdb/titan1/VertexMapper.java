
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.utils.adapters.Mapper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class VertexMapper implements Mapper<Vertex, AtlasVertex<Titan1Vertex, Titan1Edge>> {

    public static VertexMapper INSTANCE = new VertexMapper();

    private VertexMapper() {

    }
    @Override
    public AtlasVertex<Titan1Vertex, Titan1Edge> map(Vertex source) {
       return TitanObjectFactory.createVertex(source);
    }
}