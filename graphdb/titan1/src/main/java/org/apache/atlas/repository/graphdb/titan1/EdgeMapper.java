
package org.apache.atlas.repository.graphdb.titan1;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.utils.adapters.Mapper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class EdgeMapper implements Mapper<Edge, AtlasEdge<Titan1Vertex, Titan1Edge>> {

    public static EdgeMapper INSTANCE = new EdgeMapper();

    private EdgeMapper() {

    }
    @Override
    public AtlasEdge<Titan1Vertex, Titan1Edge> map(Edge source) {
       return  TitanObjectFactory.createEdge(source);
    }
}