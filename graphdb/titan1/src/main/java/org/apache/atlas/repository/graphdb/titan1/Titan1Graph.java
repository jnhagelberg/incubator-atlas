
package org.apache.atlas.repository.graphdb.titan1;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleScriptContext;

import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasIndexQuery;
import org.apache.atlas.repository.graphdb.AtlasSchemaViolationException;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.utils.adapters.IteratorAdapter;
import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.CompileStaticCustomizerProvider;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TypeCheckedCustomizerProvider;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;

import com.thinkaurelius.titan.core.SchemaViolationException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;

public class Titan1Graph implements AtlasGraph<Titan1Vertex, Titan1Edge> {


    public Titan1Graph() {

    }

    @Override
    public AtlasEdge<Titan1Vertex, Titan1Edge> addEdge(AtlasVertex<Titan1Vertex, Titan1Edge> outVertex, AtlasVertex<Titan1Vertex, Titan1Edge> inVertex, String edgeLabel) {

        try {
            Vertex oV = outVertex.getV().getWrappedElement();
            Vertex iV = inVertex.getV().getWrappedElement();
            Edge edge = oV.addEdge(edgeLabel, iV);
            return TitanObjectFactory.createEdge(edge);
        }
        catch(SchemaViolationException e) {
            throw new AtlasSchemaViolationException(e);
        }
    }

    @Override
    public AtlasGraphQuery<Titan1Vertex, Titan1Edge> query() {

        TitanGraphQuery<?> query = getGraph().query();
        return TitanObjectFactory.createQuery(query);
    }

    @Override
    public AtlasEdge<Titan1Vertex, Titan1Edge> getEdge(String edgeId) {
        Iterator<Edge> it = getGraph().edges(edgeId);
        Edge e = getSingleElement(it, edgeId);
        return TitanObjectFactory.createEdge(e);
    }

    @Override
    public void removeEdge(AtlasEdge<Titan1Vertex, Titan1Edge> edge) {

        Edge wrapped = edge.getE().getWrappedElement();
        wrapped.remove();

    }

    @Override
    public void removeVertex(AtlasVertex<Titan1Vertex, Titan1Edge> vertex) {
        Vertex wrapped = vertex.getV().getWrappedElement();
        wrapped.remove();
    }

    @Override
    public Iterable<AtlasEdge<Titan1Vertex, Titan1Edge>> getEdges() {

        Iterator<Edge> edges = getGraph().edges();
        final Iterator<AtlasEdge<Titan1Vertex, Titan1Edge>> resultIt = new IteratorAdapter<>(edges, EdgeMapper.INSTANCE);

        return new Iterable<AtlasEdge<Titan1Vertex, Titan1Edge>>() {

            @Override
            public Iterator<AtlasEdge<Titan1Vertex, Titan1Edge>> iterator() {
                return resultIt;
            }
        };

    }

    @Override
    public Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>> getVertices() {

        Iterator<Vertex> vertices = getGraph().vertices();
        final Iterator<AtlasVertex<Titan1Vertex, Titan1Edge>> resultIt = new IteratorAdapter<>(vertices, VertexMapper.INSTANCE);

        return new Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>>() {

            @Override
            public Iterator<AtlasVertex<Titan1Vertex, Titan1Edge>> iterator() {
                return resultIt;
            }
        };
    }

    @Override
    public AtlasVertex<Titan1Vertex, Titan1Edge> addVertex() {
        Vertex result = getGraph().addVertex();
        return TitanObjectFactory.createVertex(result);
    }

    @Override
    public void commit() {
        getGraph().tx().commit();
    }

    @Override
    public void rollback() {
        getGraph().tx().rollback();
    }

    @Override
    public AtlasIndexQuery<Titan1Vertex, Titan1Edge> indexQuery(String fulltextIndex, String graphQuery) {
        TitanIndexQuery query = getGraph().indexQuery(fulltextIndex, graphQuery);
        return new Titan1IndexQuery(query);
    }

    @Override
    public AtlasGraphManagement getManagementSystem() {
        return new Titan1GraphManagement(getGraph(), getGraph().openManagement());
    }

    @Override
    public void shutdown() {
        getGraph().close();
    }


    @Override
    public Set<String> getEdgeIndexKeys() {
        return getIndexKeys(Edge.class);
    }

    public Set<String> getVertexIndexKeys() {
        return getIndexKeys(Vertex.class);
    }

    private Set<String> getIndexKeys(Class<? extends Element> titanElementClass) {

        TitanManagement mgmt = getGraph().openManagement();
        Iterable<TitanGraphIndex> indices = mgmt.getGraphIndexes(titanElementClass);
        Set<String> result = new HashSet<String>();
        for(TitanGraphIndex index : indices) {
            result.add(index.name());
        }
        mgmt.commit();
        return result;

    }

    @Override
    public AtlasVertex<Titan1Vertex, Titan1Edge> getVertex(String vertexId) {

        //use lazy vertex retrieval (mostly to test this feature)
        return new Titan1Vertex(getGraph(), vertexId);
    }

    public static <T> T getSingleElement(Iterator<T> it, String id) {
        if(! it.hasNext()) {
            return null;
        }
        T element = it.next();
        if(it.hasNext()) {
            throw new RuntimeException("Multiple items were found with the id " + id);
        }
        return element;
    }

    @Override
    public Iterable<AtlasVertex<Titan1Vertex, Titan1Edge>> getVertices(String key, Object value) {
        AtlasGraphQuery<Titan1Vertex, Titan1Edge> query = query();
        query.has(key, value);
        return query.vertices();
    }

    @Override
    public Object getGremlinColumnValue(Object rowValue, String colName, int idx) {

        Object rawColumnValue = null;
        if(rowValue instanceof Map) {
           rawColumnValue = ((Map<?, ?>)rowValue).get(colName);
        }
        else {
            //when there is only one column, result does not come back as a map
            rawColumnValue = rowValue;
        }

        Object value = null;
        if(rawColumnValue instanceof List && idx >= 0) {
            value = ((List<?>)rawColumnValue).get(idx);
        }
        else {
            value = rawColumnValue;
        }

        return convertGremlinValue(value);
    }

    @Override
    public Object convertGremlinValue(Object rawValue) {
        if(rawValue instanceof Vertex) {
            return TitanObjectFactory.createVertex((Vertex)rawValue);
        }
        if(rawValue instanceof Edge) {
            return TitanObjectFactory.createEdge((Edge)rawValue);
        }
        return rawValue;
    }

    @Override
    public GremlinVersion getSupportedGremlinVersion() {

        return GremlinVersion.THREE;
    }

    @Override
    public List<Object> convertPathQueryResultToList(Object rawValue) {
        ImmutablePath path =  (ImmutablePath)rawValue;
        return path.objects();

    }

    @Override
    public void clear() {
        TitanCleanup.clear(getGraph());

    }

    private TitanGraph getGraph() {
        return Titan1Database.getGraphInstance();
    }

    @Override
    public void exportToGson(OutputStream os) throws IOException {

       GraphSONMapper mapper = getGraph().io(IoCore.graphson()).mapper().create();
       GraphSONWriter.Builder builder = GraphSONWriter.build();
       builder.mapper(mapper);
       GraphSONWriter writer = builder.create();
       writer.writeGraph(os, getGraph());
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraph#executeGremlinScript(java.lang.String)
     */
    @Override
    public Object executeGremlinScript(String gremlinQuery) throws ScriptException {


        ScriptEngineManager manager = new ScriptEngineManager();
        ScriptEngine engine = manager.getEngineByName("gremlin-groovy");
        Bindings bindings = engine.createBindings();
        bindings.put("g", getGraph().traversal());
        Object result = engine.eval("import java.util.function.Function;" + gremlinQuery, bindings);
        return result;

    }

    @Override
    public String generatePersisentToLogicalConversionExpression(String expr, IDataType<?> type) {
        //nothing special needed, value is stored in required type
        return expr;
    }

    @Override
    public boolean isPropertyValueConversionNeeded(IDataType<?> type) {        
        return false;
    }

    @Override
    public boolean requiresInitialIndexedPredicate() {        
        return false;
    }

    @Override
    public String getInitialIndexedPredicate() {
        return "";
    }
}
