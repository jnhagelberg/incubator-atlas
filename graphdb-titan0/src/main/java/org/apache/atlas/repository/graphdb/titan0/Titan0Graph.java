
package org.apache.atlas.repository.graphdb.titan0;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.script.Bindings;
import javax.script.ScriptEngine;

import org.apache.atlas.AtlasException;
import org.apache.atlas.query.Expressions;
import org.apache.atlas.query.Expressions.AliasExpression;
import org.apache.atlas.query.GraphPersistenceStrategies;
import org.apache.atlas.query.GremlinEvaluator;
import org.apache.atlas.query.GremlinQuery;
import org.apache.atlas.query.GremlinQueryResult;
import org.apache.atlas.repository.graph.util.IterableAdapter;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAGraph;
import org.apache.atlas.repository.graphdb.AAGraphQuery;
import org.apache.atlas.repository.graphdb.AAIndexQuery;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.repository.graphdb.ElementType;
import org.apache.atlas.repository.graphdb.GraphDatabaseManager;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanIndexQuery;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.pipes.util.structures.Row;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.convert.Decorators.AsScala;
import scala.collection.mutable.Buffer;

public class Titan0Graph implements AAGraph<Vertex,Edge> {

    private TitanGraph titanGraph_;
    
    public Titan0Graph(TitanGraph graph) {
        titanGraph_ = graph;
    }

    @Override
    public AAEdge<Vertex,Edge> addEdge(Object id, AAVertex<Vertex,Edge> outVertex, AAVertex<Vertex,Edge> inVertex, String edgeLabel) {
        Edge edge = titanGraph_.addEdge(id, 
                outVertex.getWrappedVertex(), 
                inVertex.getWrappedVertex(), 
                edgeLabel);
        return TitanObjectFactory.createEdge(edge);
    }

    @Override
    public AAGraphQuery<Vertex,Edge> query() {
        GraphQuery query = titanGraph_.query();
        return TitanObjectFactory.createQuery(query);
    }

    @Override
    public AAEdge<Vertex,Edge> getEdge(String edgeId) {
        Edge edge = titanGraph_.getEdge(edgeId);
        return TitanObjectFactory.createEdge(edge);
    }

    @Override
    public void removeEdge(AAEdge<Vertex,Edge> edge) {
        titanGraph_.removeEdge(edge.getWrappedEdge());
        
    }

    @Override
    public void removeVertex(AAVertex<Vertex,Edge> vertex) {
        titanGraph_.removeVertex(vertex.getWrappedVertex());
        
    }

    @Override
    public Iterable<AAEdge<Vertex,Edge>> getEdges() {
        Iterable<Edge> edges = titanGraph_.getEdges();
        return new IterableAdapter<Edge, AAEdge<Vertex, Edge>>(edges, EdgeMapper.INSTANCE);
    }

    @Override
    public Iterable<AAVertex<Vertex,Edge>> getVertices() {
        Iterable<Vertex> vertices = titanGraph_.getVertices();
        return new IterableAdapter<Vertex, AAVertex<Vertex, Edge>>(vertices, VertexMapper.INSTANCE);
    }

    @Override
    public AAVertex<Vertex,Edge> addVertex(Object id) {
        Vertex result = titanGraph_.addVertex(id);
        return TitanObjectFactory.createVertex(result);
    }

    @Override
    public void commit() {
        titanGraph_.commit();        
    }

    @Override
    public void rollback() {
        titanGraph_.rollback();
    }

    @Override
    public AAIndexQuery indexQuery(String fulltextIndex, String graphQuery) {
        TitanIndexQuery query = titanGraph_.indexQuery(fulltextIndex, graphQuery);
        return new Titan0IndexQuery(query);
    }

    @Override
    public GraphDatabaseManager getManagementSystem() {
        return new Titan0DatabaseManager(titanGraph_.getManagementSystem());
    }

    @Override
    public void shutdown() {
       titanGraph_.shutdown();        
    }

    @Override
    public Set<String> getIndexedKeys(ElementType type) {
        Class<? extends Element> titanClass = type == ElementType.VERTEX ? Vertex.class : Edge.class;
       return titanGraph_.getIndexedKeys(titanClass);
    }

    @Override
    public AAVertex<Vertex, Edge> getVertex(String vertexId) {
        Vertex v = titanGraph_.getVertex(vertexId);
        return TitanObjectFactory.createVertex(v);
    }

    @Override
    public Iterable<AAVertex<Vertex, Edge>> getVertices(String key, Object value) {
        
        Iterable<Vertex> result = titanGraph_.getVertices(key, value);
        return new IterableAdapter<>(result, VertexMapper.INSTANCE);

    }

    @Override
    public void injectBinding(Bindings bindings, String key) {
        bindings.put(key, titanGraph_);
    }

//    @Override
//    public GremlinQueryResult executeGremlinQuery(ScriptEngine engine, GremlinQuery qry, Bindings bindings, GremlinEvaluator<?,?> evaluator, GraphPersistenceStrategies persistenceStrategy) throws AtlasException {
//        
//        IDataType<?> rType = qry.expr().dataType();
//        IDataType<?> oType;
//        if(qry.isPathExpresion()) {
//            oType = qry.expr().children().head().dataType();
//        }
//        else {
//            oType = rType;
//        }
//        
//        Object rawRes = engine.eval(qry.queryStr(), bindings);
//
//        if(qry.hasSelectList()) {
//            List<Object> rows = new ArrayList<Object>();
//            List<?> list = (List)rawRes;
//            for(Object v : list) {
//                Object iV = evaluator.instanceObject(v);
//                Object o = persistenceStrategy.constructInstance(oType, iV);
//                rows.add(evaluator.addPathStruct(v, o));
//            }
//                        
//            scala.collection.immutable.List<Object> scalaRowList = 
//                    JavaConverters.asScalaBufferConverter(rows).asScala().toList();            
//            
//            return new GremlinQueryResult(qry.expr().toString(), rType, scalaRowList);             
//        }
//        else {
//            StructType sType = (StructType)oType;
//            List<Object> rows = new ArrayList();
//            List list = (List)rawRes;
//            
//            for(Object r : list) {
// 
//                Row<List> rV = (Row<List>)evaluator.instanceObject(r);
//                ITypedStruct sInstance = sType.createInstance();
//                Object temp = qry.isPathExpresion() ? qry.expr().children().head() : qry.expr();
//                Expressions.SelectExpression selExpr = (Expressions.SelectExpression)temp;
//                List<AliasExpression> selList = JavaConverters.asJavaListConverter(selExpr.selectListWithAlias()).asJava();
//                for(AliasExpression aE : selList) {
//                    String cName = aE.alias();
//                    Tuple2<String, Object> mappingTuple = qry.resultMaping().get(cName).get();                   
//                    String src = mappingTuple._1();
//                    Integer idx = (Integer)mappingTuple._2();
//                    Object v = rV.getColumn(src).get(idx);
//                    sInstance.set(cName, persistenceStrategy.constructInstance(aE.dataType(), v));
//                    
//                }
//                rows.add(evaluator.addPathStruct(r, sInstance));
//                
//            }
//            scala.collection.immutable.List<Object> scalaRowList = 
//                    JavaConverters.asScalaBufferConverter(rows).asScala().toList();            
//            
//            return new GremlinQueryResult(qry.expr().toString(), rType, scalaRowList);        
//        }
//    }
    
    @Override
    public Object getGremlinColumnValue(Object rowValue, String colName, int idx) {
        Row<List> rV = (Row<List>)rowValue;
        Object value = rV.getColumn(colName).get(idx);
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
}
