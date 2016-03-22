
package org.apache.atlas.repository.graphdb;

import java.util.Set;

import javax.script.Bindings;

//TODO: rename to graphdatabase?
public interface AAGraph<V,E> {

	AAEdge<V,E> addEdge(Object id, AAVertex<V,E> outVertex, AAVertex<V,E> inVertex, String label);

	AAGraphQuery<V,E> query();

	AAEdge<V,E> getEdge(String edgeId);

	void removeEdge(AAEdge<V,E> edge);

	void removeVertex(AAVertex<V,E> vertex);

	Iterable<AAEdge<V,E>> getEdges();

	Iterable<AAVertex<V,E>> getVertices();

	AAVertex<V,E> addVertex(Object id);

    void commit();

    void rollback();

    AAIndexQuery<V,E> indexQuery(String fulltextIndex, String graphQuery);

    GraphDatabaseManager getManagementSystem();

    void shutdown();

    Set<String> getIndexedKeys(ElementType type);

    AAVertex<V, E> getVertex(String vertexId);

    Iterable<AAVertex<V,E>> getVertices(String key, Object value);

    void injectBinding(Bindings bindings, String string);   

    Object getGremlinColumnValue(Object rowValue, String colName, int idx);

    Object convertGremlinValue(Object rawVTalue);
}
