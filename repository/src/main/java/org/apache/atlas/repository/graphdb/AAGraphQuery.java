
package org.apache.atlas.repository.graphdb;

//TODO: rename to graphdatabase?
public interface AAGraphQuery<V,E> {

	AAGraphQuery<V,E> has(String propertyKey, Object value);

	Iterable<AAVertex<V, E>> vertices();

	

}
