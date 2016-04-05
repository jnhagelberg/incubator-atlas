
package org.apache.atlas.repository.graphdb;

import org.apache.atlas.typesystem.types.Multiplicity;

public interface GraphDatabaseManager {

    boolean containsPropertyKey(String key);

    void buildMixedIndex(String index, ElementType type, String backingIndex);

    void createFullTextIndex(String indexName, String propertyKey, String backingIndex);

    void rollback();

    void commit();

    void createCompositeIndex(String indexName, Class propertyClass, Multiplicity multiplicity,
            boolean isUnique);

    void createBackingIndex(String propertyName, String vertexIndexName, Class propertyClass,
            Multiplicity multiplicity);


}
