
package org.apache.atlas.repository.graphdb.titan1;


import org.apache.atlas.repository.graphdb.ElementType;
import org.apache.atlas.repository.graphdb.GraphDatabaseManager;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;

public class Titan1DatabaseManager implements GraphDatabaseManager {

    private TitanManagement management_;
    
    public Titan1DatabaseManager(TitanManagement managementSystem) {
        management_ = managementSystem;        
    }

    @Override
    public void buildMixedIndex(String index, ElementType type, String backingIndex) {
       
       Class<? extends Element> titanClass = type == ElementType.VERTEX ? Vertex.class : Edge.class;
       management_.buildIndex(index, titanClass).buildMixedIndex(backingIndex);
    }
    
    @Override
    public void createFullTextIndex(String indexName, String propertyKey, String backingIndex) { 
        
        PropertyKey fullText = getOrCreatePropertyKey(propertyKey, String.class, null);

        management_.buildIndex(indexName, Vertex.class)
            .addKey(fullText, com.thinkaurelius.titan.core.schema.Parameter.of("mapping", Mapping.TEXT))
            .buildMixedIndex(backingIndex);
     
    }

    @Override
    public void createBackingIndex(String propertyName, String vertexIndexName, Class propertyClass, Multiplicity cardinality) {
        Cardinality titanCardinality = TitanObjectFactory.createCardinality(cardinality);
        PropertyKey propertyKey = getOrCreatePropertyKey(propertyName, propertyClass, titanCardinality);
        TitanGraphIndex vertexIndex = management_.getGraphIndex(vertexIndexName);
        management_.addIndexKey(vertexIndex, propertyKey);
    }
    
    @Override
    public void createCompositeIndex(String indexName, Class propertyClass, Multiplicity cardinality, boolean isUnique) {
        Cardinality titanCardinality = TitanObjectFactory.createCardinality(cardinality);
        PropertyKey propertyKey = getOrCreatePropertyKey(indexName, propertyClass, titanCardinality);
        TitanManagement.IndexBuilder indexBuilder =
                management_.buildIndex(indexName, Vertex.class).addKey(propertyKey);
        
        if (isUnique) {
            indexBuilder.unique();
        }
        indexBuilder.buildCompositeIndex();
    }
    
    @Override
    public boolean containsPropertyKey(String vertexTypePropertyKey) {
        return management_.containsPropertyKey(vertexTypePropertyKey);
    }
    
    private PropertyKey getOrCreatePropertyKey(String propertyName, Class propertyClass, Cardinality cardinality) {
        
        PropertyKey propertyKey = management_.getPropertyKey(propertyName);
        if (propertyKey != null) {
            return propertyKey;
        }
        PropertyKeyMaker propertyKeyBuilder = management_.makePropertyKey(propertyName).dataType(propertyClass);
        if(cardinality != null) {
            propertyKeyBuilder.cardinality(cardinality);
        }
        return propertyKey = propertyKeyBuilder.make();   
    }

    @Override
    public void rollback() {
        management_.rollback();
        
    }

    @Override
    public void commit() { 
        management_.commit();   
    }
    
}
