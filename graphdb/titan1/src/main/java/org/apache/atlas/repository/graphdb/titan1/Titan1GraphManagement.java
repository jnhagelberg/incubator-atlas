
package org.apache.atlas.repository.graphdb.titan1;


import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;

public class Titan1GraphManagement implements AtlasGraphManagement {

    private static final char[] RESERVED_CHARS = {'{', '}', '"', '$', Token.SEPARATOR_CHAR};
    
    private TitanManagement management_;
    
    public Titan1GraphManagement(TitanManagement managementSystem) {
        management_ = managementSystem;        
    }

    @Override
    public void buildMixedVertexIndex(String index, String backingIndex) {
       buildMixedIndex(index, Vertex.class, backingIndex);
    }
    
    @Override
    public void buildMixedEdgeIndex(String index, String backingIndex) {
       buildMixedIndex(index, Edge.class, backingIndex);
    }
       
    private void buildMixedIndex(String index, Class<? extends Element> titanClass, String backingIndex) {
       
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
    public void createCompositeIndex(String propertyName, Class propertyClass, Multiplicity cardinality, boolean isUnique) {
        Cardinality titanCardinality = TitanObjectFactory.createCardinality(cardinality);
        PropertyKey propertyKey = getOrCreatePropertyKey(propertyName, propertyClass, titanCardinality);
        TitanManagement.IndexBuilder indexBuilder =
                management_.buildIndex(propertyName, Vertex.class).addKey(propertyKey);
        
        if (isUnique) {
            indexBuilder.unique();
        }
        indexBuilder.buildCompositeIndex();
    }
    
    @Override
    public boolean containsPropertyKey(String propertyName) {
        return management_.containsPropertyKey(propertyName);
    }
    
    private PropertyKey getOrCreatePropertyKey(String propertyName, Class propertyClass, Cardinality cardinality) {
        
        //titan 1 does not validate that there are no special characters in property names.  Perhaps that
        //restriction was removed.  To be consistent, though, add that check back in here.  Otherwise
        //DefaultMetadataServiceTest.testTypeUpdateWithReservedAttributes() fails.
        checkName(propertyName);
        
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

    private static void checkName(String name) {
        //for some reason, name checking was removed from StandardPropertyKeyMaker.make()
        //in titan 1.  For consistency, do the check here.
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Need to specify name");
        for (char c : RESERVED_CHARS)
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name can not contains reserved character %s: %s", c, name);
       
    }
}
