
package org.apache.atlas.repository.graphdb.titan1;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.internal.Token;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Titan1GraphManagement implements AtlasGraphManagement {

    private static final Logger LOG = LoggerFactory.getLogger(Titan1GraphManagement.class);
    
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
    
        /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#waitForIndexAvailibility(java.util.Collection)
     */
    @Override
    public void waitForIndexAvailibility(Collection<String> indexNames) throws AtlasException {

        long start = System.currentTimeMillis();
        long timeoutTime = start + 1000*60*10; //wait at most 10 minutes

        //keeps track of what indices are still not fully enabled
        Collection<String> pendingIndices = new HashSet<String>();
        pendingIndices.addAll(indexNames);


        //wait for index to become active
        long currentTime = System.currentTimeMillis();

        while(currentTime < timeoutTime) {

            //check status of all indices
            removeEnabledIndicesFromCollection(pendingIndices);

            if(pendingIndices.size() == 0) {
                long completeTime = System.currentTimeMillis();

                LOG.info("Indices fully enabled after " + (completeTime - start) + " ms");
                return;
            }

            logActivationStatus(pendingIndices);

            try {
                Thread.sleep(1000);
            }
            catch(InterruptedException e) { }
        }
        StringBuilder builder = new StringBuilder();
        builder.append("Timed out waiting for indices to activate.  Could not activate the following indices:");
        appendPendingIndexList(pendingIndices, builder);
        throw new AtlasException(builder.toString());
    }



    private void removeEnabledIndicesFromCollection(Collection<String> pendingIndices) {

        Iterator<String> it = pendingIndices.iterator();
        while(it.hasNext()) {
            String name = it.next();
            if(isIndexFullyEnabled(name)) {
                it.remove();
            }
        }
    }

    private boolean isIndexFullyEnabled(String name) {
        TitanGraphIndex index = management_.getGraphIndex(name);
        for(PropertyKey key : index.getFieldKeys()) {
            if(index.getIndexStatus(key) != SchemaStatus.ENABLED) {
                return false;
            }
        }
        return true;
    }

    private void logActivationStatus(Collection<String> pendingIndices) {
        LOG.info("Waiting for indices to be enabled...");

        if(LOG.isDebugEnabled()) {
            StringBuilder builder = new StringBuilder();
            builder.append("Waiting for the following indices to be fully enabled: ");

            appendPendingIndexList(pendingIndices, builder);
            LOG.debug(builder.toString());
        }
    }


    /**
     * @param pendingIndices
     * @param builder
     */
    private void appendPendingIndexList(Collection<String> pendingIndices, StringBuilder builder) {
        List<String> sortedIndexNames = new ArrayList<String>();
        sortedIndexNames.addAll(pendingIndices);
        Collections.sort(sortedIndexNames);
        Iterator<String> it = sortedIndexNames.iterator();
        while(it.hasNext()) {
           builder.append(it.next());
           if(it.hasNext()) {
               builder.append(", ");
           }
        }
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#vertexIndexContainsPropertyKey(java.lang.String, java.lang.String)
     */
    @Override
    public boolean vertexIndexContainsPropertyKey(String indexName, String propertyName) {

        TitanGraphIndex index = management_.getGraphIndex(indexName);
        if(index == null) {
            return false;
        }

        if(index.getIndexedElement() != Vertex.class) {
            return false;
        }
        
        for(PropertyKey key : index.getFieldKeys()) {
            if(key.name().equals(propertyName)) {
                return true;
            }
        }
        return false;
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#containsVertexIndex(java.lang.String)
     */
    @Override
    public boolean containsVertexIndex(String name) {

        TitanGraphIndex index = management_.getGraphIndex(name);
        if(index == null) {
            return false;
        }

        if(index.getIndexedElement() != Vertex.class) {
            return false;
        }
        return true;
    }

}
