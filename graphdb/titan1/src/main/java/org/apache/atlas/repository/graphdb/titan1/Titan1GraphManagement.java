
package org.apache.atlas.repository.graphdb.titan1;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
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
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.Mapping;
import com.thinkaurelius.titan.core.schema.PropertyKeyMaker;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.internal.Token;

public class Titan1GraphManagement implements AtlasGraphManagement {

    private static final Logger LOG = LoggerFactory.getLogger(Titan1GraphManagement.class);

    private static final char[] RESERVED_CHARS = {'{', '}', '"', '$', Token.SEPARATOR_CHAR};

    private TitanGraph graph_;
    private TitanManagement management_;

    public Titan1GraphManagement(TitanGraph graph, TitanManagement managementSystem) {
        management_ = managementSystem;
        graph_ = graph;
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
    public void createFullTextIndex(String indexName, AtlasPropertyKey propertyKey, String backingIndex) {

        PropertyKey fullText = TitanObjectFactory.createPropertyKey(propertyKey);

        management_.buildIndex(indexName, Vertex.class)
            .addKey(fullText, com.thinkaurelius.titan.core.schema.Parameter.of("mapping", Mapping.TEXT))
            .buildMixedIndex(backingIndex);

    }


    @Override
    public boolean containsPropertyKey(String propertyName) {
        return management_.containsPropertyKey(propertyName);
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
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#makePropertyKey(java.lang.String, java.lang.Class, org.apache.atlas.typesystem.types.Multiplicity)
     */
    @Override
    public AtlasPropertyKey makePropertyKey(String propertyName, Class propertyClass, Multiplicity multiplicity) {

        PropertyKeyMaker propertyKeyBuilder = management_.makePropertyKey(propertyName).dataType(propertyClass);
        Cardinality cardinality = TitanObjectFactory.createCardinality(multiplicity);
        if(cardinality != null) {
            propertyKeyBuilder.cardinality(cardinality);
        }
        PropertyKey propertyKey = propertyKeyBuilder.make();
        return TitanObjectFactory.createPropertyKey(propertyKey);
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#getPropertyKey(java.lang.String)
     */
    @Override
    public AtlasPropertyKey getPropertyKey(String propertyName) {
        checkName(propertyName);
        return TitanObjectFactory.createPropertyKey(management_.getPropertyKey(propertyName));
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#createCompositeIndex(java.lang.String, org.apache.atlas.repository.graphdb.AtlasPropertyKey, boolean)
     */
    @Override
    public void createCompositeIndex(String propertyName, AtlasPropertyKey propertyKey, boolean enforceUniqueness) {
        PropertyKey titanKey = TitanObjectFactory.createPropertyKey(propertyKey);
        TitanManagement.IndexBuilder indexBuilder =
            management_.buildIndex(propertyName, Vertex.class).addKey(titanKey);
        if (enforceUniqueness) {
            indexBuilder.unique();
        }
        indexBuilder.buildCompositeIndex();
    }


    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#addIndexKey(java.lang.String, org.apache.atlas.repository.graphdb.AtlasPropertyKey)
     */
    @Override
    public void addIndexKey(String indexName, AtlasPropertyKey propertyKey) {
        PropertyKey titanKey = TitanObjectFactory.createPropertyKey(propertyKey);
        TitanGraphIndex vertexIndex = management_.getGraphIndex(indexName);
        management_.addIndexKey(vertexIndex, titanKey);
    }

    /* (non-Javadoc)
     * @see org.apache.atlas.repository.graphdb.AtlasGraphManagement#getGraphIndex(java.lang.String)
     */
    @Override
    public AtlasGraphIndex getGraphIndex(String indexName) {
        TitanGraphIndex index = management_.getGraphIndex(indexName);
        return TitanObjectFactory.createGraphIndex(index);
    }

}
