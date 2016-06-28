/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.repository.graph;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.discovery.SearchIndexer;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.listener.ActiveStateChangeHandler;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.IndexCreationException;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adds index for properties of a given type when its added before any instances are added.
 */
public class GraphBackedSearchIndexer implements SearchIndexer, ActiveStateChangeHandler {

    private static final Logger LOG = LoggerFactory.getLogger(GraphBackedSearchIndexer.class);

    private final AtlasGraph<?,?> graph;

    List<Class> MIXED_INDEX_EXCLUSIONS = new ArrayList<Class>() {{
            add(Boolean.class);
            add(BigDecimal.class);
            add(BigInteger.class);
        }};

    @Inject
    public GraphBackedSearchIndexer(AtlasGraphProvider graphProvider) throws RepositoryException, AtlasException {
        this(graphProvider, ApplicationProperties.get());
    }

    GraphBackedSearchIndexer( GraphProvider<AtlasGraph> graphProvider, Configuration configuration)
            throws IndexException, RepositoryException {
        this.graph = (AtlasGraph<?,?>)graphProvider.get();
	    if (!HAConfiguration.isHAEnabled(configuration)) {
            initialize();
        }
    }

    /**
     * Initializes the indices for the graph - create indices for Global AtlasVertex Keys
     */
    private void initialize() throws RepositoryException, IndexException {

        AtlasGraphManagement management = graph.getManagementSystem();

        try {

            if (management.containsPropertyKey(Constants.VERTEX_TYPE_PROPERTY_KEY)) {
                LOG.info("Global indexes already exist for graph");
                return;
            }

            Set<String> createdIndexNames = new HashSet<String>();
            /* This is called only once, which is the first time Atlas types are made indexable .*/
            LOG.info("Indexes do not exist, Creating indexes for graph.");
            management.buildMixedVertexIndex(Constants.VERTEX_INDEX, Constants.BACKING_INDEX);
            createdIndexNames.add(Constants.VERTEX_INDEX);

            management.buildMixedEdgeIndex(Constants.EDGE_INDEX, Constants.BACKING_INDEX);
            createdIndexNames.add(Constants.EDGE_INDEX);

            // create a composite index for guid as its unique
            createIndexes(management, Constants.GUID_PROPERTY_KEY, String.class, true,
                   Multiplicity.OPTIONAL, true);

            if(graph.getSupportedGremlinVersion() == GremlinVersion.THREE) {
                createIndexes(management, Constants.STATE_PROPERTY_KEY, String.class, false,
                    Multiplicity.OPTIONAL, true);
            }
            
            // create a composite index for entity creation timestamp
            createIndexes(management, Constants.TIMESTAMP_PROPERTY_KEY, Long.class, false, Multiplicity.OPTIONAL, true);

            // create a composite index for entity modification timestamp
            createIndexes(management, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class, false,
                    Multiplicity.OPTIONAL, true);


            // create a composite and mixed index for type since it can be combined with other keys
            createIndexes(management, Constants.ENTITY_TYPE_PROPERTY_KEY, String.class, false, Multiplicity.OPTIONAL,
                    true);

            // create a composite and mixed index for type since it can be combined with other keys
            createIndexes(management, Constants.SUPER_TYPES_PROPERTY_KEY, String.class, false, Multiplicity.SET,
                    true);

            // create a composite and mixed index for traitNames since it can be combined with other
            // keys. Traits must be a set and not a list.
            createIndexes(management, Constants.TRAIT_NAMES_PROPERTY_KEY, String.class, false, Multiplicity.SET,
                    true);

            // Index for full text search
            createFullTextIndex(management);

            //Indexes for graph backed type system store
            createTypeStoreIndexes(management);

            commit(management);
            LOG.info("Index creation for global keys complete.");

        } catch (Throwable t) {
            rollback(management);
            throw new RepositoryException(t);
        }
    }

    private void createFullTextIndex(AtlasGraphManagement management) {

         AtlasPropertyKey fullText =
                management.makePropertyKey(Constants.ENTITY_TEXT_PROPERTY_KEY, String.class, Multiplicity.OPTIONAL);

        management.createFullTextIndex(Constants.FULLTEXT_INDEX, fullText, Constants.BACKING_INDEX);

        LOG.info("Created mixed index for {}", Constants.ENTITY_TEXT_PROPERTY_KEY);
    }

    private void createTypeStoreIndexes(AtlasGraphManagement management) {
        //Create unique index on typeName
        createIndexes(management, Constants.TYPENAME_PROPERTY_KEY, String.class, true, Multiplicity.OPTIONAL, true);


        //create index on vertex type
        createIndexes(management, Constants.VERTEX_TYPE_PROPERTY_KEY, String.class, false, Multiplicity.OPTIONAL, true);
    }

    /**
     * This is upon adding a new type to Store.
     *
     * @param dataTypes data type
     * @throws org.apache.atlas.AtlasException
     */
    @Override
    public void onAdd(Collection<? extends IDataType> dataTypes) throws AtlasException {
        AtlasGraphManagement management = graph.getManagementSystem();
        for (IDataType dataType : dataTypes) {
            LOG.info("Creating indexes for type name={}, definition={}", dataType.getName(), dataType.getClass());
            try {
                addIndexForType(management, dataType);
                LOG.debug("Index creation for type {} complete", dataType.getName());
            } catch (Throwable throwable) {
                LOG.error("Error creating index for type {}", dataType, throwable);
                //Rollback indexes if any failure
                rollback(management);
                throw new IndexCreationException("Error while creating index for type " + dataType, throwable);
            }
        }

        //Commit indexes
        commit(management);

    }

    @Override
    public void onChange(Collection<? extends IDataType> dataTypes) throws AtlasException {
        onAdd(dataTypes);
    }

    private void addIndexForType(AtlasGraphManagement management, IDataType dataType) {
        switch (dataType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
        case ARRAY:
        case MAP:
            // do nothing since these are only attributes
            // and not types like structs, traits or classes
            break;

        case STRUCT:
            StructType structType = (StructType) dataType;
            createIndexForFields(management, structType, structType.fieldMapping().fields);
            break;

        case TRAIT:
            TraitType traitType = (TraitType) dataType;
            createIndexForFields(management, traitType, traitType.fieldMapping().fields);
            break;

        case CLASS:
            ClassType classType = (ClassType) dataType;
            createIndexForFields(management, classType, classType.fieldMapping().fields);
            break;

        default:
            throw new IllegalArgumentException("bad data type" + dataType);
        }
    }

    private void createIndexForFields(AtlasGraphManagement management, IDataType dataType, Map<String, AttributeInfo> fields) {
        for (AttributeInfo field : fields.values()) {
            if (field.isIndexable) {
                createIndexForAttribute(management, dataType.getName(), field);
            }
        }
    }

    private void createIndexForAttribute(AtlasGraphManagement management, String typeName, AttributeInfo field) {
        final String propertyName = typeName + Constants.SEPARATOR + field.name;
        switch (field.dataType().getTypeCategory()) {
        case PRIMITIVE:
            createIndexes(management, propertyName, getPrimitiveClass(field.dataType()), field.isUnique,
                    field.multiplicity, false);
            break;

        case ENUM:
            createIndexes(management, propertyName, String.class, field.isUnique, field.multiplicity, false);
            break;

        case ARRAY:
        case MAP:
            // todo - how do we overcome this limitation?
            // IGNORE: Can only index single-valued property keys on vertices in Mixed Index
            break;

        case STRUCT:
            StructType structType = (StructType) field.dataType();
            createIndexForFields(management, structType, structType.fieldMapping().fields);
            break;

        case TRAIT:
            // do nothing since this is NOT contained in other types
            break;

        case CLASS:
            // this is only A reference, index the attribute for edge
            // Commenting this out since we do not need an index for edge here
            //createEdgeMixedIndex(propertyName);
            break;

        default:
            throw new IllegalArgumentException("bad data type" + field.dataType().getName());
        }
    }

    private Class getPrimitiveClass(IDataType dataType) {
        if (dataType == DataTypes.STRING_TYPE) {
            return String.class;
        } else if (dataType == DataTypes.SHORT_TYPE) {
            return Short.class;
        } else if (dataType == DataTypes.INT_TYPE) {
            return Integer.class;
        } else if (dataType == DataTypes.BIGINTEGER_TYPE) {
            return BigInteger.class;
        } else if (dataType == DataTypes.BOOLEAN_TYPE) {
            return Boolean.class;
        } else if (dataType == DataTypes.BYTE_TYPE) {
            return Byte.class;
        } else if (dataType == DataTypes.LONG_TYPE) {
            return Long.class;
        } else if (dataType == DataTypes.FLOAT_TYPE) {
            return Float.class;
        } else if (dataType == DataTypes.DOUBLE_TYPE) {
            return Double.class;
        } else if (dataType == DataTypes.BIGDECIMAL_TYPE) {
            return BigDecimal.class;
        } else if (dataType == DataTypes.DATE_TYPE) {
            //Indexing with date converted to long as of now since Titan is yet to add support for Date type with mixed indexes
            return Long.class;
        }


        throw new IllegalArgumentException("unknown data type " + dataType);
    }


    private AtlasPropertyKey createIndexes(AtlasGraphManagement management, String propertyName, Class propertyClass,
            boolean isUnique, Multiplicity cardinality, boolean isSystemProperty) {

	    AtlasPropertyKey propertyKey = management.getPropertyKey(propertyName);
      	if(propertyKey == null) {
              propertyKey = management.makePropertyKey(propertyName, propertyClass, cardinality);

      		  enhanceMixedIndex(management, propertyName, propertyClass, cardinality, propertyKey);
      		  //graphdb needs these indices in order to be able to execute queries
 	        if (isSystemProperty) {
 	         	LOG.debug("Creating composite index for property {} of type {} ", propertyName,
                	propertyClass.getName());
            	    management.createCompositeIndex(propertyName, propertyKey, isUnique);

        	}
        	else if(isUnique || graph.getSupportedGremlinVersion() == GremlinVersion.THREE) {
        	    // send uniqueness as false because there can be many vertexes with the same property value
                // but state can be active / deleted
        		   	LOG.debug("Creating composite index for property {} of type {} ", propertyName,
                	propertyClass.getName());

        	     management.createCompositeIndex(propertyName, propertyKey, false);
        	}

	    }
	    return propertyKey;
    }

    private void enhanceMixedIndex(AtlasGraphManagement management, String propertyName, Class propertyClass,
                                   Multiplicity multiplicity, AtlasPropertyKey propertyKey) {
        if (checkIfMixedIndexApplicable(propertyClass, multiplicity)) {
            //Use backing index

            LOG.debug("Creating backing index for property {} of type {} ", propertyName, propertyClass.getName());
            management.addIndexKey(Constants.VERTEX_INDEX, propertyKey);
            LOG.debug("Created backing index for property {} of type {} ", propertyName, propertyClass.getName());
        }

    }

    private boolean checkIfMixedIndexApplicable(Class propertyClass, Multiplicity cardinality) {
        return !(MIXED_INDEX_EXCLUSIONS.contains(propertyClass) || cardinality.isMany());
    }

    public void commit(AtlasGraphManagement management) throws IndexException {
        try {
            management.commit();
        } catch (Exception e) {
            LOG.error("Index commit failed", e);
            throw new IndexException("Index commit failed ", e);
        }
    }

    public void rollback(AtlasGraphManagement management) throws IndexException {
        try {
            management.rollback();
        } catch (Exception e) {
            LOG.error("Index rollback failed ", e);
            throw new IndexException("Index rollback failed ", e);
        }
    }

    /**
     * Initialize global indices for Titan graph on server activation.
     *
     * Since the indices are shared state, we need to do this only from an active instance.
     */
    @Override
    public void instanceIsActive() throws AtlasException {
        LOG.info("Reacting to active: initializing index");
        try {
            initialize();
        } catch (RepositoryException e) {
            throw new AtlasException("Error in reacting to active on initialization", e);
        } catch (IndexException e) {
            throw new AtlasException("Error in reacting to active on initialization", e);
        }
    }

    @Override
    public void instanceIsPassive() {
        LOG.info("Reacting to passive state: No action right now.");
    }

    /* Commenting this out since we do not need an index for edge label here
    private void createEdgeMixedIndex(String propertyName) {
        EdgeLabel edgeLabel = management.getEdgeLabel(propertyName);
        if (edgeLabel == null) {
            edgeLabel = management.makeEdgeLabel(propertyName).make();
            management.buildEdgeIndex(edgeLabel, propertyName, AtlasEdgeDirection.BOTH, Order.DEFAULT);
            LOG.info("Created index for edge label {}", propertyName);
        }
    }*/
}
