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

package org.apache.atlas.repository.graphdb.titan1;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graph.GraphProviderPlugin;
import org.apache.atlas.repository.graphdb.AAGraph;
import org.apache.atlas.repository.graphdb.titan1.serializer.BigDecimalSerializer;
import org.apache.atlas.repository.graphdb.titan1.serializer.BigIntegerSerializer;
import org.apache.atlas.repository.graphdb.titan1.serializer.StringListSerializer;
import org.apache.atlas.repository.graphdb.titan1.serializer.TypeCategorySerializer;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.tinkerpop.TitanIoRegistry;

/**
 * Default implementation for Graph Provider that doles out Titan Graph.
 */
public class Titan1GraphPlugin implements GraphProviderPlugin<Vertex,Edge> {

    private static final Logger LOG = LoggerFactory.getLogger(Titan1GraphPlugin.class);

    /**
     * Constant for the configuration property that indicates the prefix.
     */
    public static final String GRAPH_PREFIX = "atlas.graph";

    public static final String INDEX_BACKEND_CONF = "index.search.backend";

    public static final String INDEX_BACKEND_LUCENE = "lucene";

    public static final String INDEX_BACKEND_ES = "elasticsearch";

    private static volatile TitanGraph graphInstance;

    public static Configuration getConfiguration() throws AtlasException {
        Configuration configProperties = ApplicationProperties.get();        
        
        Configuration titanConfig = ApplicationProperties.getSubsetConfiguration(configProperties, GRAPH_PREFIX);
       
        
        //add serializers for non-standard property value types that Atlas uses
        
        titanConfig.addProperty("attributes.custom.attribute1.attribute-class",TypeCategory.class.getName());
        titanConfig.addProperty("attributes.custom.attribute1.serializer-class",TypeCategorySerializer.class.getName());

        //not ideal, but avoids making large changes to Atlas
        titanConfig.addProperty("attributes.custom.attribute2.attribute-class", ArrayList.class.getName());
        titanConfig.addProperty("attributes.custom.attribute2.serializer-class",StringListSerializer.class.getName());

        
        titanConfig.addProperty("attributes.custom.attribute3.attribute-class", BigInteger.class.getName());
        titanConfig.addProperty("attributes.custom.attribute3.serializer-class", BigIntegerSerializer.class.getName());

        
        titanConfig.addProperty("attributes.custom.attribute4.attribute-class", BigDecimal.class.getName());
        titanConfig.addProperty("attributes.custom.attribute4.serializer-class", BigDecimalSerializer.class.getName());

        return titanConfig;
    }
    


    public static TitanGraph getGraphInstance() {
        if (graphInstance == null) {
            synchronized (Titan1GraphPlugin.class) {
                if (graphInstance == null) {
                    Configuration config;
                    try {
                        config = getConfiguration();
                    } catch (AtlasException e) {
                        throw new RuntimeException(e);
                    }

                    graphInstance = TitanFactory.open(config);
                                        
                    TitanManagement mgmt = graphInstance.openManagement();
                    //todo: refactor to use Constants class.  need that to be in the classpath...
                    createPropertyKeyIfNeeded("__traitNames", mgmt);
                    createPropertyKeyIfNeeded("__superTypeNames", mgmt);

                    mgmt.commit();
                    
                    validateIndexBackend(config);
                }
            }
        }
        return graphInstance;
    }



    private static boolean isRecreateKey(RelationType type) {
        if(! type.isPropertyKey()) {
            return true;
        }
        //check to make sure the property key is valid
        PropertyKey pk = (PropertyKey)type;
        if(pk.cardinality() != Cardinality.SET) {
            return true;
        }
        if(pk.dataType() != String.class) {
            return true;
        }
        return false;
    }
    
    private static void createPropertyKeyIfNeeded(String name, TitanManagement mgmt) {
        
        boolean create = true;
        if(mgmt.containsRelationType(name)) {
            RelationType type = mgmt.getRelationType(name);
            create = isRecreateKey(type);
            if(create) {
                type.remove();
            }
        }
        if(create) {
            mgmt.makePropertyKey(name).dataType(String.class).cardinality(Cardinality.SET).make();
        }
    }
     

    public static void unload() {
        synchronized (Titan1GraphPlugin.class) {
            
            if(graphInstance == null) {
                return;
            }
            
            graphInstance.close();
            graphInstance = null;
        }
    }

    static void validateIndexBackend(Configuration config) {
        String configuredIndexBackend = config.getString(INDEX_BACKEND_CONF);

        TitanManagement managementSystem = getGraphInstance().openManagement();
        String currentIndexBackend = managementSystem.get(INDEX_BACKEND_CONF);
        managementSystem.commit();
        
        if(!configuredIndexBackend.equals(currentIndexBackend)) {
            throw new RuntimeException("Configured Index Backend " + configuredIndexBackend + " differs from earlier configured Index Backend " + currentIndexBackend + ". Aborting!");
        }

    }


    @Override
    public void initialize() {
        //load the Gremlin 3 sugar plugin
        SugarLoader.load();
        
        //update registry
        GraphSONMapper.build().addRegistry(TitanIoRegistry.INSTANCE).create();
        
        
    }

    @Override
    public AAGraph<Vertex, Edge> createGraph() {
       //initialize up front to make sure bootstrapping is correct in test cases,
        //where the graph is unloaded and unloaded multiple times.  TBD - figure
        //out how this can be avoided
       getGraphInstance();
       return new Titan1Graph();
    }

    @Override
    public void unloadGraph() { 
        unload();
    }
}
