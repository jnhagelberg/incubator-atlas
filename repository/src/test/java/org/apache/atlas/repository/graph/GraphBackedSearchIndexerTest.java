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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.atlas.AtlasException;
import org.apache.atlas.ha.HAConfiguration;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.IndexException;
import org.apache.atlas.repository.RepositoryException;
import org.apache.atlas.repository.graphdb.AAGraph;
import org.apache.atlas.repository.graphdb.GraphDatabaseManager;
import org.apache.commons.configuration.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GraphBackedSearchIndexerTest {

    @Mock
    private Configuration configuration;

    @Mock
    private GraphProvider<AAGraph> graphProvider;

    @Mock
    private AAGraph<Object,Object> titanGraph;

    @Mock
    private GraphDatabaseManager titanManagement;

    @BeforeMethod
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testSearchIndicesAreInitializedOnConstructionWhenHAIsDisabled() throws IndexException, RepositoryException {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(false);
        when(graphProvider.get()).thenReturn(titanGraph);
        when(titanGraph.getManagementSystem()).thenReturn(titanManagement);
        when(titanManagement.containsPropertyKey(Constants.VERTEX_TYPE_PROPERTY_KEY)).thenReturn(true);

        GraphBackedSearchIndexer graphBackedSearchIndexer = new GraphBackedSearchIndexer(graphProvider, configuration);

        verify(titanManagement).containsPropertyKey(Constants.VERTEX_TYPE_PROPERTY_KEY);
    }

    @Test
    public void testSearchIndicesAreNotInitializedOnConstructionWhenHAIsEnabled() throws IndexException, RepositoryException {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(graphProvider.get()).thenReturn(titanGraph);
        when(titanGraph.getManagementSystem()).thenReturn(titanManagement);
        when(titanManagement.containsPropertyKey(Constants.VERTEX_TYPE_PROPERTY_KEY)).thenReturn(true);

        GraphBackedSearchIndexer graphBackedSearchIndexer = new GraphBackedSearchIndexer(graphProvider, configuration);

        verifyZeroInteractions(titanManagement);

    }

    @Test
    public void testIndicesAreReinitializedWhenServerBecomesActive() throws AtlasException {
        when(configuration.getBoolean(HAConfiguration.ATLAS_SERVER_HA_ENABLED_KEY, false)).thenReturn(true);
        when(graphProvider.get()).thenReturn(titanGraph);
        when(titanGraph.getManagementSystem()).thenReturn(titanManagement);
        when(titanManagement.containsPropertyKey(Constants.VERTEX_TYPE_PROPERTY_KEY)).thenReturn(true);

        GraphBackedSearchIndexer graphBackedSearchIndexer = new GraphBackedSearchIndexer(graphProvider, configuration);
        graphBackedSearchIndexer.instanceIsActive();

        verify(titanManagement).containsPropertyKey(Constants.VERTEX_TYPE_PROPERTY_KEY);
    }
}
