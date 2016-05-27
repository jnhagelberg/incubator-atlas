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

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphIndex;
import org.apache.atlas.repository.graphdb.AtlasGraphManagement;
import org.apache.atlas.repository.graphdb.AtlasPropertyKey;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.commons.lang.RandomStringUtils;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.google.inject.Inject;

@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedSearchIndexerTest {
    
    @Inject
    private GraphProvider<AtlasGraph> graphProvider;

    @Inject
    private GraphBackedSearchIndexer graphBackedSearchIndexer;
    
    @Test
    public void verifySystemMixedIndexes() {

        AtlasGraph titanGraph = graphProvider.get();
        AtlasGraphManagement managementSystem = titanGraph.getManagementSystem();
    
    
        AtlasGraphIndex edgeIndex = managementSystem.getGraphIndex(Constants.EDGE_INDEX);
        assertNotNull(edgeIndex);
        assertTrue(edgeIndex.isMixedIndex());
        assertTrue(edgeIndex.isEdgeIndex());
    }

    @Test
    public void verifySystemCompositeIndexes() {
        AtlasGraph titanGraph = graphProvider.get();
        AtlasGraphManagement managementSystem = titanGraph.getManagementSystem();

        verifySystemCompositeIndex(managementSystem, Constants.GUID_PROPERTY_KEY, true);
        verifyVertexIndexContains(managementSystem, Constants.GUID_PROPERTY_KEY);

        verifySystemCompositeIndex(managementSystem, Constants.ENTITY_TYPE_PROPERTY_KEY, false);
        verifyVertexIndexContains(managementSystem, Constants.ENTITY_TYPE_PROPERTY_KEY);

        verifySystemCompositeIndex(managementSystem, Constants.SUPER_TYPES_PROPERTY_KEY, false);
        //not added to vertex index - mult many

        verifySystemCompositeIndex(managementSystem, Constants.TRAIT_NAMES_PROPERTY_KEY, false);
        //not added to vertex index - mult many
    }

    @Test
    public void verifyFullTextIndex() {
        AtlasGraph titanGraph = graphProvider.get();
        AtlasGraphManagement managementSystem = titanGraph.getManagementSystem();

        AtlasGraphIndex fullTextIndex = managementSystem.getGraphIndex(Constants.FULLTEXT_INDEX);
        assertTrue(fullTextIndex.isMixedIndex());

        Arrays.asList(fullTextIndex.getFieldKeys()).contains(
                managementSystem.getPropertyKey(Constants.ENTITY_TEXT_PROPERTY_KEY));
    }

    @Test
    public void verifyTypeStoreIndexes() {
        AtlasGraph titanGraph = graphProvider.get();
        AtlasGraphManagement managementSystem = titanGraph.getManagementSystem();

        verifySystemCompositeIndex(managementSystem, Constants.TYPENAME_PROPERTY_KEY, true);
        verifyVertexIndexContains(managementSystem, Constants.TYPENAME_PROPERTY_KEY);

        verifySystemCompositeIndex(managementSystem, Constants.VERTEX_TYPE_PROPERTY_KEY, false);
        verifyVertexIndexContains(managementSystem, Constants.VERTEX_TYPE_PROPERTY_KEY);
    }

    @Test
    public void verifyUserDefinedTypeIndex() throws AtlasException {
        AtlasGraph titanGraph = graphProvider.get();
        AtlasGraphManagement managementSystem = titanGraph.getManagementSystem();

        TypeSystem typeSystem = TypeSystem.getInstance();

        String enumName = "randomEnum" + RandomStringUtils.randomAlphanumeric(10);
        EnumType managedType = typeSystem.defineEnumType(enumName, new EnumValue("randomEnumValue", 0));

        HierarchicalTypeDefinition<ClassType> databaseTypeDefinition =
                createClassTypeDef("Database", "Database type description", null,
                        TypesUtil.createUniqueRequiredAttrDef("name", DataTypes.STRING_TYPE),
                        TypesUtil.createUniqueRequiredAttrDef("managedType", managedType));

        ClassType databaseType = typeSystem.defineClassType(databaseTypeDefinition);
        graphBackedSearchIndexer.onAdd(Arrays.asList(databaseType));

        verifySystemCompositeIndex(managementSystem, "Database.name", false);
        verifyVertexIndexContains(managementSystem, "Database.name");

        verifySystemCompositeIndex(managementSystem, "Database.managedType", false);
        verifyVertexIndexContains(managementSystem, "Database.managedType");
    }

    private void verifyVertexIndexContains(AtlasGraphManagement managementSystem, String indexName) {
        AtlasGraphIndex vertexIndex = managementSystem.getGraphIndex(Constants.VERTEX_INDEX);
        Collection<AtlasPropertyKey> fieldKeys = vertexIndex.getFieldKeys();
        assertTrue(fieldKeys.contains(managementSystem.getPropertyKey(indexName)));
    }

    private void verifySystemCompositeIndex(AtlasGraphManagement managementSystem, String indexName, boolean isUnique) {
        AtlasGraphIndex guidIndex = managementSystem.getGraphIndex(indexName);
        assertNotNull(guidIndex);
        assertTrue(guidIndex.isCompositeIndex());
        if (isUnique) {
            assertTrue(guidIndex.isUnique());
        } else {
            assertFalse(guidIndex.isUnique());
        }
    }
}
