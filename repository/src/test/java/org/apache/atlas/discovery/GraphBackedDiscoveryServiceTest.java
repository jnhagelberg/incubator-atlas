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

package org.apache.atlas.discovery;

import static org.apache.atlas.typesystem.types.utils.TypesUtil.createClassTypeDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createOptionalAttrDef;
import static org.apache.atlas.typesystem.types.utils.TypesUtil.createRequiredAttrDef;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.atlas.AtlasException;
import org.apache.atlas.BaseRepositoryTest;
import org.apache.atlas.RepositoryMetadataModule;
import org.apache.atlas.RequestContext;
import org.apache.atlas.TestUtils;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

@Guice(modules = RepositoryMetadataModule.class)
public class GraphBackedDiscoveryServiceTest extends BaseRepositoryTest {

    @Inject
    private MetadataRepository repositoryService;

    @Inject
    private GraphBackedDiscoveryService discoveryService;

    private Collection<IDataType> getTypes(TypeSystem ts) throws AtlasException {
        Collection<IDataType> result = new ArrayList<IDataType>();
        for(String name : ts.getTypeNames()) {
            result.add(ts.getDataType(IDataType.class, name));
        }
        return result;
    }
    
    @BeforeClass
    public void setUp() throws Exception {
        super.setUp();
        Collection<String> oldTypeNames = new HashSet<String>();
        final TypeSystem typeSystem = TypeSystem.getInstance();
        oldTypeNames.addAll(typeSystem.getTypeNames());
        
        Collection<String> newTypeNames = new HashSet<String>();
        TestUtils.defineDeptEmployeeTypes(typeSystem);
        newTypeNames.addAll(typeSystem.getTypeNames());
        newTypeNames.removeAll(oldTypeNames);
        
        Collection<IDataType> newTypes = new ArrayList<IDataType>();
        for(String name : newTypeNames) {
            try {
                newTypes.add(typeSystem.getDataType(IDataType.class, name));                    
            } catch (AtlasException e) {
                e.printStackTrace();
            }
                       
        }
        GraphBackedSearchIndexer idx = new GraphBackedSearchIndexer(new AtlasGraphProvider());        
        idx.onAdd(newTypes);

        ITypedReferenceableInstance hrDept = TestUtils.createDeptEg1(typeSystem);
        repositoryService.createEntities(hrDept);

        ITypedReferenceableInstance jane = repositoryService.getEntityDefinition("Manager", "name", "Jane");
        Id janeGuid = jane.getId();
        ClassType personType = typeSystem.getDataType(ClassType.class, "Person");
        ITypedReferenceableInstance instance = personType.createInstance(janeGuid);
        instance.set("orgLevel", "L1");
        repositoryService.updateEntities(instance);
    }

    @BeforeMethod
    public void setupContext() {
        RequestContext.createContext();
    }

    @AfterClass
    public void tearDown() throws Exception {
        //super.tearDown();
    }

    @Test
    public void testSearchBySystemProperties() throws Exception {
        //system property in select
        String dslQuery = "from Department select __guid";

        String jsonResults = discoveryService.searchByDSL(dslQuery);
        assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        assertEquals(results.length(), 3);

        JSONArray rows = results.getJSONArray("rows");
        assertNotNull(rows);
        assertEquals(rows.length(), 1);
        assertNotNull(rows.getJSONObject(0).getString("__guid"));

        //system property in where clause
        String guid = rows.getJSONObject(0).getString("__guid");
        dslQuery = "Department where __guid = '" + guid + "' and __state = 'ACTIVE'";
        jsonResults = discoveryService.searchByDSL(dslQuery);
        assertNotNull(jsonResults);

        results = new JSONObject(jsonResults);
        assertEquals(results.length(), 3);

        rows = results.getJSONArray("rows");
        assertNotNull(rows);
        assertEquals(rows.length(), 1);
    }

    @Test
    public void testSearchByDSLReturnsEntity() throws Exception {
        String dslQuery = "from Department";

        String jsonResults = discoveryService.searchByDSL(dslQuery);
        assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        assertNotNull(typeName);
        assertEquals(typeName, "Department");

        JSONArray rows = results.getJSONArray("rows");
        assertNotNull(rows);
        assertEquals(rows.length(), 1);

        //Assert that entity state is set in the result entities
        String entityState = rows.getJSONObject(0).getJSONObject("$id$").getString("state");
        assertEquals(entityState, Id.EntityState.ACTIVE.name());
    }

    @Test(expectedExceptions = Throwable.class)
    public void testSearchByDSLBadQuery() throws Exception {
        String dslQuery = "from blah";

        discoveryService.searchByDSL(dslQuery);
        Assert.fail();
    }

    private String getPropertySyntax(String var, String property) {
            if(isGremlin3()) {
                //in gremlin 3, if we just get the value, an exception is thrown if the property
                //is not present.  We need to include an additional condition to check whether the property
                //exists.
                return String.format("%s.get().property('%s').isPresent() && %s.get().value('%s')",var,property,var,property);
            }
            else {
                return String.format("%s.%s",var,property);
            }

    }

    @Test
    public void testRawSearch1() throws Exception {
        // Query for all Vertices in Graph
        Object r = discoveryService.searchByGremlin("g.V().toList()");
        Assert.assertTrue(r instanceof List);
        List<Map<String, Object>> resultList = (List<Map<String, Object>>) r;
        Assert.assertTrue(resultList.size() > 0);
        System.out.println("search result = " + r);

        // Query for all Vertices of a Type
        r = discoveryService.searchByGremlin("g.V().filter{" + getPropertySyntax("it", Constants.ENTITY_TYPE_PROPERTY_KEY) + " == 'Department'}.toList()");
        Assert.assertTrue(r instanceof List);
        resultList = (List<Map<String, Object>>) r;
        Assert.assertTrue(resultList.size() > 0);
        System.out.println("search result = " + r);

        // Property Query: list all Person names
        r = discoveryService.searchByGremlin("g.V().filter{" + getPropertySyntax("it", Constants.ENTITY_TYPE_PROPERTY_KEY) + " == 'Person'}.'Person.name'.toList()");
        Assert.assertTrue(r instanceof List);
        resultList = (List<Map<String, Object>>) r;
        Assert.assertTrue(resultList.size() > 0);
        System.out.println("search result = " + r);
        List<Object> names = new ArrayList<Object>(resultList.size());
        for (Map<String, Object> vertexProps : resultList) {
            names.addAll(vertexProps.values());
        }
        for (String name : Arrays.asList("John", "Max")) {
            Assert.assertTrue(names.contains(name));
        }

        // Query for all Vertices modified after 01/01/2015 00:00:00 GMT
        r = discoveryService.searchByGremlin("g.V().filter{" + getPropertySyntax("it", Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY) + " > 1420070400000}.toList()");
        Assert.assertTrue(r instanceof List);
        resultList = (List<Map<String, Object>>) r;
        Assert.assertTrue(resultList.size() > 0);
        for (Map<String, Object> vertexProps : resultList) {
            Object object = vertexProps.get(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY);
            assertNotNull(object);
            Long timestampAsLong = Long.valueOf((String)object);
            Assert.assertTrue(timestampAsLong > 1420070400000L);
            object = vertexProps.get(Constants.TIMESTAMP_PROPERTY_KEY);
            assertNotNull(object);
        }
    }

    @DataProvider(name = "comparisonQueriesProvider")
    private Object[][] createComparisonQueries() {
        return new Object[][] {
            {"Person where (birthday < \"1950-01-01T02:35:58.440Z\" )", 0},
            {"Person where (birthday > \"1975-01-01T02:35:58.440Z\" )", 2},
            {"Person where (birthday >= \"1975-01-01T02:35:58.440Z\" )", 2},
            {"Person where (birthday <= \"1950-01-01T02:35:58.440Z\" )", 0},            
            {"Person where (birthday = \"1975-01-01T02:35:58.440Z\" )", 0},
            {"Person where (birthday != \"1975-01-01T02:35:58.440Z\" )", 2},
            
            {"Person where (isOrganDonor = true)", 2},
            {"Person where (isOrganDonor = false)", 1},
            {"Person where (isOrganDonor != false)", 2},
            {"Person where (isOrganDonor != true)", 1},            
            
            {"Person where (numberOfCars > 0)", 2},
            {"Person where (numberOfCars > 1)", 1},
            {"Person where (numberOfCars >= 1)", 2},
            {"Person where (numberOfCars < 2)", 2},
            {"Person where (numberOfCars <= 2)", 3},
            {"Person where (numberOfCars = 2)", 1},
            {"Person where (numberOfCars != 2)", 2},
            
            {"Person where (houseNumber > 0)", 2},
            {"Person where (houseNumber > 17)", 1},
            {"Person where (houseNumber >= 17)", 2},
            {"Person where (houseNumber < 153)", 2},
            {"Person where (houseNumber <= 153)", 3},
            {"Person where (houseNumber =  17)", 1},
            {"Person where (houseNumber != 12)", 3},
            
            {"Person where (carMileage > 0)", 2},
            {"Person where (carMileage > 13)", 1},
            {"Person where (carMileage >= 13)", 2},
            {"Person where (carMileage < 13364)", 2},
            {"Person where (carMileage <= 13364)", 3},
            {"Person where (carMileage =  13)", 1},
            {"Person where (carMileage != 13)", 2},
            
            {"Person where (shares > 0)", 2},
            {"Person where (shares > 13)", 2},
            {"Person where (shares >= 16000)", 1},
            {"Person where (shares < 13364)", 1},
            {"Person where (shares <= 15000)", 2},
            {"Person where (shares =  15000)", 1},
            {"Person where (shares != 1)", 3},
                       
            {"Person where (salary > 0)", 2},
            {"Person where (salary > 100000)", 2},
            {"Person where (salary >= 200000)", 1},
            {"Person where (salary < 13364)", 1},
            {"Person where (salary <= 150000)", 2},
            {"Person where (salary =  12334)", 0},
            {"Person where (salary != 12344)", 3},
            
            {"Person where (age > 36)", 1},
            {"Person where (age > 49)", 1},
            {"Person where (age >= 49)", 1},
            {"Person where (age < 50)", 2},
            {"Person where (age <= 35)", 2},
            {"Person where (age =  35)", 0},
            {"Person where (age != 35)", 3}
        };
    }
        
    @DataProvider(name = "dslQueriesProvider")
    private Object[][] createDSLQueries() {

        return new Object[][]{
                {"from hive_db", 3},
                {"hive_db", 3},
                {"hive_db where hive_db.name=\"Reporting\"", 1},
                {"hive_db hive_db.name = \"Reporting\"", 1},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner", 1},
                {"hive_db has name", 3},
                {"hive_db, hive_table", 8},
                {"View is JdbcAccess", 2},
                {"hive_db as db1, hive_table where db1.name = \"Reporting\"", isGremlin3() ? 2 : 0}, //Not working in Gremlin 2 - ATLAS-145
                // - Final working query -> discoveryService.searchByGremlin("L:{_var_0 = [] as Set;g.V().has(\"__typeName\", \"hive_db\").fill(_var_0);g.V().has(\"__superTypeNames\", \"hive_db\").fill(_var_0);_var_0._().as(\"db1\").in(\"__hive_table.db\").back(\"db1\").and(_().has(\"hive_db.name\", T.eq, \"Reporting\")).toList()}")
                /*
                {"hive_db, hive_process has name"}, //Invalid query
                {"hive_db where hive_db.name=\"Reporting\" and hive_db.createTime < " + System.currentTimeMillis()}
                */
                {"from hive_table", 8},
                {"hive_table", 8},
                {"hive_table isa Dimension", 3},
                {"hive_column where hive_column isa PII", 6},
                {"View is Dimension" , 2},
//                {"hive_column where hive_column isa PII select hive_column.name", 6}, //Not working - ATLAS-175
                {"hive_column select hive_column.name", 29},
                {"hive_column select name", 29},
                {"hive_column where hive_column.name=\"customer_id\"", 4},
                {"from hive_table select hive_table.name", 8},
                {"hive_db where (name = \"Reporting\")", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1", 1},
                {"hive_db where hive_db is JdbcAccess", 0}, //Not supposed to work
                {"hive_db hive_table", 8},
                {"hive_db where hive_db has name", 3},

                //this query works correctly with Gremlin 3.  Fails with Gremlin 2 --> ATLAS-145
                {"hive_db as db1 hive_table where (db1.name = \"Reporting\")", isGremlin3() ? 2 : 0},

                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 ", 1},
                
            /*
            todo: does not work - ATLAS-146
            {"hive_db where (name = \"Reporting\") and ((createTime + 1) > 0)"},
            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") select db1.name
            as dbName, tab.name as tabName"},
            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) or (db1.name = \"Reporting\") select db1.name
            as dbName, tab.name as tabName"},
            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner
             select db1.name as dbName, tab.name as tabName"},
            {"hive_db as db1 hive_table as tab where ((db1.createTime + 1) > 0) and (db1.name = \"Reporting\") or db1 has owner
             select db1.name as dbName, tab.name as tabName"},
            */
                // trait searches
                {"Dimension", 5},
                {"JdbcAccess", 2},
                {"ETL", 3},
                {"Metric", 5},
                {"PII", 6},
                {"`Log Data`", 4},

                /* Lineage queries are fired through ClosureQuery and are tested through HiveLineageJerseyResourceIt in webapp module.
                   Commenting out the below queries since DSL to Gremlin parsing/translation fails with lineage queries when there are array types
                   used within loop expressions which is the case with DataSet.inputs and outputs.`
                  // Lineage
                  {"Table LoadProcess outputTable"}, {"Table loop (LoadProcess outputTable)"},
                  {"Table as _loop0 loop (LoadProcess outputTable) withPath"},
                  {"Table as src loop (LoadProcess outputTable) as dest select src.name as srcTable, dest.name as "
                                        + "destTable withPath"},
                 */
//                {"hive_table as t, sd, hive_column as c where t.name=\"sales_fact\" select c.name as colName, c.dataType as "
//                        + "colType", 0}, //Not working - ATLAS-145 and ATLAS-166

                {"hive_table where name='sales_fact', db where name='Sales'", 1},
                {"hive_table where name='sales_fact', db where name='Reporting'", 0},
                {"hive_partition as p where values = ['2015-01-01']", 1},
//              {"StorageDesc select cols", 6} //Not working since loading of lists needs to be fixed yet
        };

    }

    private boolean isGremlin3() {
        return AtlasGraphProvider.getGraphInstance().getSupportedGremlinVersion() == GremlinVersion.THREE;

    }

    @DataProvider(name = "dslLimitQueriesProvider")
    private Object[][] createDSLQueriesWithLimit() {
        return new Object[][]{
                {"hive_column  limit 10 ", 10},
                {"hive_column select hive_column.name limit 10 ", 10},
                {"hive_column select hive_column.name  withPath", 29},
                {"hive_column select hive_column.name limit 10 withPath", 10},
                {"from hive_db", 3},
                {"from hive_db limit 2", 2},
                {"from hive_db limit 2 offset 0", 2},
                {"from hive_db limit 2 offset 1", 2},
                {"from hive_db limit 3 offset 1", 2},
                {"hive_db", 3}, //10
                {"hive_db where hive_db.name=\"Reporting\"", 1},
                {"hive_db where hive_db.name=\"Reporting\" limit 10 ", 1},
                {"hive_db hive_db.name = \"Reporting\"", 1},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner", 1},
                {"hive_db has name", 3},
                {"hive_db has name limit 2 offset 0", 2},
                {"hive_db has name limit 2 offset 1", 2},
                {"hive_db has name limit 10 offset 1", 2},
                {"hive_db has name limit 10 offset 0", 3},
                {"hive_db, hive_table", 8}, //20
                {"hive_db, hive_table limit 5", 5},
                {"hive_db, hive_table limit 5 offset 0", 5},
                {"hive_db, hive_table limit 5 offset 5", 3},

                {"View is JdbcAccess", 2},
                {"View is JdbcAccess limit 1", 1},
                {"View is JdbcAccess limit 2 offset 1", 1},
                {"hive_db as db1, hive_table where db1.name = \"Reporting\"", isGremlin3() ? 2 : 0}, //Not working - ATLAS-145


                {"from hive_table", 8},
                {"from hive_table limit 5", 5},
                {"from hive_table limit 5 offset 5", 3}, //30

                {"hive_table", 8},
                {"hive_table limit 5", 5},
                {"hive_table limit 5 offset 5", 3},

                {"hive_table isa Dimension", 3},
                {"hive_table isa Dimension limit 2", 2},
                {"hive_table isa Dimension limit 2 offset 0", 2},
                {"hive_table isa Dimension limit 2 offset 1", 2},
                {"hive_table isa Dimension limit 3 offset 1", 2},

                {"hive_column where hive_column isa PII", 6},
                {"hive_column where hive_column isa PII limit 5", 5}, //40
                {"hive_column where hive_column isa PII limit 5 offset 1", 5},
                {"hive_column where hive_column isa PII limit 5 offset 5", 1},


                {"View is Dimension" , 2},
                {"View is Dimension limit 1" , 1},
                {"View is Dimension limit 1 offset 1" , 1},
                {"View is Dimension limit 10 offset 1" , 1},

                {"hive_column select hive_column.name", 29},
                {"hive_column select hive_column.name limit 5", 5},
                {"hive_column select hive_column.name limit 5 offset 28", 1},

                {"hive_column select name", 29}, //50
                {"hive_column select name limit 5", 5},
                {"hive_column select name limit 5 offset 28 ", 1},

                {"hive_column where hive_column.name=\"customer_id\"", 4},
                {"hive_column where hive_column.name=\"customer_id\" limit 2", 2},
                {"hive_column where hive_column.name=\"customer_id\" limit 2 offset 1", 2},
                {"hive_column where hive_column.name=\"customer_id\" limit 10 offset 3", 1},

                {"from hive_table select hive_table.name", 8},
                {"from hive_table select hive_table.name limit 5", 5},
                {"from hive_table select hive_table.name limit 5 offset 5", 3},

                {"hive_db where (name = \"Reporting\")", 1}, //60
                {"hive_db where (name = \"Reporting\") limit 10", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1 limit 10", 1},
                {"hive_db where hive_db is JdbcAccess", 0}, //Not supposed to work
                {"hive_db hive_table", 8},
                {"hive_db hive_table limit 5", 5},
                {"hive_db hive_table limit 5 offset 5", 3},
                {"hive_db where hive_db has name", 3},
                {"hive_db where hive_db has name limit 5", 3},
                {"hive_db where hive_db has name limit 2 offset 0", 2}, //70
                {"hive_db where hive_db has name limit 2 offset 1", 2},

                {"hive_db as db1 hive_table where (db1.name = \"Reporting\")", isGremlin3() ? 2 : 0}, //Not working -> ATLAS-145
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 ", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 limit 10", 1},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 limit 10 offset 1", 0},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 limit 10 offset 0", 1},

                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10 offset 0", 1},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 limit 10 offset 5", 0},

                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 ", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10 offset 0", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10 offset 1", 0},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 10", 1},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 limit 0 offset 1", 0},

                // trait searches
                {"Dimension", 5},
                {"Dimension limit 2", 2},
                {"Dimension limit 2 offset 1", 2},
                {"Dimension limit 5 offset 4", 1},

                {"JdbcAccess", 2},
                {"JdbcAccess limit 5 offset 0", 2},
                {"JdbcAccess limit 2 offset 1", 1},
                {"JdbcAccess limit 1", 1},

                {"ETL", 3},
                {"ETL limit 2", 2},
                {"ETL limit 1", 1},
                {"ETL limit 1 offset 0", 1},
                {"ETL limit 2 offset 1", 2},

                {"Metric", 5},
                {"Metric limit 10", 5},
                {"Metric limit 2", 2},
                {"Metric limit 10 offset 1", 4},



                {"PII", 6},
                {"PII limit 10", 6},
                {"PII limit 2", 2},
                {"PII limit 10 offset 1", 5},

                {"`Log Data`", 4},
                {"`Log Data` limit 3", 3},
                {"`Log Data` limit 10 offset 2", 2},


                {"hive_table where name='sales_fact', db where name='Sales'", 1},
                {"hive_table where name='sales_fact', db where name='Sales' limit 10", 1},
                {"hive_table where name='sales_fact', db where name='Sales' limit 10 offset 1", 0},
                {"hive_table where name='sales_fact', db where name='Reporting'", 0},
                {"hive_table where name='sales_fact', db where name='Reporting' limit 10", 0},
                {"hive_table where name='sales_fact', db where name='Reporting' limit 10 offset 1", 0},
                {"hive_partition as p where values = ['2015-01-01']", 1},
                {"hive_partition as p where values = ['2015-01-01'] limit 10", 1},
                {"hive_partition as p where values = ['2015-01-01'] limit 10 offset 1", 0},

        };
    }

    @DataProvider(name = "dslOrderByQueriesProvider")
    private Object[][] createDSLQueriesWithOrderBy() {
        Boolean isAscending = Boolean.TRUE;
        return new Object[][]{
                //Not working, the problem is in server code not figuring out how to sort. not sure if it is valid use case.
//                {"hive_db  hive_table  orderby 'hive_db.owner'", 8, "owner", isAscending},
//                {"hive_db hive_table orderby 'hive_db.owner' limit 5", 5, "owner", isAscending},
//                {"hive_db hive_table orderby 'hive_db.owner' limit 5 offset 5", 3, "owner", isAscending},
                {"hive_column select hive_column.name orderby 'hive_column.name' limit 10 withPath", 10, "name", isAscending},
                {"hive_column select hive_column.name orderby 'hive_column.name' asc limit 10 withPath", 10, "name", isAscending},
                {"hive_column select hive_column.name orderby 'hive_column.name' desc limit 10 withPath", 10, "name", !isAscending},
                {"from hive_db orderby 'hive_db.owner' limit 3", 3, "owner", isAscending},
                {"hive_db where hive_db.name=\"Reporting\" orderby 'owner'", 1, "owner", isAscending},

                {"hive_db where hive_db.name=\"Reporting\" orderby 'hive_db.owner' limit 10 ", 1, "owner", isAscending},
                {"hive_db where hive_db.name=\"Reporting\" select name, owner orderby 'hive_db.name' ", 1, "name", isAscending},
                {"hive_db has name orderby 'hive_db.owner' limit 10 offset 0", 3, "owner", isAscending},

                {"from hive_table orderby 'hive_table.owner'", 8, "owner", isAscending},
                {"from hive_table orderby 'hive_table.owner' limit 8", 8, "owner", isAscending},

                {"hive_table orderby 'hive_table.owner'", 8, "owner", isAscending},
                {"hive_table orderby 'hive_table.owner' limit 8", 8, "owner", isAscending},
                {"hive_table orderby 'hive_table.owner' limit 8 offset 0", 8, "owner", isAscending},
                {"hive_table orderby 'hive_table.owner' desc limit 8 offset 0", 8, "owner", !isAscending},

                {"hive_table isa Dimension orderby 'hive_table.owner'", 3, "owner", isAscending},//order not working
                {"hive_table isa Dimension orderby 'hive_table.owner' limit 3", 3, "owner", isAscending},
                {"hive_table isa Dimension orderby 'hive_table.owner' limit 3 offset 0", 3, "owner", isAscending},
                {"hive_table isa Dimension orderby 'hive_table.owner' desc limit 3 offset 0", 3, "owner", !isAscending},

                {"hive_column where hive_column isa PII orderby 'hive_column.name'", 6, "name", isAscending},
                {"hive_column where hive_column isa PII orderby 'hive_column.name' limit 5", 5, "name", isAscending},
                {"hive_column where hive_column isa PII orderby 'hive_column.name' limit 5 offset 1", 5, "name", isAscending},
                {"hive_column where hive_column isa PII orderby 'hive_column.name' desc limit 5 offset 1", 5, "name", !isAscending},


                {"hive_column select hive_column.name orderby 'hive_column.name' ", 29, "hive_column.name", isAscending},
                {"hive_column select hive_column.name orderby 'hive_column.name' limit 5", 5, "hive_column.name", isAscending},
                {"hive_column select hive_column.name orderby 'hive_column.name' desc limit 5", 5, "hive_column.name", !isAscending},
                {"hive_column select hive_column.name orderby 'hive_column.name' limit 5 offset 28", 1, "hive_column.name", isAscending},

                {"hive_column select name orderby 'hive_column.name'", 29, "name", isAscending},
                {"hive_column select name orderby 'hive_column.name' limit 5", 5, "name", isAscending},
                {"hive_column select name orderby 'hive_column.name' desc", 29, "name", !isAscending},

                {"hive_column where hive_column.name=\"customer_id\" orderby 'hive_column.name'", 4, "name", isAscending},
                {"hive_column where hive_column.name=\"customer_id\" orderby 'hive_column.name' limit 2", 2, "name", isAscending},
                {"hive_column where hive_column.name=\"customer_id\" orderby 'hive_column.name' limit 2 offset 1", 2, "name", isAscending},

                {"from hive_table select owner orderby 'hive_table.owner'", 8, "owner", isAscending},
                {"from hive_table select owner orderby 'hive_table.owner' limit 5", 5, "owner", isAscending},
                {"from hive_table select owner orderby 'hive_table.owner' desc limit 5", 5, "owner", !isAscending},
                {"from hive_table select owner orderby 'hive_table.owner' limit 5 offset 5", 3, "owner", isAscending},

                {"hive_db where (name = \"Reporting\") orderby 'hive_db.name'", 1, "name", isAscending},
                {"hive_db where (name = \"Reporting\") orderby 'hive_db.name' limit 10", 1, "name", isAscending},
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1 orderby '_col_1'", 1, "_col_1", isAscending}, //will it work
                {"hive_db where (name = \"Reporting\") select name as _col_0, owner as _col_1  orderby '_col_1' limit 10", 1, "_col_1", isAscending},
                {"hive_db where hive_db has name orderby 'hive_db.owner'", 3, "owner", isAscending},
                {"hive_db where hive_db has name orderby 'hive_db.owner' limit 5", 3, "owner", isAscending},
                {"hive_db where hive_db has name orderby 'hive_db.owner' limit 2 offset 0", 2, "owner", isAscending},
                {"hive_db where hive_db has name orderby 'hive_db.owner' limit 2 offset 1", 2, "owner", isAscending},

                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 orderby '_col_1'", 1, "_col_1", isAscending},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 orderby '_col_1' limit 10", 1, "_col_1", isAscending},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 orderby '_col_1' limit 10 offset 1", 0, "_col_1", isAscending},
                {"hive_db where (name = \"Reporting\") select name as _col_0, (createTime + 1) as _col_1 orderby '_col_1' limit 10 offset 0", 1, "_col_1", isAscending},

                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby '_col_1' ", 1, "_col_1", isAscending},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby '_col_1' limit 10 ", 1, "_col_1", isAscending},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby '_col_1' limit 10 offset 0", 1, "_col_1", isAscending},
                {"hive_table where (name = \"sales_fact\" and createTime > \"2014-01-01\" ) select name as _col_0, createTime as _col_1 orderby '_col_1' limit 10 offset 5", 0, "_col_1", isAscending},

                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' ", 1, "_col_0", isAscending},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' limit 10 offset 0", 1, "_col_0", isAscending},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' limit 10 offset 1", 0, "_col_0", isAscending},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' limit 10", 1, "_col_0", isAscending},
                {"hive_table where (name = \"sales_fact\" and createTime >= \"2014-12-11T02:35:58.440Z\" ) select name as _col_0, createTime as _col_1 orderby '_col_0' limit 0 offset 1", 0, "_col_0", isAscending},

        };
    }

    @Test(dataProvider = "dslOrderByQueriesProvider")
    public void  testSearchByDSLQueriesWithOrderBy(String dslQuery, Integer expectedNumRows, String orderBy, boolean ascending) throws Exception {
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        assertEquals(results.length(), 3);

        Object query = results.get("query");
        assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        assertNotNull(typeName);

        JSONArray rows = results.getJSONArray("rows");

        assertNotNull(rows);
        assertEquals(rows.length(), expectedNumRows.intValue()); // some queries may not have any results
        List<String> returnedList = new ArrayList<String> ();
        for (int i = 0; i < rows.length(); i++) {
            JSONObject row = rows.getJSONObject(i);
            try
            {
                returnedList.add(row.get(orderBy).toString());
            }
            catch(Exception ex)
            {
                System.out.println( " Exception occured " + ex.getMessage());
            }
        }
        Iterator<String> iter = returnedList.iterator();
        String _current = null, _prev = null;
        //Following code compares the results in rows and makes sure data is sorted as expected.
        while(iter.hasNext())
        {
            _prev = _current;
            _current = iter.next().toLowerCase();
            if (_prev != null && _prev.compareTo(_current) != 0)
            {
                if(ascending)
                {
                    Assert.assertTrue(_prev.compareTo(_current) < 0);
                }
                else
                {
                    Assert.assertTrue(_prev.compareTo(_current) > 0);
                }
            }
        }

        System.out.println("query [" + dslQuery + "] returned [" + rows.length() + "] rows");
    }

    @Test(dataProvider = "dslQueriesProvider")
    public void  testSearchByDSLQueries(String dslQuery, Integer expectedNumRows) throws Exception {
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        assertNotNull(typeName);

        JSONArray rows = results.getJSONArray("rows");
        assertNotNull(rows);
        assertEquals( rows.length(), expectedNumRows.intValue(), "query [" + dslQuery + "] returned [" + rows.length() + "] rows.  Expected " + expectedNumRows.intValue() + " rows."); // some queries may not have any results
        System.out.println("query [" + dslQuery + "] returned [" + rows.length() + "] rows");
    }
    
    @Test(dataProvider = "comparisonQueriesProvider")
    public void  testDataTypeComparisonQueries(String dslQuery, Integer expectedNumRows) throws Exception {
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        assertNotNull(typeName);

        JSONArray rows = results.getJSONArray("rows");
        assertNotNull(rows);
        assertEquals( rows.length(), expectedNumRows.intValue(), "query [" + dslQuery + "] returned [" + rows.length() + "] rows.  Expected " + expectedNumRows.intValue() + " rows."); // some queries may not have any results
        System.out.println("query [" + dslQuery + "] returned [" + rows.length() + "] rows");
    }

    @Test(dataProvider = "dslLimitQueriesProvider")
    public void  testSearchByDSLQueriesWithLimit(String dslQuery, Integer expectedNumRows) throws Exception {
        System.out.println("Executing dslQuery = " + dslQuery);
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        assertEquals(results.length(), 3);
        System.out.println("results = " + results);

        Object query = results.get("query");
        assertNotNull(query);

        JSONObject dataType = results.getJSONObject("dataType");
        assertNotNull(dataType);
        String typeName = dataType.getString("typeName");
        assertNotNull(typeName);

        JSONArray rows = results.getJSONArray("rows");
        assertNotNull(rows);
        System.out.println("query [" + dslQuery + "] returned [" + rows.length() + "] rows.  Expected: " + expectedNumRows.intValue() + ".  Result: " + ((rows.length() == expectedNumRows.intValue()) ? "SUCCESS" : "FAILURE"));
        assertEquals(rows.length(), expectedNumRows.intValue()); // some queries may not have any results

    }

    @DataProvider(name = "invalidDslQueriesProvider")
    private Object[][] createInvalidDSLQueries() {
        return new String[][]{{"from Unknown"}, {"Unknown"}, {"Unknown is Blah"},};
    }

    @Test(dataProvider = "invalidDslQueriesProvider", expectedExceptions = DiscoveryException.class)
    public void testSearchByDSLInvalidQueries(String dslQuery) throws Exception {
        System.out.println("Executing dslQuery = " + dslQuery);
        discoveryService.searchByDSL(dslQuery);
        Assert.fail();
    }

    @Test
    public void testSearchForTypeInheritance() throws Exception {
        createTypesWithMultiLevelInheritance();
        createInstances();

        String dslQuery = "from D where a = 1";
        String jsonResults = discoveryService.searchByDSL(dslQuery);
        assertNotNull(jsonResults);

        JSONObject results = new JSONObject(jsonResults);
        System.out.println("results = " + results);
    }

    /*
     * Type Hierarchy is:
     *   A(a)
     *   B(b) extends A
     *   C(c) extends B
     *   D(d) extends C
     */
    private void createTypesWithMultiLevelInheritance() throws Exception {
        HierarchicalTypeDefinition A = createClassTypeDef("A", null, createRequiredAttrDef("a", DataTypes.INT_TYPE));

        HierarchicalTypeDefinition B =
                createClassTypeDef("B", ImmutableSet.of("A"), createOptionalAttrDef("b", DataTypes.BOOLEAN_TYPE));

        HierarchicalTypeDefinition C =
                createClassTypeDef("C", ImmutableSet.of("B"), createOptionalAttrDef("c", DataTypes.BYTE_TYPE));

        HierarchicalTypeDefinition D =
                createClassTypeDef("D", ImmutableSet.of("C"), createOptionalAttrDef("d", DataTypes.SHORT_TYPE));

        TypeSystem.getInstance().defineClassTypes(A, B, C, D);
    }

    private void createInstances() throws Exception {
        Referenceable instance = new Referenceable("D");
        instance.set("d", 1);
        instance.set("c", 1);
        instance.set("b", true);
        instance.set("a", 1);

        ClassType deptType = TypeSystem.getInstance().getDataType(ClassType.class, "D");
        ITypedReferenceableInstance typedInstance = deptType.convert(instance, Multiplicity.REQUIRED);

        repositoryService.createEntities(typedInstance);
    }
}