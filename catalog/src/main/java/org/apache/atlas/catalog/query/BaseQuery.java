/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas.catalog.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.atlas.catalog.Request;
import org.apache.atlas.catalog.VertexWrapper;
import org.apache.atlas.catalog.definition.ResourceDefinition;
import org.apache.atlas.catalog.exception.ResourceNotFoundException;
import org.apache.atlas.catalog.projection.Projection;
import org.apache.atlas.catalog.projection.ProjectionResult;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.typesystem.persistence.Id;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import com.tinkerpop.pipes.Pipe;
import com.tinkerpop.pipes.filter.PropertyFilterPipe;

/**
 * Base Query implementation.
 */
public abstract class BaseQuery implements AtlasQuery {
    protected final QueryExpression queryExpression;
    protected final ResourceDefinition resourceDefinition;
    protected final Request request;

    public BaseQuery(QueryExpression queryExpression, ResourceDefinition resourceDefinition, Request request) {
        this.queryExpression = queryExpression;
        this.resourceDefinition = resourceDefinition;
        this.request = request;
    }

    public Collection<Map<String, Object>> execute() throws ResourceNotFoundException {
        Collection<Map<String, Object>> resultMaps = new ArrayList<>();

        for (Vertex vertex : executeQuery()) {
            resultMaps.add(processPropertyMap(wrapVertex(vertex)));
        }
        return resultMaps;
    }

    private List<Vertex> executeQuery() {
        GremlinPipeline pipeline = buildPipeline().as("root");

        Pipe expressionPipe = queryExpression.asPipe();
        try {
            // AlwaysQuery returns null for pipe
            List<Vertex> vertices =  expressionPipe == null ? pipeline.toList() :
                    pipeline.add(expressionPipe).back("root").toList();

            // Even non-mutating queries can result in objects being created in
            // the graph such as new fields or property keys. So, it is important
            // to commit the implicit query after execution, otherwise the uncommitted
            // transaction will still be associated with the thread when it is re-pooled.
            getGraph().commit();
            return vertices;
        } catch (Throwable e) {
            getGraph().rollback();
            throw e;
        }
    }

    protected GremlinPipeline buildPipeline() {
        GremlinPipeline pipeline = getRootVertexPipeline();
        Pipe queryPipe = getQueryPipe();
        if (queryPipe != null) {
            pipeline.add(queryPipe);
        }
        //todo: may be more efficient to move the notDeleted pipe after the expression pipe
        pipeline.add(getNotDeletedPipe());
        return pipeline;
    }

    protected abstract Pipe getQueryPipe();

    protected GremlinPipeline getRootVertexPipeline() {
        return new GremlinPipeline(getGraph().getVertices());
    }

    protected Pipe getNotDeletedPipe() {
        return new PropertyFilterPipe(Constants.STATE_PROPERTY_KEY, Compare.EQUAL,
                Id.EntityState.ACTIVE.name());
    }

    protected Map<String, Object> processPropertyMap(VertexWrapper vertex) {
        Map<String, Object> propertyMap = resourceDefinition.filterProperties(
                request, vertex.getPropertyMap());
        addHref(propertyMap);

        return request.getCardinality() == Request.Cardinality.INSTANCE ?
                applyProjections(vertex, propertyMap) :
                propertyMap;
    }

    protected void addHref(Map<String, Object> propertyMap) {
        String href = resourceDefinition.resolveHref(propertyMap);
        if (href != null) {
            propertyMap.put("href", href);
        }
    }

    protected Map<String, Object> applyProjections(VertexWrapper vertex, Map<String, Object> propertyMap) {
        for (Projection p : resourceDefinition.getProjections().values()) {
            for (ProjectionResult projectionResult : p.values(vertex)) {
                if (p.getCardinality() == Projection.Cardinality.MULTIPLE) {
                    propertyMap.put(projectionResult.getName(), projectionResult.getPropertyMaps());
                } else {
                    for (Map<String, Object> projectionMap : projectionResult.getPropertyMaps()) {
                        propertyMap.put(projectionResult.getName(), projectionMap);
                    }
                }
            }
        }
        return propertyMap;
    }

    protected QueryExpression getQueryExpression() {
        return queryExpression;
    }

    protected ResourceDefinition getResourceDefinition() {
        return resourceDefinition;
    }

    protected Request getRequest() {
        return request;
    }

    //todo: abstract
    // Underlying method is synchronized and caches the graph in a static field
    protected AtlasGraph getGraph() {
        return AtlasGraphProvider.getGraphInstance();
    }

    protected VertexWrapper wrapVertex(Vertex v) {
        return new VertexWrapper(v, resourceDefinition);
    }
}
