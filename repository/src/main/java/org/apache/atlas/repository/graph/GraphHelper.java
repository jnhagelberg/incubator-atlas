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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAGraph;
import org.apache.atlas.repository.graphdb.AAGraphQuery;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for graph operations.
 */
public final class GraphHelper {

    private static final Logger LOG = LoggerFactory.getLogger(GraphHelper.class);
    public static final String EDGE_LABEL_PREFIX = "__";

    private static final TypeSystem typeSystem = TypeSystem.getInstance();

    private static final GraphHelper INSTANCE = new GraphHelper(AtlasGraphProvider.getGraphInstance());

    private AAGraph<?,?> graph;

    private GraphHelper(AAGraph<?,?> graph) {
        this.graph = graph;
    }

    public static GraphHelper getInstance() {
        return INSTANCE;
    }

    public AAVertex<?,?> createVertexWithIdentity(ITypedReferenceableInstance typedInstance, Set<String> superTypeNames) {
        final String guid = UUID.randomUUID().toString();

        final AAVertex<?,?> vertexWithIdentity = createVertexWithoutIdentity(typedInstance.getTypeName(),
                new Id(guid, 0 , typedInstance.getTypeName()), superTypeNames);

        // add identity
        setProperty(vertexWithIdentity, Constants.GUID_PROPERTY_KEY, guid);

        // add version information
        setProperty(vertexWithIdentity, Constants.VERSION_PROPERTY_KEY, typedInstance.getId().version);

        return vertexWithIdentity;
    }

    public <V,E> AAVertex<V,E> createVertexWithoutIdentity(String typeName, Id typedInstanceId, Set<String> superTypeNames) {
        LOG.debug("Creating vertex for type {} id {}", typeName,
                typedInstanceId != null ? typedInstanceId._getId() : null);
        AAGraph<V,E> graph = getGraph();
        final AAVertex<V,E> vertexWithoutIdentity = graph.addVertex(null);

        // add type information
        setProperty(vertexWithoutIdentity, Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);

        // add super types
        for (String superTypeName : superTypeNames) {
            addProperty(vertexWithoutIdentity, Constants.SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        // add timestamp information
        setProperty(vertexWithoutIdentity, Constants.TIMESTAMP_PROPERTY_KEY, System.currentTimeMillis());

        return vertexWithoutIdentity;
    }

    public <V,E> AAEdge<V,E> addEdge(AAVertex<V,E> fromVertex, AAVertex<V,E> toVertex, String edgeLabel) {
        LOG.debug("Adding edge for {} -> label {} -> {}", fromVertex, edgeLabel, toVertex);
        AAGraph<V,E> graph = getGraph();
        AAEdge<V,E> edge = graph.addEdge(null, fromVertex, toVertex, edgeLabel);
        LOG.debug("Added edge for {} -> label {}, id {} -> {}", fromVertex, edgeLabel, edge.getId(), toVertex);
        return edge;
    }

    public <V,E> AAVertex<V,E> findVertex(String propertyKey, Object value) {
        LOG.debug("Finding vertex for {}={}", propertyKey, value);
        AAGraph<V,E> graph = getGraph();
        AAGraphQuery<V,E> query = graph.query().has(propertyKey, value);
      
        Iterator<AAVertex<V,E>> results = query.vertices().iterator();
        // returning one since entityType, qualifiedName should be unique
        return results.hasNext() ? results.next() : null;
    }

    public static <V,E> Iterable<AAEdge<V,E>> getOutGoingEdgesByLabel(AAVertex<V,E> instanceVertex, String edgeLabel) {
        if(instanceVertex != null && edgeLabel != null) {
            return instanceVertex.getEdges(AADirection.OUT, edgeLabel);
        }
        return null;
    }

    public <V,E> AAEdge<V,E> getOutGoingEdgeById(String edgeId) {
        if(edgeId != null) {
            AAGraph<V,E> graph = getGraph();
            return graph.getEdge(edgeId);
        }
        return null;
    }

    public static String vertexString(final AAVertex<?,?> vertex) {
        StringBuilder properties = new StringBuilder();
        for (String propertyKey : vertex.getPropertyKeys()) {
            Collection<?> propertyValues = vertex.getPropertyValues(propertyKey);
            properties.append(propertyKey).append("=").append(propertyValues.toString()).append(", ");
        }

        return "v[" + vertex.getId() + "], Properties[" + properties + "]";
    }

    public static String edgeString(final AAEdge<?,?> edge) {
        return "e[" + edge.getLabel() + "], [" + edge.getVertex(AADirection.OUT) + " -> " + edge.getLabel() + " -> "
                + edge.getVertex(AADirection.IN) + "]";
    }

    public static void setProperty(AAVertex<?,?> vertex, String propertyName, Object value) {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        Object existValue = vertex.getProperty(propertyName);
        if(value == null || (value instanceof Collection && ((Collection) value).isEmpty())) {
            if(existValue != null) {
                LOG.info("Removing property - {} value from vertex {}", propertyName, vertex);
                vertex.removeProperty(propertyName);
            }
        } else {
            if (!value.equals(existValue)) {
                vertex.setProperty(propertyName, value);
                LOG.debug("Set property {} = \"{}\" to vertex {}", propertyName, value, vertex);
            }
        }
    }

    public static void addProperty(AAVertex<?,?> vertex, String propertyName, Object value) {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        vertex.addProperty(propertyName, value);
    }

    public <V,E> AAEdge<V,E> removeRelation(String edgeId, boolean cascade) {
        LOG.debug("Removing edge with id {}", edgeId);
        AAGraph<V,E> graph = getGraph();
        final AAEdge<V,E> edge = graph.getEdge(edgeId);
        graph.removeEdge(edge);
        LOG.info("Removed edge {}", edge);
        if (cascade) {
           AAVertex<?,?> referredVertex = edge.getVertex(AADirection.IN);
           removeVertex(referredVertex);
        }
        return edge;
    }
    
    /**
     * Remove the specified edge from the graph.
     * 
     * @param edge
     */
    public <V,E> void removeEdge(AAEdge<V,E> edge) {
        LOG.debug("Removing edge {}", edge);
        AAGraph<V,E> graph = getGraph();
        graph.removeEdge(edge);
        LOG.info("Removed edge {}", edge);
    }
    
    /**
     * Return the edge and target vertex for the specified edge ID.
     * 
     * @param edgeId
     * @return edge and target vertex
     */
    public <V,E> Pair<AAEdge<V,E>, AAVertex<V,E>> getEdgeAndTargetVertex(String edgeId) {
        AAGraph<V,E> graph = getGraph();        
        final AAEdge<V,E> edge = graph.getEdge(edgeId);
        AAVertex<V,E> referredVertex = edge.getVertex(AADirection.IN);
        return Pair.of(edge, referredVertex);
    }
    
    /**
     * Remove the specified vertex from the graph.
     * 
     * @param vertex
     */
    public <V,E> void removeVertex(AAVertex<V,E> vertex) {
        LOG.debug("Removing vertex {}", vertex);
        AAGraph<V,E> graph = getGraph();
        graph.removeVertex(vertex);
        LOG.info("Removed vertex {}", vertex);
    }

    public <V,E> AAVertex<V,E> getVertexForGUID(String guid) throws EntityNotFoundException {
        return getVertexForProperty(Constants.GUID_PROPERTY_KEY, guid);
    }


    public <V,E> AAVertex<V,E> getVertexForProperty(String propertyKey, Object value) throws EntityNotFoundException {
        AAVertex<V,E> instanceVertex = findVertex(propertyKey, value);
        if (instanceVertex == null) {
            LOG.debug("Could not find a vertex with {}={}", propertyKey, value);
            throw new EntityNotFoundException("Could not find an entity in the repository with " + propertyKey + "="
                + value);
        } else {
            LOG.debug("Found a vertex {} with {}={}", instanceVertex, propertyKey, value);
        }

        return instanceVertex;
    }

    public static String getQualifiedFieldName(ITypedInstance typedInstance, AttributeInfo attributeInfo) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typedInstance.getTypeName());
        return getQualifiedFieldName(dataType, attributeInfo.name);
    }

    public static String getQualifiedFieldName(IDataType dataType, String attributeName) throws AtlasException {
        return dataType.getTypeCategory() == DataTypes.TypeCategory.STRUCT ? dataType.getName() + "." + attributeName
            // else class or trait
            : ((HierarchicalType) dataType).getQualifiedName(attributeName);
    }

    public static String getTraitLabel(String typeName, String attrName) {
        return typeName + "." + attrName;
    }

    public static Object getProperty(AAVertex<?,?> entityVertex, String propertyName) {
        
        //these are the only two properties that are defined as
        //being multiplicity many properties in Gremlin.  Todo - 
        //generalize this.
        if(propertyName.equals(Constants.TRAIT_NAMES_PROPERTY_KEY) ||
           propertyName.equals(Constants.SUPER_TYPES_PROPERTY_KEY)) {
            return entityVertex.getPropertyValues(propertyName);
        }
        else {
            return entityVertex.getProperty(propertyName);
        }
    }
    
    public static List<String> getTraitNames(AAVertex<?,?> entityVertex) {
        ArrayList<String> traits = new ArrayList<>();
        Collection<String> propertyValues = entityVertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY);
        for(String value : propertyValues) {
            traits.add(value);
        }        
        return traits;
    }

    public static String getEdgeLabel(ITypedInstance typedInstance, AttributeInfo aInfo) throws AtlasException {
        IDataType dataType = typeSystem.getDataType(IDataType.class, typedInstance.getTypeName());
        return getEdgeLabel(dataType, aInfo);
    }

    public static String getEdgeLabel(IDataType dataType, AttributeInfo aInfo) throws AtlasException {
        return GraphHelper.EDGE_LABEL_PREFIX + getQualifiedFieldName(dataType, aInfo.name);
    }

    public static Id getIdFromVertex(String dataTypeName, AAVertex<?,?> vertex) {
        return new Id(vertex.<String>getProperty(Constants.GUID_PROPERTY_KEY),
            vertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), dataTypeName);
    }

    public static String getTypeName(AAVertex<?,?> instanceVertex) {
        return instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
    }

    /**
     * For the given type, finds an unique attribute and checks if there is an existing instance with the same
     * unique value
     *
     * @param classType
     * @param instance
     * @return
     * @throws AtlasException
     */
    public AAVertex getVertexForInstanceByUniqueAttribute(ClassType classType, IReferenceableInstance instance)
        throws AtlasException {
        LOG.debug("Checking if there is an instance with the same unique attributes for instance {}", instance);
        AAVertex result = null;
        for (AttributeInfo attributeInfo : classType.fieldMapping().fields.values()) {
            if (attributeInfo.isUnique) {
                String propertyKey = getQualifiedFieldName(classType, attributeInfo.name);
                try {
                    result = getVertexForProperty(propertyKey, instance.get(attributeInfo.name));
                    LOG.debug("Found vertex by unique attribute : " + propertyKey + "=" + instance.get(attributeInfo.name));
                } catch (EntityNotFoundException e) {
                    //Its ok if there is no entity with the same unique value
                }
            }
        }

        return result;
    }

    private <V,E> AAGraph<V,E> getGraph() {
        return (AAGraph<V,E>)graph;
    }
    
    public static void dumpToLog(final AAGraph<?,?> graph) {
        LOG.debug("*******************Graph Dump****************************");
        LOG.debug("Vertices of {}", graph);
        for (AAVertex<?,?> vertex : graph.getVertices()) {
            LOG.debug(vertexString(vertex));
        }

        LOG.debug("Edges of {}", graph);
        for (AAEdge<?,?> edge : graph.getEdges()) {
            LOG.debug(edgeString(edge));
        }
        LOG.debug("*******************Graph Dump****************************");
    }
}