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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasElement;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasGraphQuery;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.IReferenceableInstance;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.exception.EntityNotFoundException;
import org.apache.atlas.typesystem.exception.TypeNotFoundException;
import org.apache.atlas.typesystem.json.InstanceSerialization;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.persistence.Id.EntityState;
import org.apache.atlas.typesystem.persistence.ReferenceableInstance;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.ValueConversionException;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.apache.atlas.utils.ParamChecker;
import org.codehaus.jettison.json.JSONArray;
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

    private AtlasGraph<?,?> graph;

    private GraphHelper(AtlasGraph<?,?> graph) {
        this.graph = graph;
    }

    public static GraphHelper getInstance() {
        return INSTANCE;
    }

    public AtlasVertex<?,?> createVertexWithIdentity(ITypedReferenceableInstance typedInstance, Set<String> superTypeNames) {
        final String guid = UUID.randomUUID().toString();

        final AtlasVertex<?,?> vertexWithIdentity = createVertexWithoutIdentity(typedInstance.getTypeName(),
                new Id(guid, 0, typedInstance.getTypeName()), superTypeNames);

        // add identity
        setProperty(vertexWithIdentity, Constants.GUID_PROPERTY_KEY, guid);

        // add version information
        setProperty(vertexWithIdentity, Constants.VERSION_PROPERTY_KEY, typedInstance.getId().version);

        return vertexWithIdentity;
    }

    public <V,E> AtlasVertex<V,E> createVertexWithoutIdentity(String typeName, Id typedInstanceId, Set<String> superTypeNames) {
        LOG.debug("Creating vertex for type {} id {}", typeName,
                typedInstanceId != null ? typedInstanceId._getId() : null);
        AtlasGraph<V,E> graph = getGraph();
        final AtlasVertex<V,E> vertexWithoutIdentity = graph.addVertex();

        // add type information
        setProperty(vertexWithoutIdentity, Constants.ENTITY_TYPE_PROPERTY_KEY, typeName);


        // add super types
        for (String superTypeName : superTypeNames) {
            addProperty(vertexWithoutIdentity, Constants.SUPER_TYPES_PROPERTY_KEY, superTypeName);
        }

        // add state information
        setProperty(vertexWithoutIdentity, Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());

        // add timestamp information
        setProperty(vertexWithoutIdentity, Constants.TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        setProperty(vertexWithoutIdentity, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                RequestContext.get().getRequestTime());

        return vertexWithoutIdentity;
    }

    public <V,E> AtlasEdge<V,E> addEdge(AtlasVertex<V,E> fromVertex, AtlasVertex<V,E> toVertex, String edgeLabel) {
        LOG.debug("Adding edge for {} -> label {} -> {}", string(fromVertex), edgeLabel, string(toVertex));
        AtlasGraph<V,E> graph = getGraph();
        AtlasEdge<V,E> edge = graph.addEdge(fromVertex, toVertex, edgeLabel);

        setProperty(edge, Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        setProperty(edge, Constants.TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());
        setProperty(edge, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, RequestContext.get().getRequestTime());

        LOG.debug("Added {}", string(edge));
        return edge;
    }

    public <V,E> AtlasEdge<V,E> getOrCreateEdge(AtlasVertex<V,E> outVertex, AtlasVertex<V,E> inVertex, String edgeLabel) {
        Iterable<AtlasEdge<V,E>> edges = inVertex.getEdges(AtlasEdgeDirection.IN, edgeLabel);
        for (AtlasEdge<V,E> edge : edges) {
            if (edge.getOutVertex().getId().toString().equals(outVertex.getId().toString())) {
                Id.EntityState edgeState = getState(edge);
                if (edgeState == null || edgeState == Id.EntityState.ACTIVE) {
                return edge;
            }
        }
        }
        return addEdge(outVertex, inVertex, edgeLabel);
    }


    public AtlasEdge getEdgeByEdgeId(AtlasVertex outVertex, String edgeLabel, String edgeId) {
        if (edgeId == null) {
            return null;
        }
        return graph.getEdge(edgeId);

        //TODO get edge id is expensive. Use this logic. But doesn't work for now
        /**
        Iterable<Edge> edges = outVertex.getEdges(Direction.OUT, edgeLabel);
        for (Edge edge : edges) {
            if (edge.getId().toString().equals(edgeId)) {
                return edge;
            }
        }
        return null;
         **/
    }

     /**
     * Args of the format prop1, key1, prop2, key2...
     * Searches for a vertex with prop1=key1 && prop2=key2
     * @param args
     * @return vertex with the given property keys
     * @throws EntityNotFoundException
     */
    private <V,E> AtlasVertex<V,E> findVertex(Object... args) throws EntityNotFoundException {
        StringBuilder condition = new StringBuilder();
        AtlasGraph<V,E> graph = getGraph();
        AtlasGraphQuery<V,E> query = graph.query();
        for (int i = 0 ; i < args.length; i+=2) {
            query = query.has((String) args[i], args[i+1]);
            condition.append(args[i]).append(" = ").append(args[i+1]).append(", ");
        }
        String conditionStr = condition.toString();
        LOG.debug("Finding vertex with {}", conditionStr);

        Iterator<AtlasVertex<V,E>> results = query.vertices().iterator();
        // returning one since entityType, qualifiedName should be unique
        AtlasVertex<V,E> vertex = results.hasNext() ? results.next() : null;

        //in some cases, even though the state property has been changed to DELETED, the 
        //graph query will return elements
        if (!GraphHelper.elementExists(vertex)) {
            LOG.debug("Could not find a vertex with {}", condition.toString());
            throw new EntityNotFoundException("Could not find an entity in the repository with " + conditionStr);
        } else {
            LOG.debug("Found a vertex {} with {}", string(vertex), conditionStr);
        }

        return vertex;
    }

    public static <V,E> Iterable<AtlasEdge<V,E>> getOutGoingEdgesByLabel(AtlasVertex<V,E> instanceVertex, String edgeLabel) {
        if(GraphHelper.elementExists(instanceVertex ) && edgeLabel != null) {
            return instanceVertex.getEdges(AtlasEdgeDirection.OUT, edgeLabel);
        }

        return null;
    }

        /**
     * Returns the active edge for the given edge label.
     * If the vertex is deleted and there is no active edge, it returns the latest deleted edge
     * @param vertex
     * @param edgeLabel
     * @return
     */
    public static <V,E> AtlasEdge<V,E> getEdgeForLabel(AtlasVertex<V,E> vertex, String edgeLabel) {
        Iterable<AtlasEdge<V,E>> edges = GraphHelper.getOutGoingEdgesByLabel(vertex, edgeLabel);
        AtlasEdge<V,E> latestDeletedEdge = null;
        if(edges != null) {

            long latestDeletedEdgeTime = Long.MIN_VALUE;
            for(AtlasEdge<V,E> edge : edges) {
                Id.EntityState edgeState = getState(edge);
                if (edgeState == null || edgeState == Id.EntityState.ACTIVE) {
                    LOG.debug("Found {}", string(edge));
                    return edge;
                } else {
                    Long modificationTime = edge.getProperty(Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY, Long.class);
                    if (modificationTime != null && modificationTime >= latestDeletedEdgeTime) {
                        latestDeletedEdgeTime = modificationTime;
                        latestDeletedEdge = edge;
                    }
                }
            }
        }

        //If the vertex is deleted, return latest deleted edge
        LOG.debug("Found {}", latestDeletedEdge == null ? "null" : string(latestDeletedEdge));
        return latestDeletedEdge;
    }


    public static String vertexString(final AtlasVertex<?,?> vertex) {
        StringBuilder properties = new StringBuilder();
        for (String propertyKey : vertex.getPropertyKeys()) {
            Collection<?> propertyValues = vertex.getPropertyValues(propertyKey, Object.class);
            properties.append(propertyKey).append("=").append(propertyValues.toString()).append(", ");
        }

        return "v[" + vertex.getId() + "], Properties[" + properties + "]";
    }

    public static String edgeString(final AtlasEdge<?,?> edge) {
        return "e[" + edge.getLabel() + "], [" + edge.getOutVertex() + " -> " + edge.getLabel() + " -> "
                + edge.getInVertex() + "]";
    }

    public static <T extends AtlasElement> void setProperty(T element, String propertyName, Object value) {
        String elementStr = string(element);
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, elementStr);
        Object existValue = element.getProperty(propertyName, Object.class);
        if(value == null || (value instanceof Collection && ((Collection) value).isEmpty())) {
            if(existValue != null) {
                LOG.info("Removing property - {} value from {}", propertyName, elementStr);
                element.removeProperty(propertyName);
            }
        } else {
            if (!value.equals(existValue)) {
                element.setProperty(propertyName, value);
                LOG.debug("Set property {} = \"{}\" to {}", propertyName, value, elementStr);
            }
        }
    }

    private static <T extends AtlasElement> String string(T element) {
        if (element instanceof AtlasVertex) {
            return string((AtlasVertex) element);
        } else if (element instanceof AtlasEdge) {
            return string((AtlasEdge)element);
        }
        return element.toString();
    }

    public static void addProperty(AtlasVertex<?,?> vertex, String propertyName, Object value) {
        LOG.debug("Adding property {} = \"{}\" to vertex {}", propertyName, value, string(vertex));
        vertex.addProperty(propertyName, value);
    }

    /**
     * Remove the specified edge from the graph.
     *
     * @param edge
     */
    public <V,E> void removeEdge(AtlasEdge<V,E> edge) {
        String edgeString = string(edge);
        LOG.debug("Removing {}", edgeString);
        AtlasGraph<V,E> graph = getGraph();
        graph.removeEdge(edge);
        LOG.info("Removed edge {}", edge);
    }


    /**
     * Remove the specified vertex from the graph.
     *
     * @param vertex
     */
    public <V,E> void removeVertex(AtlasVertex<V,E> vertex) {
        String vertexString = string(vertex);
        LOG.debug("Removing {}", vertexString);
        AtlasGraph<V,E> graph = getGraph();
        graph.removeVertex(vertex);
        LOG.info("Removed {}", vertexString);
    }

    public <V,E> AtlasVertex<V,E> getVertexForGUID(String guid) throws EntityNotFoundException {
        return findVertex(Constants.GUID_PROPERTY_KEY, guid);
    }

    public <V,E> AtlasVertex<V,E> getVertexForProperty(String propertyKey, Object value) throws EntityNotFoundException {
        
    	AtlasVertex<V,E> result = findVertex(propertyKey, value, Constants.STATE_PROPERTY_KEY, Id.EntityState.ACTIVE.name());
        if(GraphHelper.getState(result) == EntityState.DELETED) {
        	//in some cases, the graph query will return elements whose state is actually deleted.  This
        	//can happen if we query the graph before the updated state for the vertex has been
        	//propagated into the graph index.
        	StringBuilder conditionString = new StringBuilder();
        	conditionString.append(propertyKey);
        	conditionString.append(" = ");
        	conditionString.append(value);
        	conditionString.append(" and state__ = ACTIVE");
        	LOG.debug("Could not find a vertex with {}", conditionString);
        	throw new EntityNotFoundException("Could not find an entity in the repository with " + conditionString);
        }
        return result;
    }

    public static String getQualifiedNameForMapKey(String prefix, String key) {
        return prefix + "." + key;
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

    public static Object getProperty(AtlasVertex<?,?> entityVertex, String propertyName) {

        //these are the only two properties that are defined as
        //being multiplicity many properties in Gremlin.  Todo -
        //generalize this.
        if(propertyName.equals(Constants.TRAIT_NAMES_PROPERTY_KEY) ||
           propertyName.equals(Constants.SUPER_TYPES_PROPERTY_KEY)) {
            return entityVertex.getPropertyValues(propertyName, String.class);
        }
        else {
            return entityVertex.getProperty(propertyName, Object.class);
        }
    }

    public static List<String> getTraitNames(AtlasVertex<?,?> entityVertex) {
        ArrayList<String> traits = new ArrayList<>();
        Collection<String> propertyValues = entityVertex.getPropertyValues(Constants.TRAIT_NAMES_PROPERTY_KEY, String.class);
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

    public static Id getIdFromVertex(String dataTypeName, AtlasVertex<?,?> vertex) {
        return new Id(vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class),
            vertex.getProperty(Constants.VERSION_PROPERTY_KEY, Integer.class), dataTypeName);
    }

    public static String getIdFromVertex(AtlasVertex<?,?> vertex) {
        return vertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
    }

    public static String getTypeName(AtlasVertex<?,?> instanceVertex) {
        return instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
    }

    public static Id.EntityState getState(AtlasElement element) {
        String state = getStateAsString(element);
        return state == null ? null : Id.EntityState.valueOf(state);
    }

    public static String getStateAsString(AtlasElement element) {
        return element.getProperty(Constants.STATE_PROPERTY_KEY, String.class);
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
    public AtlasVertex getVertexForInstanceByUniqueAttribute(ClassType classType, IReferenceableInstance instance)
        throws AtlasException {
        LOG.debug("Checking if there is an instance with the same unique attributes for instance {}", instance.toShortString());
        AtlasVertex result = null;
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

    private <V,E> AtlasGraph<V,E> getGraph() {
        return (AtlasGraph<V,E>)graph;
    }

    public static void dumpToLog(final AtlasGraph<?,?> graph) {
        LOG.debug("*******************Graph Dump****************************");
        LOG.debug("Vertices of {}", graph);
        for (AtlasVertex<?,?> vertex : graph.getVertices()) {
            LOG.debug(vertexString(vertex));
        }

        LOG.debug("Edges of {}", graph);
        for (AtlasEdge<?,?> edge : graph.getEdges()) {
            LOG.debug(edgeString(edge));
        }
        LOG.debug("*******************Graph Dump****************************");
    }

    public static String string(ITypedReferenceableInstance instance) {
        return String.format("entity[type=%s guid=%]", instance.getTypeName(), instance.getId()._getId());
    }

    public static String string(AtlasVertex<?,?> vertex) {
        if (LOG.isDebugEnabled()) {
            return String.format("vertex[id=%s type=%s guid=%s]", vertex.getId().toString(), getTypeName(vertex),
                    getIdFromVertex(vertex));
        } else {
            return String.format("vertex[id=%s]", vertex.getId().toString());
        }
    }

    public static String string(AtlasEdge<?,?> edge) {
   		if (LOG.isDebugEnabled()) {
            return String.format("edge[id=%s label=%s from %s -> to %s]", edge.getId().toString(), edge.getLabel(),
                    string(edge.getOutVertex()), string(edge.getInVertex()));
        } else {
            return String.format("edge[id=%s]", edge.getId().toString());
        }
    }


    public static AttributeInfo getAttributeInfoForSystemAttributes(String field) {
        switch (field) {
        case Constants.STATE_PROPERTY_KEY:
        case Constants.GUID_PROPERTY_KEY:
            return TypesUtil.newAttributeInfo(field, DataTypes.STRING_TYPE);

        case Constants.TIMESTAMP_PROPERTY_KEY:
        case Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY:
            return TypesUtil.newAttributeInfo(field, DataTypes.LONG_TYPE);
        }
        return null;
    }

    public static ITypedReferenceableInstance[] deserializeClassInstances(TypeSystem typeSystem, String entityInstanceDefinition)
    throws AtlasException {
        try {
            JSONArray referableInstances = new JSONArray(entityInstanceDefinition);
            ITypedReferenceableInstance[] instances = new ITypedReferenceableInstance[referableInstances.length()];
            for (int index = 0; index < referableInstances.length(); index++) {
                Referenceable entityInstance =
                        InstanceSerialization.fromJsonReferenceable(referableInstances.getString(index), true);
                final String entityTypeName = entityInstance.getTypeName();
                ParamChecker.notEmpty(entityTypeName, "Entity type cannot be null");

                ClassType entityType = typeSystem.getDataType(ClassType.class, entityTypeName);

                //Both assigned id and values are required for full update
                //classtype.convert() will remove values if id is assigned. So, set temp id, convert and
                // then replace with original id
                Id origId = entityInstance.getId();
                entityInstance.replaceWithNewId(new Id(entityInstance.getTypeName()));
                ITypedReferenceableInstance typedInstrance = entityType.convert(entityInstance, Multiplicity.REQUIRED);
                ((ReferenceableInstance)typedInstrance).replaceWithNewId(origId);
                instances[index] = typedInstrance;
            }
            return instances;
        } catch(ValueConversionException | TypeNotFoundException  e) {
            throw e;
        } catch (Exception e) {  // exception from deserializer
            LOG.error("Unable to deserialize json={}", entityInstanceDefinition, e);
            throw new IllegalArgumentException("Unable to deserialize json", e);
        }
    }

    public static boolean elementExists(AtlasElement v) {
        return v != null && v.exists();
    }
}