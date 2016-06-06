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

package org.apache.atlas.repository.typestore;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.atlas.AtlasException;
import org.apache.atlas.GraphTransaction;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.GraphHelper;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.TypesDef;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;
import org.apache.atlas.typesystem.types.EnumType;
import org.apache.atlas.typesystem.types.EnumTypeDefinition;
import org.apache.atlas.typesystem.types.EnumValue;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.StructTypeDefinition;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeUtils;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.codehaus.jettison.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.Singleton;

@Singleton
public class GraphBackedTypeStore<V,E> implements ITypeStore {
    public static final String VERTEX_TYPE = "typeSystem";
    private static final String PROPERTY_PREFIX = Constants.INTERNAL_PROPERTY_KEY_PREFIX + "type.";
    public static final String SUPERTYPE_EDGE_LABEL = PROPERTY_PREFIX + ".supertype";

    private static Logger LOG = LoggerFactory.getLogger(GraphBackedTypeStore.class);

    private final AtlasGraph<V,E> graph;

    private GraphHelper graphHelper = GraphHelper.getInstance();

    @Inject
    public GraphBackedTypeStore(AtlasGraphProvider graphProvider) {
        graph = (AtlasGraph<V,E>)graphProvider.get();
    }

    @Override
    @GraphTransaction
    public void store(TypeSystem typeSystem, ImmutableList<String> typeNames) throws AtlasException {
        for (String typeName : typeNames) {
            IDataType dataType = typeSystem.getDataType(IDataType.class, typeName);
            LOG.debug("Processing {}.{}.{} in type store", dataType.getTypeCategory(), dataType.getName(), dataType.getDescription());
            switch (dataType.getTypeCategory()) {
            case ENUM:
                storeInGraph((EnumType) dataType);
                break;

            case STRUCT:
                StructType structType = (StructType) dataType;
                storeInGraph(typeSystem, dataType.getTypeCategory(), dataType.getName(), dataType.getDescription(),
                        ImmutableList.copyOf(structType.infoToNameMap.keySet()), ImmutableSet.<String>of());
                break;

            case TRAIT:
            case CLASS:
                HierarchicalType type = (HierarchicalType) dataType;
                storeInGraph(typeSystem, dataType.getTypeCategory(), dataType.getName(), type.getDescription(), type.immediateAttrs,
                        type.superTypes);
                break;

            default:    //Ignore primitive/collection types as they are covered under references
                break;
            }
        }
    }

    private void addProperty(AtlasVertex<V,E> vertex, String propertyName, Object value) throws AtlasException {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        vertex.setProperty(propertyName, value);
    }

    private void addJsonProperty(AtlasVertex<V,E> vertex, String propertyName, String value) throws AtlasException {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        vertex.setJsonProperty(propertyName, value);
    }

    private void addListProperty(AtlasVertex<V,E> vertex, String propertyName, List<String> value) throws AtlasException {
        LOG.debug("Setting property {} = \"{}\" to vertex {}", propertyName, value, vertex);
        vertex.setListProperty(propertyName, value);
    }

    private void storeInGraph(EnumType dataType) throws AtlasException {
        AtlasVertex<V,E> vertex = createVertex(dataType.getTypeCategory(), dataType.getName(), dataType.getDescription());
        List<String> values = new ArrayList<>(dataType.values().size());
        for (EnumValue enumValue : dataType.values()) {
            String key = getPropertyKey(dataType.getName(), enumValue.value);
            addProperty(vertex, key, enumValue.ordinal);
            values.add(enumValue.value);
        }
        addListProperty(vertex, getPropertyKey(dataType.getName()), values);
    }

    private String getPropertyKey(String name) {
        return PROPERTY_PREFIX + name;
    }

    private String getPropertyKey(String parent, String child) {
        return PROPERTY_PREFIX + parent + "." + child;
    }

    String getEdgeLabel(String parent, String child) {
        return PROPERTY_PREFIX + "edge." + parent + "." + child;
    }

    private void storeInGraph(TypeSystem typeSystem, DataTypes.TypeCategory category, String typeName, String typeDescription,
            ImmutableList<AttributeInfo> attributes, ImmutableSet<String> superTypes) throws AtlasException {
        AtlasVertex<V,E> vertex = createVertex(category, typeName, typeDescription);
        List<String> attrNames = new ArrayList<>();
        if (attributes != null) {
            for (AttributeInfo attribute : attributes) {
                String propertyKey = getPropertyKey(typeName, attribute.name);
                try {
                    addJsonProperty(vertex, propertyKey, attribute.toJson());
                } catch (JSONException e) {
                    throw new StorageException(typeName, e);
                }
                attrNames.add(attribute.name);
                addReferencesForAttribute(typeSystem, vertex, attribute);
            }
        }
        addListProperty(vertex, getPropertyKey(typeName), attrNames);

        //Add edges for hierarchy
        if (superTypes != null) {
            for (String superTypeName : superTypes) {
                HierarchicalType superType = typeSystem.getDataType(HierarchicalType.class, superTypeName);
                AtlasVertex<V,E> superVertex = createVertex(superType.getTypeCategory(), superTypeName, superType.getDescription());
                graphHelper.getOrCreateEdge(vertex, superVertex, SUPERTYPE_EDGE_LABEL);
            }
        }
    }

    private void addReferencesForAttribute(TypeSystem typeSystem, AtlasVertex<V,E> vertex, AttributeInfo attribute)
            throws AtlasException {
        ImmutableList<String> coreTypes = typeSystem.getCoreTypes();
        List<IDataType> attrDataTypes = new ArrayList<>();
        IDataType attrDataType = attribute.dataType();
        String vertexTypeName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);

        switch (attrDataType.getTypeCategory()) {
        case ARRAY:
            String attrType = TypeUtils.parseAsArrayType(attrDataType.getName());
            IDataType elementType = typeSystem.getDataType(IDataType.class, attrType);
            attrDataTypes.add(elementType);
            break;

        case MAP:
            String[] attrTypes = TypeUtils.parseAsMapType(attrDataType.getName());
            IDataType keyType = typeSystem.getDataType(IDataType.class, attrTypes[0]);
            IDataType valueType = typeSystem.getDataType(IDataType.class, attrTypes[1]);
            attrDataTypes.add(keyType);
            attrDataTypes.add(valueType);
            break;

        case ENUM:
        case STRUCT:
        case CLASS:
            attrDataTypes.add(attrDataType);
            break;

        case PRIMITIVE: //no vertex for primitive type, hence no edge required
            break;

        default:
            throw new IllegalArgumentException(
                    "Attribute cannot reference instances of type : " + attrDataType.getTypeCategory());
        }

        for (IDataType attrType : attrDataTypes) {
            if (!coreTypes.contains(attrType.getName())) {
                AtlasVertex<V,E> attrVertex = createVertex(attrType.getTypeCategory(), attrType.getName(), attrType.getDescription());
                String label = getEdgeLabel(vertexTypeName, attribute.name);
                graphHelper.getOrCreateEdge(vertex, attrVertex, label);
            }
        }
    }

    @Override
    @GraphTransaction
    public TypesDef restore() throws AtlasException {
        //Get all vertices for type system
        Iterator<AtlasVertex<V,E>> vertices =
                graph.query().has(Constants.VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE).vertices().iterator();

        ImmutableList.Builder<EnumTypeDefinition> enums = ImmutableList.builder();
        ImmutableList.Builder<StructTypeDefinition> structs = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<ClassType>> classTypes = ImmutableList.builder();
        ImmutableList.Builder<HierarchicalTypeDefinition<TraitType>> traits = ImmutableList.builder();

        while (vertices.hasNext()) {
            AtlasVertex<V,E> vertex = vertices.next();
            //temporary workaround

            DataTypes.TypeCategory typeCategory = getTypeCategory(vertex);
            String typeName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
            String typeDescription = vertex.getProperty(Constants.TYPEDESCRIPTION_PROPERTY_KEY, String.class);
            LOG.info("Restoring type {}.{}.{}", typeCategory, typeName, typeDescription);
            switch (typeCategory) {
            case ENUM:
                enums.add(getEnumType(vertex));
                break;

            case STRUCT:
                AttributeDefinition[] attributes = getAttributes(vertex, typeName);
                structs.add(new StructTypeDefinition(typeName, typeDescription, attributes));
                break;

            case CLASS:
                ImmutableSet<String> superTypes = getSuperTypes(vertex);
                attributes = getAttributes(vertex, typeName);
                classTypes.add(new HierarchicalTypeDefinition(ClassType.class, typeName, typeDescription, superTypes, attributes));
                break;

            case TRAIT:
                superTypes = getSuperTypes(vertex);
                attributes = getAttributes(vertex, typeName);
                traits.add(new HierarchicalTypeDefinition(TraitType.class, typeName, typeDescription, superTypes, attributes));
                break;

            default:
                throw new IllegalArgumentException("Unhandled type category " + typeCategory);
            }
        }
        return TypesUtil.getTypesDef(enums.build(), structs.build(), traits.build(), classTypes.build());
    }

    private TypeCategory getTypeCategory(AtlasVertex<V, E> vertex) {
        Object result =  vertex.getProperty(Constants.TYPE_CATEGORY_PROPERTY_KEY, Object.class);
        if(result instanceof TypeCategory) {
            return (TypeCategory)result;
        }
        return TypeCategory.valueOf(String.valueOf(result));
    }

    private EnumTypeDefinition getEnumType(AtlasVertex<V,E> vertex) throws AtlasException {
        String typeName = vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
        String typeDescription = vertex.getProperty(Constants.TYPEDESCRIPTION_PROPERTY_KEY, String.class);
        List<EnumValue> enumValues = new ArrayList<>();
        List<String> values = vertex.getListProperty(getPropertyKey(typeName));
        for (String value : values) {
            String valueProperty = getPropertyKey(typeName, value);
            enumValues.add(new EnumValue(value, vertex.getProperty(valueProperty, Integer.class)));
        }
        return new EnumTypeDefinition(typeName, typeDescription, enumValues.toArray(new EnumValue[enumValues.size()]));
    }

    private ImmutableSet<String> getSuperTypes(AtlasVertex<V,E> vertex) {
        Set<String> superTypes = new HashSet<>();
        Iterator<AtlasEdge<V,E>> edges = vertex.getEdges(AtlasEdgeDirection.OUT, SUPERTYPE_EDGE_LABEL).iterator();
        while (edges.hasNext()) {
            AtlasEdge<V,E> edge = edges.next();
            superTypes.add(edge.getInVertex().getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class));
        }
        return ImmutableSet.copyOf(superTypes);
    }

    private AttributeDefinition[] getAttributes(AtlasVertex<V,E> vertex, String typeName) throws AtlasException {
        List<AttributeDefinition> attributes = new ArrayList<>();
        List<String> attrNames = vertex.getListProperty(getPropertyKey(typeName));
        if (attrNames != null) {
            for (String attrName : attrNames) {
                try {
                    String propertyKey = getPropertyKey(typeName, attrName);
                    attributes.add(AttributeInfo.fromJson((String) vertex.getJsonProperty(propertyKey)));
                } catch (JSONException e) {
                    throw new AtlasException(e);
                }
            }
        }
        return attributes.toArray(new AttributeDefinition[attributes.size()]);
    }

    private String toString(AtlasVertex<V,E> vertex) {
        return PROPERTY_PREFIX + vertex.getProperty(Constants.TYPENAME_PROPERTY_KEY, String.class);
    }

    /**
     * Find vertex for the given type category and name, else create new vertex
     * @param category
     * @param typeName
     * @return vertex
     */
    AtlasVertex<V,E> findVertex(DataTypes.TypeCategory category, String typeName) {
        LOG.debug("Finding vertex for {}.{}", category, typeName);

        Iterator<AtlasVertex<V,E>> results = graph.query().has(Constants.TYPENAME_PROPERTY_KEY, typeName).vertices().iterator();
        AtlasVertex<V,E> vertex = null;
        if (results != null && results.hasNext()) {
            //There should be just one vertex with the given typeName
            vertex = results.next();
        }
        return vertex;
    }

    private AtlasVertex<V,E> createVertex(DataTypes.TypeCategory category, String typeName, String typeDescription) throws AtlasException {
        AtlasVertex<V,E> vertex = findVertex(category, typeName);
        if (! GraphHelper.elementExists(vertex)) {
            LOG.debug("Adding vertex {}{}", PROPERTY_PREFIX, typeName);
            vertex = graph.addVertex();
            addProperty(vertex, Constants.VERTEX_TYPE_PROPERTY_KEY, VERTEX_TYPE); // Mark as type vertex
            addProperty(vertex, Constants.TYPE_CATEGORY_PROPERTY_KEY, category);
            addProperty(vertex, Constants.TYPENAME_PROPERTY_KEY, typeName);
        }
        if (typeDescription != null) {
            String oldDescription = getPropertyKey(Constants.TYPEDESCRIPTION_PROPERTY_KEY);
            if (!typeDescription.equals(oldDescription)) {
                addProperty(vertex, Constants.TYPEDESCRIPTION_PROPERTY_KEY, typeDescription);
            }
        } else {
            LOG.debug(" type description is null ");
        }
        return vertex;
    }
}
