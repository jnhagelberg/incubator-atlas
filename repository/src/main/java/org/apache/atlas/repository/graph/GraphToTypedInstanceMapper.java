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



import org.apache.atlas.repository.graphdb.AAGraph;
import org.apache.atlas.repository.graphdb.AAEdge;
import org.apache.atlas.repository.graphdb.AAVertex;
import org.apache.atlas.repository.graphdb.AADirection;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.typesystem.ITypedInstance;
import org.apache.atlas.typesystem.ITypedReferenceableInstance;
import org.apache.atlas.typesystem.ITypedStruct;
import org.apache.atlas.typesystem.persistence.Id;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TraitType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class GraphToTypedInstanceMapper<V,E> {

    private static final Logger LOG = LoggerFactory.getLogger(GraphToTypedInstanceMapper.class);
    private static TypeSystem typeSystem = TypeSystem.getInstance();
    private final AAGraph<V,E> graph;

    public GraphToTypedInstanceMapper(AAGraph<V,E> graph) {
        this.graph = graph;
    }

    public ITypedReferenceableInstance mapGraphToTypedInstance(String guid, AAVertex<V,E> instanceVertex)
        throws AtlasException {

        LOG.debug("Mapping graph root vertex {} to typed instance for guid {}", instanceVertex, guid);
        String typeName = instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY);
        List<String> traits = GraphHelper.getTraitNames(instanceVertex);

        Id id = new Id(guid, instanceVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY), typeName);
        LOG.debug("Created id {} for instance type {}", id, typeName);

        ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
        ITypedReferenceableInstance typedInstance =
            classType.createInstance(id, traits.toArray(new String[traits.size()]));

        mapVertexToInstance(instanceVertex, typedInstance, classType.fieldMapping().fields);
        mapVertexToInstanceTraits(instanceVertex, typedInstance, traits);

        return typedInstance;
    }

    private void mapVertexToInstanceTraits(AAVertex instanceVertex, ITypedReferenceableInstance typedInstance,
        List<String> traits) throws AtlasException {
        for (String traitName : traits) {
            LOG.debug("mapping trait {} to instance", traitName);
            TraitType traitType = typeSystem.getDataType(TraitType.class, traitName);
            mapVertexToTraitInstance(instanceVertex, typedInstance, traitName, traitType);
        }
    }

    public void mapVertexToInstance(AAVertex instanceVertex, ITypedInstance typedInstance,
        Map<String, AttributeInfo> fields) throws AtlasException {

        LOG.debug("Mapping vertex {} to instance {} for fields", instanceVertex, typedInstance.getTypeName(),
            fields);
        for (AttributeInfo attributeInfo : fields.values()) {
            mapVertexToAttribute(instanceVertex, typedInstance, attributeInfo);
        }
    }


    private void mapVertexToAttribute(AAVertex instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("Mapping attributeInfo {}", attributeInfo.name);
        final IDataType dataType = attributeInfo.dataType();
        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);

        switch (dataType.getTypeCategory()) {
        case PRIMITIVE:
            mapVertexToPrimitive(instanceVertex, typedInstance, attributeInfo);
            break;  // add only if vertex has this attribute

        case ENUM:
            if (instanceVertex.getProperty(vertexPropertyName) == null) {
                return;
            }

            typedInstance.set(attributeInfo.name,
                dataType.convert(instanceVertex.<String>getProperty(vertexPropertyName),
                    Multiplicity.REQUIRED));
            break;

        case ARRAY:
            mapVertexToArrayInstance(instanceVertex, typedInstance, attributeInfo, vertexPropertyName);
            break;

        case MAP:
            mapVertexToMapInstance(instanceVertex, typedInstance, attributeInfo, vertexPropertyName);
            break;

        case STRUCT:
            mapVertexToStructInstance(instanceVertex, typedInstance, attributeInfo);
            break;

        case TRAIT:
            // do NOTHING - handled in class
            break;

        case CLASS:
            String relationshipLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);
            Object idOrInstance = mapVertexToClassReference(instanceVertex, attributeInfo, relationshipLabel,
                attributeInfo.dataType());
            if (idOrInstance != null) {
                typedInstance.set(attributeInfo.name, idOrInstance);
            }
            break;

        default:
            break;
        }
    }

    private Object mapVertexToClassReference(AAVertex<V,E> instanceVertex, AttributeInfo attributeInfo,
        String relationshipLabel, IDataType dataType) throws AtlasException {
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        Iterator<AAEdge<V,E>> results = instanceVertex.getEdges(AADirection.OUT, relationshipLabel).iterator();
        if (results.hasNext()) {
            final AAVertex<V,E> referenceVertex = results.next().getVertex(AADirection.IN);
            if (referenceVertex != null) {
                final String guid = referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                LOG.debug("Found vertex {} for label {} with guid {}", referenceVertex, relationshipLabel, guid);
                if (attributeInfo.isComposite) {
                    //Also, when you retrieve a type's instance, you get the complete object graph of the composites
                    LOG.debug("Found composite, mapping vertex to instance");
                    return mapGraphToTypedInstance(guid, referenceVertex);
                } else {
                    Id referenceId =
                        new Id(guid, referenceVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                            dataType.getName());
                    LOG.debug("Found non-composite, adding id {} ", referenceId);
                    return referenceId;
                }
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private void mapVertexToArrayInstance(AAVertex<V,E> instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo, String propertyName) throws AtlasException {
        LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
        List list = instanceVertex.getProperty(propertyName);
        if (list == null || list.size() == 0) {
            return;
        }
        DataTypes.ArrayType arrayType = (DataTypes.ArrayType) attributeInfo.dataType();
        final IDataType elementType = arrayType.getElemType();

        String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + propertyName;
        ArrayList values = new ArrayList();
        for (int index = 0; index < list.size(); index++) {
            values.add(mapVertexToCollectionEntry(instanceVertex, attributeInfo, elementType, list.get(index),
                edgeLabel));
        }

        if (values.size() > 0) {
            typedInstance.set(attributeInfo.name, values);
        }
    }

    private Object mapVertexToCollectionEntry(AAVertex instanceVertex, AttributeInfo attributeInfo,
        IDataType elementType, Object value, String edgeLabel) throws AtlasException {
        switch (elementType.getTypeCategory()) {
        case PRIMITIVE:
        case ENUM:
            return value;

        case ARRAY:
        case MAP:
        case TRAIT:
            // do nothing
            break;

        case STRUCT:
            return getStructInstanceFromVertex(instanceVertex, elementType, attributeInfo.name, edgeLabel,
                (String) value);

        case CLASS:
            return mapVertexToClassReference(instanceVertex, attributeInfo, edgeLabel, elementType, (String) value);

        default:
            break;
        }

        throw new IllegalArgumentException();
    }

    @SuppressWarnings("unchecked")
    private void mapVertexToMapInstance(AAVertex<V,E> instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo, final String propertyName) throws AtlasException {
        LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
        List<String> keys = instanceVertex.getProperty(propertyName);
        if (keys == null || keys.size() == 0) {
            return;
        }
        DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
        final IDataType valueType = mapType.getValueType();

        HashMap values = new HashMap();
        for (String key : keys) {
            final String keyPropertyName = propertyName + "." + key;
            final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + keyPropertyName;
            final Object keyValue = instanceVertex.getProperty(keyPropertyName);
            Object mapValue = mapVertexToCollectionEntry(instanceVertex, attributeInfo, valueType, keyValue, edgeLabel);
            if (mapValue != null) {
                values.put(key, mapValue);
            }
        }

        if (!values.isEmpty()) {
            typedInstance.set(attributeInfo.name, values);
        }
    }

    private ITypedStruct getStructInstanceFromVertex(AAVertex<V,E> instanceVertex, IDataType elemType,
        String attributeName, String relationshipLabel, String edgeId) throws AtlasException {
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        for (AAEdge<V,E> edge : instanceVertex.getEdges(AADirection.OUT, relationshipLabel)) {
            if (edgeId.equals(String.valueOf(edge.getId()))) {
                AAVertex structInstanceVertex = edge.getVertex(AADirection.IN);
                LOG.debug("mapping vertex {} to struct {}", structInstanceVertex, attributeName);

                if (structInstanceVertex != null) {
                    LOG.debug("Found struct instance vertex {}, mapping to instance {} ", structInstanceVertex,
                        elemType.getName());
                    StructType structType = typeSystem.getDataType(StructType.class, elemType.getName());
                    ITypedStruct structInstance = structType.createInstance();
                    mapVertexToInstance(structInstanceVertex, structInstance, structType.fieldMapping().fields);
                    return structInstance;
                }

                break;
            }
        }

        return null;
    }

    private Object mapVertexToClassReference(AAVertex<V,E> instanceVertex, AttributeInfo attributeInfo,
        String relationshipLabel, IDataType dataType, String edgeId) throws AtlasException {
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        for (AAEdge<V,E> edge : instanceVertex.getEdges(AADirection.OUT, relationshipLabel)) {
            if (edgeId.equals(String.valueOf(edge.getId()))) {
                final AAVertex<V,E> referenceVertex = edge.getVertex(AADirection.IN);
                if (referenceVertex != null) {
                    final String guid = referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                    LOG.debug("Found vertex {} for label {} with guid {}", referenceVertex, relationshipLabel,
                        guid);
                    if (attributeInfo.isComposite) {
                        //Also, when you retrieve a type's instance, you get the complete object graph of the composites
                        LOG.debug("Found composite, mapping vertex to instance");
                        return mapGraphToTypedInstance(guid, referenceVertex);
                    } else {
                        Id referenceId =
                            new Id(guid, referenceVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                                dataType.getName());
                        LOG.debug("Found non-composite, adding id {} ", referenceId);
                        return referenceId;
                    }
                }

                break;
            }
        }

        return null;
    }

    private void mapVertexToStructInstance(AAVertex<V,E> instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("mapping vertex {} to struct {}", instanceVertex, attributeInfo.name);
        StructType structType = typeSystem.getDataType(StructType.class, attributeInfo.dataType().getName());
        ITypedStruct structInstance = null;

        String relationshipLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        final Iterable<AAEdge<V,E>> edges = instanceVertex.getEdges(AADirection.OUT, relationshipLabel);
        if (edges.iterator().hasNext()) {
            structInstance = structType.createInstance();
            typedInstance.set(attributeInfo.name, structInstance);
        }

        for (AAEdge<V,E> edge : edges) {
            final AAVertex structInstanceVertex = edge.getVertex(AADirection.IN);
            if (structInstanceVertex != null) {
                LOG.debug("Found struct instance vertex {}, mapping to instance {} ", structInstanceVertex,
                    structInstance.getTypeName());
                mapVertexToInstance(structInstanceVertex, structInstance, structType.fieldMapping().fields);
                break;
            }
        }
    }

    private void mapVertexToTraitInstance(AAVertex<V,E> instanceVertex, ITypedReferenceableInstance typedInstance,
        String traitName, TraitType traitType) throws AtlasException {
        ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

        mapVertexToTraitInstance(instanceVertex, typedInstance.getTypeName(), traitName, traitType, traitInstance);
    }

    private void mapVertexToTraitInstance(AAVertex<V,E> instanceVertex, String typedInstanceTypeName, String traitName,
        TraitType traitType, ITypedStruct traitInstance) throws AtlasException {
        String relationshipLabel = GraphHelper.getTraitLabel(typedInstanceTypeName, traitName);
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        for (AAEdge<V,E> edge : instanceVertex.getEdges(AADirection.OUT, relationshipLabel)) {
            final AAVertex<V,E> traitInstanceVertex = edge.getVertex(AADirection.IN);
            if (traitInstanceVertex != null) {
                LOG.debug("Found trait instance vertex {}, mapping to instance {} ", traitInstanceVertex,
                    traitInstance.getTypeName());
                mapVertexToInstance(traitInstanceVertex, traitInstance, traitType.fieldMapping().fields);
                break;
            }
        }
    }

    private void mapVertexToPrimitive(AAVertex<V,E> instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("Adding primitive {} from vertex {}", attributeInfo, instanceVertex);
        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        if (instanceVertex.getProperty(vertexPropertyName) == null) {
            return;
        }

        if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
            typedInstance.setString(attributeInfo.name, instanceVertex.<String>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
            typedInstance.setShort(attributeInfo.name, instanceVertex.<Short>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
            typedInstance.setInt(attributeInfo.name, instanceVertex.<Integer>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
            typedInstance.setBigInt(attributeInfo.name, instanceVertex.<BigInteger>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
            typedInstance.setBoolean(attributeInfo.name, instanceVertex.<Boolean>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
            typedInstance.setByte(attributeInfo.name, instanceVertex.<Byte>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
            typedInstance.setLong(attributeInfo.name, instanceVertex.<Long>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
            typedInstance.setFloat(attributeInfo.name, instanceVertex.<Float>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
            typedInstance.setDouble(attributeInfo.name, instanceVertex.<Double>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            typedInstance
                .setBigDecimal(attributeInfo.name, instanceVertex.<BigDecimal>getProperty(vertexPropertyName));
        } else if (attributeInfo.dataType() == DataTypes.DATE_TYPE) {
            final Long dateVal = instanceVertex.<Long>getProperty(vertexPropertyName);
            typedInstance.setDate(attributeInfo.name, new Date(dateVal));
        }
    }

    public ITypedInstance getReferredEntity(String edgeId, IDataType<?> referredType) throws AtlasException {
        final AAEdge<V,E> edge = graph.getEdge(edgeId);
        if (edge != null) {
            final AAVertex<V,E> referredVertex = edge.getVertex(AADirection.IN);
            if (referredVertex != null) {
                switch (referredType.getTypeCategory()) {
                case STRUCT:
                    LOG.debug("Found struct instance vertex {}, mapping to instance {} ", referredVertex,
                        referredType.getName());
                    StructType structType = (StructType) referredType;
                    ITypedStruct instance = structType.createInstance();
                    Map<String, AttributeInfo> fields = structType.fieldMapping().fields;
                    mapVertexToInstance(referredVertex, instance, fields);
                    return instance;
                case CLASS:
                    //TODO isComposite handling for class loads
                    final String guid = referredVertex.getProperty(Constants.GUID_PROPERTY_KEY);
                    Id referenceId =
                        new Id(guid, referredVertex.<Integer>getProperty(Constants.VERSION_PROPERTY_KEY),
                            referredType.getName());
                    return referenceId;
                default:
                    throw new UnsupportedOperationException("Loading " + referredType.getTypeCategory() + " is not supported");
                }
            }
        }
        return null;
    }
}

