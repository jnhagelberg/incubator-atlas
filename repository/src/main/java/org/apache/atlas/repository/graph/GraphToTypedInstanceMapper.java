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

import com.google.inject.Singleton;


import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
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
import java.util.List;
import java.util.Map;

import static org.apache.atlas.repository.graph.GraphHelper.string;

@Singleton
public final class GraphToTypedInstanceMapper {

    private static final Logger LOG = LoggerFactory.getLogger(GraphToTypedInstanceMapper.class);
    private static TypeSystem typeSystem = TypeSystem.getInstance();
    private static final GraphHelper graphHelper = GraphHelper.getInstance();

    private final AtlasGraph<?,?> graph;


    public GraphToTypedInstanceMapper(AtlasGraph<?,?> graph) {
        this.graph = graph;
    }

    public ITypedReferenceableInstance mapGraphToTypedInstance(String guid, AtlasVertex<?,?> instanceVertex)
        throws AtlasException {

        LOG.debug("Mapping graph root vertex {} to typed instance for guid {}", instanceVertex, guid);
        String typeName = instanceVertex.getProperty(Constants.ENTITY_TYPE_PROPERTY_KEY, String.class);
        List<String> traits = GraphHelper.getTraitNames(instanceVertex);
        String state = GraphHelper.getStateAsString(instanceVertex);

	Id id = new Id(guid, instanceVertex.getProperty(Constants.VERSION_PROPERTY_KEY, Integer.class), typeName, state);        LOG.debug("Created id {} for instance type {}", id, typeName);
	LOG.debug("Created id {} for instance type {}", id, typeName);
	
        ClassType classType = typeSystem.getDataType(ClassType.class, typeName);
        ITypedReferenceableInstance typedInstance =
            classType.createInstance(id, traits.toArray(new String[traits.size()]));

        mapVertexToInstance(instanceVertex, typedInstance, classType.fieldMapping().fields);
        mapVertexToInstanceTraits(instanceVertex, typedInstance, traits);

        return typedInstance;
    }

    private void mapVertexToInstanceTraits(AtlasVertex instanceVertex, ITypedReferenceableInstance typedInstance,
        List<String> traits) throws AtlasException {
        for (String traitName : traits) {
            LOG.debug("mapping trait {} to instance", traitName);
            TraitType traitType = typeSystem.getDataType(TraitType.class, traitName);
            mapVertexToTraitInstance(instanceVertex, typedInstance, traitName, traitType);
        }
    }

    public void mapVertexToInstance(AtlasVertex instanceVertex, ITypedInstance typedInstance,
        Map<String, AttributeInfo> fields) throws AtlasException {

        LOG.debug("Mapping vertex {} to instance {} for fields", instanceVertex, typedInstance.getTypeName(),
            fields);
        for (AttributeInfo attributeInfo : fields.values()) {
            mapVertexToAttribute(instanceVertex, typedInstance, attributeInfo);
        }
    }

    private void mapVertexToAttribute(AtlasVertex instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("Mapping attributeInfo {}", attributeInfo.name);
        final IDataType dataType = attributeInfo.dataType();
        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        String relationshipLabel = GraphHelper.getEdgeLabel(typedInstance, attributeInfo);

        switch (dataType.getTypeCategory()) {
        case PRIMITIVE:
            mapVertexToPrimitive(instanceVertex, typedInstance, attributeInfo);
            break;  // add only if vertex has this attribute

        case ENUM:
            if (instanceVertex.getProperty(vertexPropertyName, Object.class) == null) {
                return;
            }

            typedInstance.set(attributeInfo.name,
                dataType.convert(instanceVertex.getProperty(vertexPropertyName, String.class),
                    Multiplicity.REQUIRED));
            break;

        case ARRAY:
            mapVertexToArrayInstance(instanceVertex, typedInstance, attributeInfo, vertexPropertyName);
            break;

        case MAP:
            mapVertexToMapInstance(instanceVertex, typedInstance, attributeInfo, vertexPropertyName);
            break;

        case STRUCT:
            ITypedStruct structInstance = mapVertexToStructInstance(instanceVertex,
                    (StructType) attributeInfo.dataType(), relationshipLabel, null);
            typedInstance.set(attributeInfo.name, structInstance);
            break;

        case TRAIT:
            // do NOTHING - handled in class
            break;

        case CLASS:
            AtlasEdge nullEdge = null;
            Object idOrInstance = mapVertexToClassReference(instanceVertex, attributeInfo, relationshipLabel,
                attributeInfo.dataType(), nullEdge);
            if (idOrInstance != null) {
                typedInstance.set(attributeInfo.name, idOrInstance);
            }
            break;

        default:
            break;
        }
    }

    private <V,E> Object mapVertexToClassReference(AtlasVertex<V,E> instanceVertex, AttributeInfo attributeInfo,
            String relationshipLabel, IDataType dataType, String edgeId) throws AtlasException {
        
        AtlasEdge<V,E> edge = graphHelper.getEdgeByEdgeId(instanceVertex, relationshipLabel, edgeId);
        return mapVertexToClassReference(instanceVertex, attributeInfo, relationshipLabel, dataType, edge);
        
    }
    private <V,E> Object mapVertexToClassReference(AtlasVertex<V,E> instanceVertex, AttributeInfo attributeInfo,
        String relationshipLabel, IDataType dataType, AtlasEdge<V,E> theEdge) throws AtlasException {
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);

 		AtlasEdge<V,E> edge = null;
        if (theEdge == null) {
            edge = GraphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);
        } else {
            edge = theEdge;
        }

        if (GraphHelper.elementExists(edge)) {
            final AtlasVertex<V,E> referenceVertex = edge.getInVertex();
            final String guid = referenceVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
            LOG.debug("Found vertex {} for label {} with guid {}", referenceVertex, relationshipLabel, guid);
            if (attributeInfo.isComposite) {
                //Also, when you retrieve a type's instance, you get the complete object graph of the composites
                LOG.debug("Found composite, mapping vertex to instance");
                return mapGraphToTypedInstance(guid, referenceVertex);
            } else {
                String state = GraphHelper.getStateAsString(referenceVertex);
                Id referenceId =
                        new Id(guid, referenceVertex.getProperty(Constants.VERSION_PROPERTY_KEY, Integer.class),
                                dataType.getName(), state);
                LOG.debug("Found non-composite, adding id {} ", referenceId);
                return referenceId;
            }
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private void mapVertexToArrayInstance(AtlasVertex<?,?> instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo, String propertyName) throws AtlasException {
        LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
        List list = instanceVertex.getListProperty(propertyName);
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

    private Object mapVertexToCollectionEntry(AtlasVertex instanceVertex, AttributeInfo attributeInfo,
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
            return mapVertexToStructInstance(instanceVertex, (StructType) elementType, edgeLabel, (String) value);

        case CLASS:
            return mapVertexToClassReference(instanceVertex, attributeInfo, edgeLabel, elementType, (String) value);

        default:
            break;
        }

        throw new IllegalArgumentException();
    }

    @SuppressWarnings("unchecked")
    private void mapVertexToMapInstance(AtlasVertex<?,?> instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo, final String propertyName) throws AtlasException {
        LOG.debug("mapping vertex {} to array {}", instanceVertex, attributeInfo.name);
        List<String> keys = instanceVertex.getListProperty(propertyName);
        if (keys == null || keys.size() == 0) {
            return;
        }
        DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
        final IDataType valueType = mapType.getValueType();

        HashMap values = new HashMap();
        for (String key : keys) {
            final String keyPropertyName = propertyName + Constants.SEPARATOR + key;
            final String edgeLabel = GraphHelper.EDGE_LABEL_PREFIX + keyPropertyName;
            final Object keyValue = instanceVertex.getProperty(keyPropertyName, Object.class);
            Object mapValue = mapVertexToCollectionEntry(instanceVertex, attributeInfo, valueType, keyValue, edgeLabel);
            if (mapValue != null) {
                values.put(key, mapValue);
            }
        }

        if (!values.isEmpty()) {
            typedInstance.set(attributeInfo.name, values);
        }
    }


    private <V,E> ITypedStruct mapVertexToStructInstance(AtlasVertex<V,E> instanceVertex, StructType structType,
                                                   String relationshipLabel, String edgeId) throws AtlasException {
        LOG.debug("mapping {} to struct {}", string(instanceVertex), relationshipLabel);
        ITypedStruct structInstance = null;

        AtlasEdge<V,E> edge;
        if (edgeId == null) {
            edge = GraphHelper.getEdgeForLabel(instanceVertex, relationshipLabel);
        } else {
            edge = graphHelper.getEdgeByEdgeId(instanceVertex, relationshipLabel, edgeId);
        }

        if (GraphHelper.elementExists(edge)) {
            structInstance = structType.createInstance();
            AtlasVertex<V,E> structInstanceVertex = edge.getInVertex();
            LOG.debug("Found struct instance {}, mapping to instance {} ", string(structInstanceVertex),
                    structInstance.getTypeName());
            mapVertexToInstance(structInstanceVertex, structInstance, structType.fieldMapping().fields);

        }
        return structInstance;
    }

    private void mapVertexToTraitInstance(AtlasVertex<?,?> instanceVertex, ITypedReferenceableInstance typedInstance,
        String traitName, TraitType traitType) throws AtlasException {
        ITypedStruct traitInstance = (ITypedStruct) typedInstance.getTrait(traitName);

        mapVertexToTraitInstance(instanceVertex, typedInstance.getTypeName(), traitName, traitType, traitInstance);
    }

    private void mapVertexToTraitInstance(AtlasVertex<?,?> instanceVertex, String typedInstanceTypeName, String traitName,
        TraitType traitType, ITypedStruct traitInstance) throws AtlasException {
        String relationshipLabel = GraphHelper.getTraitLabel(typedInstanceTypeName, traitName);
        LOG.debug("Finding edge for {} -> label {} ", instanceVertex, relationshipLabel);
        for (AtlasEdge<?,?> edge : instanceVertex.getEdges(AtlasEdgeDirection.OUT, relationshipLabel)) {
            final AtlasVertex<?,?> traitInstanceVertex = edge.getInVertex();
            if (traitInstanceVertex != null) {
                LOG.debug("Found trait instance vertex {}, mapping to instance {} ", traitInstanceVertex,
                    traitInstance.getTypeName());
                mapVertexToInstance(traitInstanceVertex, traitInstance, traitType.fieldMapping().fields);
                break;
            }
        }
    }

    private void mapVertexToPrimitive(AtlasVertex<?,?> instanceVertex, ITypedInstance typedInstance,
        AttributeInfo attributeInfo) throws AtlasException {
        LOG.debug("Adding primitive {} from vertex {}", attributeInfo, instanceVertex);
        final String vertexPropertyName = GraphHelper.getQualifiedFieldName(typedInstance, attributeInfo);
        if (instanceVertex.getProperty(vertexPropertyName, Object.class) == null) {
            return;
        }

        if (attributeInfo.dataType() == DataTypes.STRING_TYPE) {
            typedInstance.setString(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, String.class));
        } else if (attributeInfo.dataType() == DataTypes.SHORT_TYPE) {
            typedInstance.setShort(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, Short.class));
        } else if (attributeInfo.dataType() == DataTypes.INT_TYPE) {
            typedInstance.setInt(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, Integer.class));
        } else if (attributeInfo.dataType() == DataTypes.BIGINTEGER_TYPE) {
            typedInstance.setBigInt(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, BigInteger.class));
        } else if (attributeInfo.dataType() == DataTypes.BOOLEAN_TYPE) {
            typedInstance.setBoolean(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, Boolean.class));
        } else if (attributeInfo.dataType() == DataTypes.BYTE_TYPE) {
            typedInstance.setByte(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, Byte.class));
        } else if (attributeInfo.dataType() == DataTypes.LONG_TYPE) {
            typedInstance.setLong(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, Long.class));
        } else if (attributeInfo.dataType() == DataTypes.FLOAT_TYPE) {
            typedInstance.setFloat(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, Float.class));
        } else if (attributeInfo.dataType() == DataTypes.DOUBLE_TYPE) {
            typedInstance.setDouble(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, Double.class));
        } else if (attributeInfo.dataType() == DataTypes.BIGDECIMAL_TYPE) {
            typedInstance
                .setBigDecimal(attributeInfo.name, instanceVertex.getProperty(vertexPropertyName, BigDecimal.class));
        } else if (attributeInfo.dataType() == DataTypes.DATE_TYPE) {
            final Long dateVal = instanceVertex.getProperty(vertexPropertyName, Long.class);
            typedInstance.setDate(attributeInfo.name, new Date(dateVal));
        }
    }

    public ITypedInstance getReferredEntity(String edgeId, IDataType<?> referredType) throws AtlasException {
        final AtlasEdge<?,?> edge = graph.getEdge(edgeId);
        if (GraphHelper.elementExists(edge)) {
            final AtlasVertex<?,?> referredVertex = edge.getInVertex();
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
                    final String guid = referredVertex.getProperty(Constants.GUID_PROPERTY_KEY, String.class);
                    Id referenceId =
                        new Id(guid, referredVertex.getProperty(Constants.VERSION_PROPERTY_KEY, Integer.class),
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

