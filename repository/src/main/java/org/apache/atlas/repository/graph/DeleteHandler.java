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

package org.apache.atlas.repository.graph;

import java.util.ArrayList;
import java.util.List;

import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.Constants;
import org.apache.atlas.repository.graphdb.AtlasEdge;
import org.apache.atlas.repository.graphdb.AtlasEdgeDirection;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.typesystem.exception.NullRequiredAttributeException;
import org.apache.atlas.typesystem.types.AttributeInfo;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.FieldMapping;
import org.apache.atlas.typesystem.types.HierarchicalType;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.StructType;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.atlas.repository.graph.GraphHelper.EDGE_LABEL_PREFIX;
import static org.apache.atlas.repository.graph.GraphHelper.string;

public abstract class DeleteHandler {
    public static final Logger LOG = LoggerFactory.getLogger(DeleteHandler.class);

    protected static final GraphHelper graphHelper = GraphHelper.getInstance();

    protected TypeSystem typeSystem;
    private boolean shouldUpdateReverseAttribute;
    private boolean softDelete;

    public DeleteHandler(TypeSystem typeSystem, boolean shouldUpdateReverseAttribute, boolean softDelete) {
        this.typeSystem = typeSystem;
        this.shouldUpdateReverseAttribute = shouldUpdateReverseAttribute;
        this.softDelete = softDelete;
    }

    /**
     * Deletes the entity vertex - deletes the traits and all the references
     * @param instanceVertex
     * @throws AtlasException
     */
    public void deleteEntity(DeleteContext deleteContext, AtlasVertex<?,?> instanceVertex) throws AtlasException {
        RequestContext requestContext = RequestContext.get();
        String guid = GraphHelper.getIdFromVertex(instanceVertex);

        if (deleteContext.isProcessedOrDeleted(instanceVertex)) {
            LOG.debug("Skipping deleting {} as its already deleted", guid);
            return;
        }
        String typeName = GraphHelper.getTypeName(instanceVertex);
        requestContext.recordEntityDelete(guid, typeName);
        deleteContext.addProcessedVertex(instanceVertex);

        deleteAllTraits(deleteContext, instanceVertex);

        deleteTypeVertex(deleteContext, instanceVertex, false);
    }

    protected abstract void deleteEdge(DeleteContext deleteContext, AtlasEdge<?,?> edge, boolean force) throws AtlasException;

    /**
     * Deletes a type vertex - can be entity(class type) or just vertex(struct/trait type)
     * @param instanceVertex
     * @param typeCategory
     * @throws AtlasException
     */
    protected void deleteTypeVertex(DeleteContext deleteContext, AtlasVertex<?,?> instanceVertex, DataTypes.TypeCategory typeCategory, boolean force) throws AtlasException {
        switch (typeCategory) {
        case STRUCT:
        case TRAIT:
            deleteTypeVertex(deleteContext, instanceVertex, force);
            break;

        case CLASS:
            deleteEntity(deleteContext, instanceVertex);
            break;

        default:
            throw new IllegalStateException("Type category " + typeCategory + " not handled");
        }
    }

    /**
     * Deleting any type vertex. Goes over the complex attributes and removes the references
     * @param instanceVertex
     * @throws AtlasException
     */
    protected <V,E> void deleteTypeVertex(DeleteContext deleteContext, AtlasVertex<V,E> instanceVertex, boolean force) throws AtlasException {
        LOG.debug("Deleting {}", string(instanceVertex));
        String typeName = GraphHelper.getTypeName(instanceVertex);
        IDataType type = typeSystem.getDataType(IDataType.class, typeName);
        FieldMapping fieldMapping = getFieldMapping(type);

        for (AttributeInfo attributeInfo : fieldMapping.fields.values()) {
            LOG.debug("Deleting attribute {} for {}", attributeInfo.name, string(instanceVertex));
            String edgeLabel = GraphHelper.getEdgeLabel(type, attributeInfo);

            switch (attributeInfo.dataType().getTypeCategory()) {
            case CLASS:
                //If its class attribute, delete the reference
                deleteEdgeReference(deleteContext, instanceVertex, edgeLabel, DataTypes.TypeCategory.CLASS, attributeInfo.isComposite);
                break;

            case STRUCT:
                //If its struct attribute, delete the reference
                deleteEdgeReference(deleteContext, instanceVertex, edgeLabel, DataTypes.TypeCategory.STRUCT, false);
                break;

            case ARRAY:
                //For array attribute, if the element is struct/class, delete all the references
                IDataType elementType = ((DataTypes.ArrayType) attributeInfo.dataType()).getElemType();
                DataTypes.TypeCategory elementTypeCategory = elementType.getTypeCategory();
                if (elementTypeCategory == DataTypes.TypeCategory.STRUCT ||
                        elementTypeCategory == DataTypes.TypeCategory.CLASS) {
                    Iterable<AtlasEdge<V,E>> edges = GraphHelper.getOutGoingEdgesByLabel(instanceVertex, edgeLabel);
                    if (edges != null) {
                        for(AtlasEdge<?,?> edge : edges) {
                            deleteEdgeReference(deleteContext, edge, elementType.getTypeCategory(), attributeInfo.isComposite, false);
                        }
                    }
                }
                break;

            case MAP:
                //For map attribute, if the value type is struct/class, delete all the references
                DataTypes.MapType mapType = (DataTypes.MapType) attributeInfo.dataType();
                DataTypes.TypeCategory valueTypeCategory = mapType.getValueType().getTypeCategory();
                String propertyName = GraphHelper.getQualifiedFieldName(type, attributeInfo.name);

                if (valueTypeCategory == DataTypes.TypeCategory.STRUCT ||
                        valueTypeCategory == DataTypes.TypeCategory.CLASS) {
                    List<String> keys = deleteContext.getListProperty(instanceVertex, propertyName);
                    if (keys != null) {
                        for (String key : keys) {
                            String mapEdgeLabel = GraphHelper.getQualifiedNameForMapKey(edgeLabel, key);
                            deleteEdgeReference(deleteContext, instanceVertex, mapEdgeLabel, valueTypeCategory, attributeInfo.isComposite);
                        }
                    }
                }
            }
        }

        deleteVertex(deleteContext, instanceVertex, force);
    }

    /**
     * Force delete is used to remove struct/trait in case of entity updates
     * @param edge
     * @param typeCategory
     * @param isComposite
     * @param forceDeleteStructTrait
     * @return returns true if the edge reference is hard deleted
     * @throws AtlasException
     */
    public boolean deleteEdgeReference(DeleteContext deleteContext, AtlasEdge<?,?> edge, DataTypes.TypeCategory typeCategory, boolean isComposite,
                                 boolean forceDeleteStructTrait) throws AtlasException {
        if(! deleteContext.isActive(edge)) {
            LOG.debug("Skipping deleted edge {}", string(edge));
            //edge has already been deleted, soft delete processing is not necessary.  It would
            //have been done during the actual delete.
            return true;
        }
        else {
        LOG.debug("Deleting {}", string(edge));
        }

        boolean forceDelete =
                (typeCategory == DataTypes.TypeCategory.STRUCT || typeCategory == DataTypes.TypeCategory.TRAIT)
                        ? forceDeleteStructTrait : false;
        if (typeCategory == DataTypes.TypeCategory.STRUCT || typeCategory == DataTypes.TypeCategory.TRAIT
                || (typeCategory == DataTypes.TypeCategory.CLASS && isComposite)) {
            //If the vertex is of type struct/trait, delete the edge and then the reference vertex as the vertex is not shared by any other entities.
            //If the vertex is of type class, and its composite attribute, this reference vertex' lifecycle is controlled
            //through this delete, hence delete the edge and the reference vertex.
            AtlasVertex<?,?> vertexForDelete = edge.getInVertex();

            //If deleting the edge and then the in vertex, reverse attribute shouldn't be updated
            deleteEdge(deleteContext, edge, false, forceDelete);
            deleteTypeVertex(deleteContext, vertexForDelete, typeCategory, forceDelete);
        } else {
            //If the vertex is of type class, and its not a composite attributes, the reference vertex' lifecycle is not controlled
            //through this delete. Hence just remove the reference edge. Leave the reference vertex as is

            //If deleting just the edge, reverse attribute should be updated for any references
            //For example, for the department type system, if the person's manager edge is deleted, subordinates of manager should be updated
            deleteEdge(deleteContext, edge, true, false);
        }
        return !softDelete || forceDelete;
    }

    public void deleteEdgeReference(DeleteContext deleteContext, AtlasVertex<?,?> instanceVertex, String edgeLabel, DataTypes.TypeCategory typeCategory,
                                    boolean isComposite) throws AtlasException {
        AtlasEdge<?,?> edge = GraphHelper.getEdgeForLabel(instanceVertex, edgeLabel);
        if (GraphHelper.elementExists(edge)) {
            deleteEdgeReference(deleteContext, edge, typeCategory, isComposite, false);
        }
    }

    protected void deleteEdge(DeleteContext deleteContext, AtlasEdge<?,?> edge, boolean updateReverseAttribute, boolean force) throws AtlasException {
        //update reverse attribute
        if (updateReverseAttribute) {
            AttributeInfo attributeInfo = getAttributeForEdge(edge.getLabel());
            if (attributeInfo.reverseAttributeName != null) {
                deleteEdgeBetweenVertices(deleteContext, edge.getInVertex(), edge.getOutVertex(),
                        attributeInfo.reverseAttributeName);
            }
        }

        deleteEdge(deleteContext, edge, force);
    }

    protected <V,E> void deleteVertex(DeleteContext deleteContext, AtlasVertex<V,E> instanceVertex, boolean force) throws AtlasException {
        //Update external references(incoming edges) to this vertex
        LOG.debug("Setting the external references to {} to null(removing edges)", string(instanceVertex));
        Iterable<AtlasEdge<V,E>> edges = instanceVertex.getEdges(AtlasEdgeDirection.IN);

        for(AtlasEdge<V,E> edge : edges) {

            if (deleteContext.isActive(edge)) {
                //Delete only the active edge references
                AttributeInfo attribute = getAttributeForEdge(edge.getLabel());
                //TODO use delete edge instead??
                deleteEdgeBetweenVertices(deleteContext, edge.getOutVertex(), edge.getInVertex(), attribute.name);
            }
        }
        _deleteVertex(deleteContext, instanceVertex, force);
    }

    protected abstract void _deleteVertex(DeleteContext deleteContext, AtlasVertex<?,?> instanceVertex, boolean force);

    /**
     * Deletes the edge between outvertex and inVertex. The edge is for attribute attributeName of outVertex
     * @param outVertex
     * @param inVertex
     * @param attributeName
     * @throws AtlasException
     */
    protected void deleteEdgeBetweenVertices(DeleteContext deleteContext, AtlasVertex outVertex, AtlasVertex inVertex, String attributeName) throws AtlasException {
        LOG.debug("Removing edge from {} to {} with attribute name {}", string(outVertex), string(inVertex),
                attributeName);
        String typeName = GraphHelper.getTypeName(outVertex);
        String outId = GraphHelper.getIdFromVertex(outVertex);

        if (deleteContext.isProcessedOrDeleted(outVertex)) {
            //If the reference vertex is marked for deletion, skip updating the reference
            return;
        }

        IDataType type = typeSystem.getDataType(IDataType.class, typeName);
        AttributeInfo attributeInfo = getFieldMapping(type).fields.get(attributeName);
        String propertyName = GraphHelper.getQualifiedFieldName(type, attributeName);
        String edgeLabel = EDGE_LABEL_PREFIX + propertyName;
        AtlasEdge edge = null;

        switch (attributeInfo.dataType().getTypeCategory()) {
        case CLASS:
            //If its class attribute, its the only edge between two vertices
            if (attributeInfo.multiplicity.nullAllowed()) {
                edge = GraphHelper.getEdgeForLabel(outVertex, edgeLabel);
                if (shouldUpdateReverseAttribute) {
                    deleteContext.setProperty(outVertex, propertyName, null);
                }
            } else {
                // Cannot unset a required attribute.
                throw new NullRequiredAttributeException("Cannot unset required attribute " + GraphHelper.getQualifiedFieldName(type, attributeName) +
                    " on " + string(outVertex) + " edge = " + edgeLabel);
            }
            break;

        case ARRAY:
            //If its array attribute, find the right edge between the two vertices and update array property
            List<String> elements = deleteContext.getListProperty(outVertex, propertyName);
            if (elements != null) {
                elements = new ArrayList<>(elements);   //Make a copy, else list.remove reflects on titan.getProperty()
                for (String elementEdgeId : elements) {
                    AtlasEdge elementEdge = graphHelper.getEdgeByEdgeId(outVertex, edgeLabel, elementEdgeId);
                    if (! GraphHelper.elementExists(elementEdge)) {
                        continue;
                    }

                    AtlasVertex<?,?> elementVertex = elementEdge.getInVertex();
                    if (elementVertex.getId().toString().equals(inVertex.getId().toString())) {
                        edge = elementEdge;

                        //TODO element.size includes deleted items as well. should exclude
                        if (!attributeInfo.multiplicity.nullAllowed()
                                && elements.size() <= attributeInfo.multiplicity.lower) {
                            // Deleting this edge would violate the attribute's lower bound.
                            throw new NullRequiredAttributeException(
                                "Cannot remove array element from required attribute " +
                                            GraphHelper.getQualifiedFieldName(type, attributeName) + " on "
                                            + string(outVertex) + " " + string(elementEdge));
                        }

                        if (shouldUpdateReverseAttribute) {
                            //if composite attribute, remove the reference as well. else, just remove the edge
                            //for example, when table is deleted, process still references the table
                            //but when column is deleted, table will not reference the deleted column
                            LOG.debug("Removing edge {} from the array attribute {}", string(elementEdge),
                                    attributeName);
                            elements.remove(elementEdge.getId().toString());
                            deleteContext.setProperty(outVertex, propertyName, elements);
                            break;

                        }
                    }
                }
            }
            break;

        case MAP:
            //If its map attribute, find the right edge between two vertices and update map property
            List<String> keys = deleteContext.getListProperty(outVertex, propertyName);
            if (keys != null) {
                keys = new ArrayList<>(keys);   //Make a copy, else list.remove reflects on titan.getProperty()
                for (String key : keys) {
                    String keyPropertyName = GraphHelper.getQualifiedNameForMapKey(propertyName, key);
                    String mapEdgeId = deleteContext.getProperty(outVertex, keyPropertyName, String.class);
                    AtlasEdge<?,?> mapEdge = graphHelper.getEdgeByEdgeId(outVertex, keyPropertyName, mapEdgeId);
                    AtlasVertex<?,?> mapVertex = mapEdge.getInVertex();
                    if (mapVertex.getId().toString().equals(inVertex.getId().toString())) {
                        //TODO keys.size includes deleted items as well. should exclude
                        if (attributeInfo.multiplicity.nullAllowed() || keys.size() > attributeInfo.multiplicity.lower) {
                            edge = mapEdge;
                        }
                        else {
                            // Deleting this entry would violate the attribute's lower bound.
                            throw new NullRequiredAttributeException(
                                "Cannot remove map entry " + keyPropertyName + " from required attribute " +
                                    GraphHelper.getQualifiedFieldName(type, attributeName) + " on " + string(outVertex) + " " + string(mapEdge));
                        }

                        if (shouldUpdateReverseAttribute) {
                            //remove this key
                            LOG.debug("Removing edge {}, key {} from the map attribute {}", string(mapEdge), key,
                                    attributeName);
                            keys.remove(key);
                            deleteContext.setProperty(outVertex, propertyName, keys);
                            deleteContext.setProperty(outVertex, keyPropertyName, null);
                        }
                        break;
                    }
                }
            }
            break;

        case STRUCT:
        case TRAIT:
            break;

        default:
            throw new IllegalStateException("There can't be an edge from " + string(outVertex) + " to "
                    + string(inVertex) + " with attribute name " + attributeName + " which is not class/array/map attribute");
        }

        if (GraphHelper.elementExists(edge)) {
            deleteEdge(deleteContext, edge, false);
            RequestContext requestContext = RequestContext.get();
            deleteContext.setProperty(outVertex, Constants.MODIFICATION_TIMESTAMP_PROPERTY_KEY,
                    requestContext.getRequestTime());
            requestContext.recordEntityUpdate(outId);
        }
    }

    protected AttributeInfo getAttributeForEdge(String edgLabel) throws AtlasException {
        AtlasEdgeLabel atlasEdgeLabel = new AtlasEdgeLabel(edgLabel);
        IDataType referenceType = typeSystem.getDataType(IDataType.class, atlasEdgeLabel.getTypeName());
        return getFieldMapping(referenceType).fields.get(atlasEdgeLabel.getAttributeName());
    }

    protected FieldMapping getFieldMapping(IDataType type) {
        switch (type.getTypeCategory()) {
        case CLASS:
        case TRAIT:
            return ((HierarchicalType)type).fieldMapping();

        case STRUCT:
            return ((StructType)type).fieldMapping();

        default:
            throw new IllegalStateException("Type " + type + " doesn't have any fields!");
        }
    }

    /**
     * Delete all traits from the specified vertex.
     * @param instanceVertex
     * @throws AtlasException
     */
    private void deleteAllTraits(DeleteContext deleteContext, AtlasVertex<?,?> instanceVertex) throws AtlasException {
        List<String> traitNames = GraphHelper.getTraitNames(instanceVertex);
        LOG.debug("Deleting traits {} for {}", traitNames, string(instanceVertex));
        String typeName = GraphHelper.getTypeName(instanceVertex);

        for (String traitNameToBeDeleted : traitNames) {
            String relationshipLabel = GraphHelper.getTraitLabel(typeName, traitNameToBeDeleted);
            deleteEdgeReference(deleteContext, instanceVertex, relationshipLabel, DataTypes.TypeCategory.TRAIT, false);
        }
    }
}
