
package org.apache.atlas.repository.graphdb.titan1.serializer;

import org.apache.atlas.typesystem.types.DataTypes.TypeCategory;

import com.thinkaurelius.titan.graphdb.database.serialize.attribute.EnumSerializer;

public class TypeCategorySerializer extends EnumSerializer<TypeCategory> {
    public TypeCategorySerializer() {
        super(TypeCategory.class);
    }       
}