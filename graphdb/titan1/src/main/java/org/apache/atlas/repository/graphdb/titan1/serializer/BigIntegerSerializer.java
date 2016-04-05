
package org.apache.atlas.repository.graphdb.titan1.serializer;

import java.math.BigInteger;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.ByteArraySerializer;

public class BigIntegerSerializer implements AttributeSerializer<BigInteger> {

    ByteArraySerializer delegate = new ByteArraySerializer();
    
    @Override
    public BigInteger read(ScanBuffer buffer) {            
        byte[] value = delegate.read(buffer);
        return new BigInteger(value);
    }

    @Override
    public void write(WriteBuffer buffer, BigInteger attribute) {
        byte[] value = attribute.toByteArray();
        delegate.write(buffer, value);
    }
    
}