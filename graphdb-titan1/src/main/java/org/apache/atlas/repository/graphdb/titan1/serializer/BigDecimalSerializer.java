
package org.apache.atlas.repository.graphdb.titan1.serializer;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

public class BigDecimalSerializer implements AttributeSerializer<BigDecimal> {

    private BigIntegerSerializer bigIntegerDelegate_ = new BigIntegerSerializer();
    
    @Override
    public BigDecimal read(ScanBuffer buffer) {            
        BigInteger unscaledVal = bigIntegerDelegate_.read(buffer);
        int scale = buffer.getInt();
        return new BigDecimal(unscaledVal, scale);
    }

    @Override
    public void write(WriteBuffer buffer, BigDecimal attribute) {
        BigInteger unscaledVal = attribute.unscaledValue();
        int scale = attribute.scale();
        bigIntegerDelegate_.write(buffer, unscaledVal);
        buffer.putInt(scale);        
    }
    
}