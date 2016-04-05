
package org.apache.atlas.repository.graphdb.titan1.serializer;

import java.util.ArrayList;
import java.util.List;

import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.StringSerializer;

public class StringListSerializer implements AttributeSerializer<List<String>> {

    private StringSerializer stringSerializer_ = new StringSerializer();
    @Override
    public List<String> read(ScanBuffer buffer) {            
        int length = (int)VariableLong.readPositive(buffer);
        List<String> result = new ArrayList<String>(length);
        for(int i = 0; i < length; i++) {
            result.add(stringSerializer_.read(buffer));
        } 
        return result;
    }

    @Override
    public void write(WriteBuffer buffer, List<String> attributes) {
        VariableLong.writePositive(buffer, attributes.size());
        for(String attr : attributes) {
            stringSerializer_.write(buffer, attr);
        }
    }
    
}