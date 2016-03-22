
package org.apache.atlas.repository.graphdb;

import java.util.Iterator;

public interface AAIndexQuery<V,E> {

    Iterator<Result<V,E>> vertices();
    
    interface Result<V,E> {
        AAVertex<V,E> getVertex();
        double getScore();
        
    }

}
