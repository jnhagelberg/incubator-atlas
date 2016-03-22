
package org.apache.atlas.repository.graph;

import org.apache.atlas.repository.graphdb.AAGraph;

public interface GraphProviderPlugin<V,E> {
    
    void initialize();
    AAGraph<V,E> createGraph();
    void cleanup();
    
}