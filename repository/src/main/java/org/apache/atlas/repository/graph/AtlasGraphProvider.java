
package org.apache.atlas.repository.graph;

import javax.inject.Singleton;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;
import org.apache.commons.configuration.Configuration;

import com.google.inject.Provides;


public class AtlasGraphProvider implements GraphProvider<AtlasGraph> {

    private static final String IMPL_PROPERTY = "atlas.graphdb.backend";
    private static volatile GraphDatabase<?,?> plugin_;
    private static volatile AtlasGraph<?,?> graph_;
        
    public static <V,E> AtlasGraph<V,E> getGraphInstance() {
        
        if(graph_ == null) {
            try {
                if(plugin_ == null) {
                    String implClassName = getPluginImplClass();
                    
                    Class implClass = Thread.currentThread().getContextClassLoader().loadClass(implClassName);
                    plugin_ = (GraphDatabase<V, E>)implClass.newInstance();
                    
            }
                graph_ = plugin_.getGraph();
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException("Error initializing graph database provider", e);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Error initializing graph database provider", e);
            } catch (InstantiationException e) {
                throw new RuntimeException("Error initializing graph database provider", e);
            } catch (AtlasException e) {
                throw new RuntimeException("Error initializing graph database provider", e);
            }
            
        }
        return (AtlasGraph<V,E>)graph_;
        
    }   

    private static String getPluginImplClass() throws AtlasException {
        
        String implClassName = System.getProperty(IMPL_PROPERTY);
        
        if(implClassName == null) {               
            Configuration config = ApplicationProperties.get();
            config.getString(IMPL_PROPERTY);
            implClassName = config.getString(IMPL_PROPERTY);            
        }
        
        if(implClassName == null) {
            throw new AtlasException("Could not initialize Atlas.  The required configuration property " + IMPL_PROPERTY + " was not found");
        }
        
        return implClassName;
    }
    
    @Override
    @Singleton
    @Provides
    public AtlasGraph<?,?> get() {
       return getGraphInstance();
    }
    
    public static void unloadGraph() {
        if(plugin_ != null) {
            plugin_.unloadGraph();
        }
        graph_ = null;
    }
    
}
