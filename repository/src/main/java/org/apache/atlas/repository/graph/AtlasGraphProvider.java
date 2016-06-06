
package org.apache.atlas.repository.graph;

import javax.inject.Singleton;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.GraphDatabase;

import com.google.inject.Provides;


public class AtlasGraphProvider implements GraphProvider<AtlasGraph> {

    private static final String IMPL_PROPERTY = "atlas.graphdb.backend";
    private static final String DEFAULT_DATABASE_IMPL_CLASS = "org.apache.atlas.repository.graphdb.titan0.Titan0Database";
    private static volatile GraphDatabase<?,?> graphDb_;
    private static volatile AtlasGraph<?,?> graph_;

    public static <V,E> AtlasGraph<V,E> getGraphInstance() {

        if(graph_ == null) {
            try {
                if(graphDb_ == null) {
                    Class implClass = ApplicationProperties.getClass(IMPL_PROPERTY, DEFAULT_DATABASE_IMPL_CLASS, GraphDatabase.class);
                    graphDb_ = (GraphDatabase<V, E>)implClass.newInstance();
                }
                graph_ = graphDb_.getGraph();
            }
            catch (IllegalAccessException e) {
                throw new RuntimeException("Error initializing graph database", e);
            } catch (InstantiationException e) {
                throw new RuntimeException("Error initializing graph database", e);
            } catch (AtlasException e) {
                throw new RuntimeException("Error initializing graph database", e);
            }

        }
        return (AtlasGraph<V,E>)graph_;

    }

    @Override
    @Singleton
    @Provides
    public AtlasGraph<?,?> get() {
       return getGraphInstance();
    }

    public static void unloadGraph() {
        if(graphDb_ != null) {
            graphDb_.unloadGraph();
        }
        graph_ = null;
    }

}
