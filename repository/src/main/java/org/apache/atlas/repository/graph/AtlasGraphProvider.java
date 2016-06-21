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

import java.util.HashMap;
import java.util.Map;

import javax.inject.Singleton;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.atlas.RequestContext;
import org.apache.atlas.repository.Constants;
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
                Map<String, String> initParams = new HashMap <String, String> ();
                initParams.put(Constants.TENANT_ID, RequestContext.get().getTenantId());
                graphDb_.initialize(initParams);
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
