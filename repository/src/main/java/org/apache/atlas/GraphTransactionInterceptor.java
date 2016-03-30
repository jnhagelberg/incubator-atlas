/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.atlas;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.repository.graphdb.AAGraph;
import org.apache.atlas.repository.graphdb.GremlinVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.Inject;

public class GraphTransactionInterceptor implements MethodInterceptor {
    private static final Logger LOG = LoggerFactory.getLogger(GraphTransactionInterceptor.class);
    private AAGraph<?,?> graph;

    @Inject
    GraphProvider<AAGraph> graphProvider;

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {
        if (graph == null) {
            graph = (AAGraph<?,?>)graphProvider.get();
        }

        try {
            //force rollback to ensure this thread has a consistent view
            //of the graph
            if(graph.getSupportedGremlinVersion() == GremlinVersion.THREE) {
                graph.rollback();
            }
            Object response = invocation.proceed();
            graph.commit();
            LOG.debug("graph commit");
            return response;
        } catch (Throwable t) {
            graph.rollback();
            LOG.error("graph rollback due to exception ", t);
            throw t;
        }
    }
}
