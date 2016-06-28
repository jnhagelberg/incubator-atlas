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

package org.apache.atlas;

import org.aopalliance.intercept.MethodInterceptor;
import org.apache.atlas.discovery.DataSetLineageService;
import org.apache.atlas.discovery.DiscoveryService;
import org.apache.atlas.discovery.LineageService;
import org.apache.atlas.discovery.graph.GraphBackedDiscoveryService;
import org.apache.atlas.listener.EntityChangeListener;
import org.apache.atlas.listener.TypesChangeListener;
import org.apache.atlas.repository.ITenantRegisterationListener;
import org.apache.atlas.repository.MetadataRepository;
import org.apache.atlas.repository.audit.EntityAuditListener;
import org.apache.atlas.repository.audit.EntityAuditRepository;
import org.apache.atlas.repository.audit.HBaseBasedAuditRepository;
import org.apache.atlas.repository.graph.AtlasGraphProvider;
import org.apache.atlas.repository.graph.DeleteHandler;
import org.apache.atlas.repository.graph.GraphBackedMetadataRepository;
import org.apache.atlas.repository.graph.GraphBackedSearchIndexer;
import org.apache.atlas.repository.graph.GraphProvider;
import org.apache.atlas.repository.graph.SoftDeleteHandler;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.typestore.GraphBackedTypeStore;
import org.apache.atlas.repository.typestore.ITypeStore;
import org.apache.atlas.service.Service;
import org.apache.atlas.services.DefaultMetadataService;
import org.apache.atlas.services.IBootstrapTypesRegistrar;
import org.apache.atlas.services.MetadataService;
import org.apache.atlas.services.ReservedTypesRegistrar;
import org.apache.atlas.services.TenantRegisterationListener;
import org.apache.atlas.typesystem.types.TypeSystem;
import org.apache.atlas.typesystem.types.TypeSystemProvider;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matchers;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.throwingproviders.ThrowingProviderBinder;

/**
 * Guice module for Repository module.
 */
public class RepositoryMetadataModule extends com.google.inject.AbstractModule {


    @Override
    protected void configure() {
        // special wiring for Titan Graph
        ThrowingProviderBinder.create(binder()).bind(GraphProvider.class, AtlasGraph.class).to(AtlasGraphProvider.class)
                .asEagerSingleton();

        // allow for dynamic binding of the metadata repo & graph service
        // bind the MetadataRepositoryService interface to an implementation
        bind(MetadataRepository.class).to(GraphBackedMetadataRepository.class).asEagerSingleton();
       
        bind(TypeSystem.class).toProvider(TypeSystemProvider.class).in(Singleton.class);

        // bind the ITypeStore interface to an implementation
        bind(ITypeStore.class).to(GraphBackedTypeStore.class).asEagerSingleton();

        //make search indexer an eager singleton to force the index creation to happen
        //before we try to restore the typesystem
        Multibinder<TypesChangeListener> typesChangeListenerBinder =
                Multibinder.newSetBinder(binder(), TypesChangeListener.class);
        typesChangeListenerBinder.addBinding().to(GraphBackedSearchIndexer.class);

        // bind the MetadataService interface to an implementation
        bind(MetadataService.class).to(DefaultMetadataService.class).asEagerSingleton();

        bind(IBootstrapTypesRegistrar.class).to(ReservedTypesRegistrar.class);

        // bind the DiscoveryService interface to an implementation
        bind(DiscoveryService.class).to(GraphBackedDiscoveryService.class).asEagerSingleton();

        // bind the tenant registration listener interface to an implementation
        bind(ITenantRegisterationListener.class).to(TenantRegisterationListener.class).asEagerSingleton();
        
        bind(LineageService.class).to(DataSetLineageService.class).asEagerSingleton();

        bindAuditRepository(binder());

        bind(DeleteHandler.class).to(getDeleteHandlerImpl()).asEagerSingleton();

        //Add EntityAuditListener as EntityChangeListener
        Multibinder<EntityChangeListener> entityChangeListenerBinder =
                Multibinder.newSetBinder(binder(), EntityChangeListener.class);
        entityChangeListenerBinder.addBinding().to(EntityAuditListener.class);

        MethodInterceptor interceptor = new GraphTransactionInterceptor();
        requestInjection(interceptor);
        bindInterceptor(Matchers.any(), Matchers.annotatedWith(GraphTransaction.class), interceptor);
                
    }

    protected void bindAuditRepository(Binder binder) {

        Class<? extends EntityAuditRepository> auditRepoImpl = getAuditRepositoryImpl();

        //Map EntityAuditRepository interface to configured implementation
        binder.bind(EntityAuditRepository.class).to(auditRepoImpl).asEagerSingleton();

        if(Service.class.isAssignableFrom(auditRepoImpl)) {
            Class<? extends Service> auditRepoService = (Class<? extends Service>)auditRepoImpl;
            //if it's a service, make sure that it gets properly closed at shutdown
        Multibinder<Service> serviceBinder = Multibinder.newSetBinder(binder, Service.class);
            serviceBinder.addBinding().to(auditRepoService);
        }
    }


    private static final String AUDIT_REPOSITORY_IMPLEMENTATION_PROPERTY = "atlas.EntityAuditRepository.impl";

    private Class<? extends EntityAuditRepository> getAuditRepositoryImpl() {
        try {
            return ApplicationProperties.getClass(AUDIT_REPOSITORY_IMPLEMENTATION_PROPERTY,
                    HBaseBasedAuditRepository.class.getName(), EntityAuditRepository.class);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }

    private static final String DELETE_HANDLER_IMPLEMENTATION_PROPERTY = "atlas.DeleteHandler.impl";

    private Class<? extends DeleteHandler> getDeleteHandlerImpl() {
        try {
            return ApplicationProperties.getClass(DELETE_HANDLER_IMPLEMENTATION_PROPERTY,
                    SoftDeleteHandler.class.getName(), DeleteHandler.class);
        } catch (AtlasException e) {
            throw new RuntimeException(e);
        }
    }
}
