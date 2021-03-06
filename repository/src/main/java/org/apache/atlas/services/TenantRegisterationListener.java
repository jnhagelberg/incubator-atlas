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
package org.apache.atlas.services;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.atlas.AtlasException;
import org.apache.atlas.repository.ITenantRegisterationListener;

/**
 * This class is to register the tenant in multitenant environment. 
 * This acts as callback to register the bootstrap types for each tenant during tenant registration.
 */
@Singleton
public class TenantRegisterationListener implements ITenantRegisterationListener {

    public TenantRegisterationListener() {
        super();
    }

    @Inject
    DefaultMetadataService metadataService;
    
    /* (non-Javadoc)
     * @see org.apache.atlas.repository.ITypeRegisterListener#registerBootstrapTypes()
     */
    @Override
    public void registerBootstrapTypes() throws AtlasException {
        metadataService.restoreTypeSystem();

    }

}
