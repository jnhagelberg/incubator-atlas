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
package org.apache.atlas.utils.adapters;

import java.util.Iterator;

/**
 * Adapter that allows an Iterator for objects of one type (S)
 * to be used as an Iterator for objects of another type (T).   The
 * objects are lazily converted to the target type using a Mapper.
 */
public class IteratorAdapter<S, T> implements Iterator<T> {

    private Mapper<S, T> mapper_;
    private Iterator<S> source_;

    /**
     * @param iterator
     */
    public IteratorAdapter(Iterator<S> iterator, Mapper<S, T> mapper) {

        source_ = iterator;
        mapper_ = mapper;

    }

    @Override
    public boolean hasNext() {

        return source_.hasNext();
    }

    @Override
    public T next() {

        return mapper_.map(source_.next());
    }

    @Override
    public void remove() {

        source_.remove();
    }
}
