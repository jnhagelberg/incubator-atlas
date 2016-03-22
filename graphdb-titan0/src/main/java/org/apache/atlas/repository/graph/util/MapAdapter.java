// IBM Confidential
// OCO Source Materials
// (C) Copyright IBM Corp. 2014

package org.apache.atlas.repository.graph.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;


public class MapAdapter<K,S,T> implements Map<K, T> {

    private Mapper<S,T> mapper_;
    private Mapper<java.util.Map.Entry<K, S>, java.util.Map.Entry<K, T>> entryMapper_ = new EntryMapper();
    private Map<K,S> delegate_;
    
    public MapAdapter(Map<K,S> delegate, Mapper<S,T> mapper) {
        delegate_ = delegate;
        mapper_ = mapper;
    }
    
    @Override
    public void clear() {
        delegate_.clear();        
    }

    @Override
    public boolean containsKey(Object key) {
        return delegate_.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return delegate_.containsValue(value);
    }

    @Override
    public Set<Map.Entry<K, T>> entrySet() {
        Set<Map.Entry<K, S>> delegateEntrySet = delegate_.entrySet();        
        return new SetAdapter<Map.Entry<K, S>, Map.Entry<K, T>>(delegateEntrySet, entryMapper_);
    }

    @Override
    public T get(Object key) {

        return mapper_.map(delegate_.get(key));
    }

    @Override
    public boolean isEmpty() {

        return delegate_.isEmpty();
    }

    @Override
    public Set<K> keySet() {
        return delegate_.keySet();
    }

    @Override
    public T put(K key, T value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(Map<? extends K, ? extends T> m) {
        throw new UnsupportedOperationException();        
    }

    @Override
    public T remove(Object key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {

        return delegate_.size();
    }

    @Override
    public Collection<T> values() {

        return new CollectionAdapter<S,T>(delegate_.values(), mapper_);
    }

    private final class EntryMapper implements Mapper<Map.Entry<K, S>, Map.Entry<K, T>> {

        @Override
        public java.util.Map.Entry<K, T> map(java.util.Map.Entry<K, S> source) {

            return new EntryAdapter<K,S,T>(source, mapper_);
        }
    }

    static class EntryAdapter<K, S, T> implements Map.Entry<K,T> {
        
        private Mapper<S,T> mapper_;
        private Map.Entry<K, S> entry_;
        
        public EntryAdapter(Map.Entry<K, S> entry, Mapper<S,T> mapper) {
            entry_ = entry;
            mapper_ = mapper;
        }
        
        @Override
        public K getKey() {

            return entry_.getKey();
        }
        @Override
        public T getValue() {

            return mapper_.map(entry_.getValue());
        }
        @Override
        public T setValue(T value) {

            throw new UnsupportedOperationException();
        }
    
    }
}
