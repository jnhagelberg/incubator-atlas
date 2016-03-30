// IBM Confidential
// OCO Source Materials
// (C) Copyright IBM Corp. 2014

package org.apache.atlas.utils;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;


public class SetAdapter<S, T> extends AbstractSet<T> {

    private Set<S> source_;
    private Mapper<S,T> mapper_;
    
    public SetAdapter(Set<S> source, Mapper<S, T> mapper) {
        source_ = source;
        mapper_ = mapper;
    }
    
    @Override
    public Iterator<T> iterator() {
        
        return new IteratorAdapter(source_.iterator(), mapper_);
    }

    @Override
    public int size() {

        return source_.size();
    }

}
