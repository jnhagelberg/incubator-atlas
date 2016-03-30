// IBM Confidential
// OCO Source Materials
// (C) Copyright IBM Corp. 2014

package org.apache.atlas.utils;

import java.util.AbstractCollection;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author jeff
 */
public class CollectionAdapter<S, T> extends AbstractCollection<T> {

    private Mapper<S, T> mapper_;
    private Collection<S> source_;

    public CollectionAdapter(Collection<S> source, Mapper<S, T> mapper) {

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
