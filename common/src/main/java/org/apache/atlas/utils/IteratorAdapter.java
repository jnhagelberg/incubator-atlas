// IBM Confidential
// OCO Source Materials
// (C) Copyright IBM Corp. 2014

package org.apache.atlas.utils;

import java.util.Iterator;

/**
 * @author jeff
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
