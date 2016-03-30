// IBM Confidential
// OCO Source Materials
// (C) Copyright IBM Corp. 2014

package org.apache.atlas.utils;

import java.util.Iterator;

/**
 * @author jeff
 */
public class IterableAdapter<S,T> implements Iterable<T> {

    private Mapper<S, T> mapper_;
    private Iterable<S> source_;

    /**
     * @param iterator
     */
    public IterableAdapter(Iterable<S> iterator, Mapper<S, T> mapper) {

        source_ = iterator;
        mapper_ = mapper;
    }

    @Override
    public Iterator<T> iterator() {
        return new IteratorAdapter<S,T>(source_.iterator(), mapper_);
    }
}