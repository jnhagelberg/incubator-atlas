// IBM Confidential
// OCO Source Materials
// (C) Copyright IBM Corp. 2014

package org.apache.atlas.repository.graph.util;

import java.util.AbstractList;
import java.util.List;

/**
 * Adapter to present a list of objects of one type as a list of objects
 * of a different type without the need to copy its contents
 * 
 * @author jeff
 */
public class ListAdapter<S, T> extends AbstractList<T> {

    private Mapper<S, T> mapper_;
    private List<S> source_;

    public ListAdapter(List<S> source, Mapper<S, T> mapper) {

        source_ = source;
        mapper_ = mapper;
    }

    @Override
    public int size() {

        return source_.size();
    }

    @Override
    public T get(int index) {

        return mapper_.map(source_.get(index));
    }
}
