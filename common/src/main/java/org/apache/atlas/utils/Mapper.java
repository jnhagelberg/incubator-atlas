// IBM Confidential
// OCO Source Materials
// (C) Copyright IBM Corp. 2014

package org.apache.atlas.utils;

/**
 * @author jeff
 */
public interface Mapper<S, T> {

    T map(S source);
}
