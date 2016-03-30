//
// IBM Confidential
// OCO Source Materials
// (c) Copyright IBM Corp. 2007, 2013
//
package org.apache.atlas.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A fixed size Map that evicts the least recently accessed value when adding a
 * new value if the map is full.
 * <P>
 * Can be used as a LRU cache. When an entry is added or retrieved, a time stamp
 * associated with that entry is updated. If too many entries are added, the
 * least recently used entry will be evicted.
 * <P>
 * The programmer can also explicitly evict entries which allows the entry to be
 * returned to the programmer in case some kind of clean up is needed. For
 * example: <br/><code>
 * if (mycache.size() == mycache.getCapacity()) {<br/>
 *    Map.Entry e = mycache.evict();<br/>
 *    MyObject o = (MyObject)e.getValue();<br/>
 *    o.cleanup();<br/>
 * }<br/>
 * mycache.put("key", anotherMyObject);<br/>
 * </code>
 * <br/>But if no cleanup is needed for evicted objects, just <br/>
 * <code>mycache.put("key", anotherMyObject);</code> <br/>is enough.
 * <P>
 * Note that this implementation is not synchronized. Also, entrySet() and
 * values() have not been implemented.
 */
public class LruMap<TypeKey, TypeValue> implements Map<TypeKey, TypeValue> {

    private static final Logger logger_ = Logger.getLogger(LruMap.class.getName());
    
    /**
     * The entries held in the LruMap.
     */
    private static class CachedEntry<TypeValue> {
        // Time this entry was last accessed.
        private Date lastUsed_;
        // The object stored in this entry.
        private TypeValue object_;

        /**
         * Constructor.
         * @param o the value of the entry.
         */
        public CachedEntry(TypeValue o) {
            object_ = o;
            lastUsed_ = new Date();
        }
    }

    /**
     * Implementation of the Entry interface that is externally visible, specifically 
     * as returned from {@link LruMap#evict()} and related methods.
     */
    private static class LruEntryImpl<TypeKey, TypeValue> implements Entry<TypeKey, TypeValue> {

        private TypeKey key_;
        private TypeValue value_;

        /**
         * Constructor.
         * @param key
         * @param value
         */
        public LruEntryImpl(TypeKey key, TypeValue value) {
           key_ = key;
           value_ = value;
        }

        /* (non-Javadoc)
         * @see java.util.Map.Entry#getKey()
         */
        @Override
        public TypeKey getKey() {
            return key_;
        }

        /* (non-Javadoc)
         * @see java.util.Map.Entry#getValue()
         */
        @Override
        public TypeValue getValue() {
            return value_;
        }

        /* (non-Javadoc)
         * @see java.util.Map.Entry#setValue(java.lang.Object)
         */
        @Override
        public TypeValue setValue(TypeValue object) {
            throw new UnsupportedOperationException();
        }
        
    }
    
    // The Map that backs this cache.
    private Map<TypeKey, CachedEntry<TypeValue>> cache_;
    // The maximum number of entries the cache holds.
    private int capacity_;


    /**
     * Construct a new LruMap of a given size.
     * @param capacity the maximum number of entries the map can hold.
     */
    public LruMap(int capacity) {
        capacity_ = capacity;
        cache_ = new HashMap<TypeKey, CachedEntry<TypeValue>>(capacity);

        if (logger_.isLoggable(Level.FINE)) {
            logger_.fine("created LruMap=" + this + " capacity=" + capacity);
        }
    }


    /* (non-Javadoc)
     * @see java.util.Map#clear()
     */
    @Override
    public void clear() {
        if (logger_.isLoggable(Level.FINE)) {
            logger_.fine("clearing LruMap=" + this);
        }
        cache_.clear();
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    @Override
    public boolean containsKey(Object key) {
        return cache_.containsKey(key);
    }

    /* (non-Javadoc)
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    @Override
    public boolean containsValue(Object value) {
        for (CachedEntry<TypeValue> e : cache_.values()) {
            if (value.equals(e)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Not implemented.  Throws UnsupportedOperationException.
     * 
     * @see java.util.Map#entrySet()
     */
    @Override
    public Set<Entry<TypeKey, TypeValue>> entrySet() {
        // TODO need to implement
        // This is harder to do since they want the set to be backed by the map.
        throw new UnsupportedOperationException();
    }

    /**
     * Equals is not implemented as described in the Javadoc for Map.
     * It just sees if two objects are equal.
     * @see java.util.Map#equals(Object o)
     */
    public boolean equals(Object o) {
        // TODO Need to implement equals too.  
        // This depends on entrySet being implemented first.
        if (!(o instanceof LruMap)) {
            return false;
        }
        return super.equals(o);
    }

    /**
     * Not really implemented at this point. Provided to avoid code analysis issue due to
     * equals() being defined, but not hashCode().
     */
    @Override
    public int hashCode() {
        return 0;
    }

    /**
     * Gets an entry and also resets its last used time.
     * 
     * @see java.util.Map#get(Object key)
     */
    @Override
    public TypeValue get(Object key) {
        CachedEntry<TypeValue> e = cache_.get(key);
        if (e == null) {
            return null;
        }

        e.lastUsed_ = new Date();
        return e.object_;
    }

    /* (non-Javadoc)
     * @see java.util.Map#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return cache_.isEmpty();
    }

    /* (non-Javadoc)
     * @see java.util.Map#keySet()
     */
    @Override
    public Set<TypeKey> keySet() {
        return cache_.keySet();
    }

    /* (non-Javadoc)
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    @Override
    public TypeValue put(TypeKey key, TypeValue value) {
        CachedEntry<TypeValue> e = new CachedEntry<TypeValue>(value);
        if (cache_.size() == capacity_) {
            evict();
        }

        CachedEntry<TypeValue> previousCachedEntry = cache_.put(key, e);
        if (previousCachedEntry == null) {
            return null;
        }
        return previousCachedEntry.object_;
    }

    /* (non-Javadoc)
     * @see java.util.Map#putAll(java.util.Map)
     */
    @Override
    public void putAll(Map<? extends TypeKey, ? extends TypeValue> map) {
        for (Entry<? extends TypeKey, ? extends TypeValue> entry : map.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    /* (non-Javadoc)
     * @see java.util.Map#remove(java.lang.Object)
     */
    @Override
    public TypeValue remove(Object key) {
        CachedEntry<TypeValue> e = cache_.remove(key);
        if (e == null) {
            return null;
        }
        return e.object_;
    }

    /* (non-Javadoc)
     * @see java.util.Map#size()
     */
    @Override
    public int size() {
        return cache_.size();
    }

    /**
     * Not implemented.  Throws UnsupportedOperationException.
     * 
     * @see java.util.Map#values()
     */
    @Override
    public Collection<TypeValue> values() {
        // TODO not implemented
        throw new UnsupportedOperationException();
    }

    /**
     * Evicts the least recently used entry.
     * @return the least recently used entry or null if the LruMap is empty.
     */
    public Entry<TypeKey, TypeValue> evict() {
        // If cache is empty, return null.
        if (size() == 0) {
            return null;
        }

        Entry<TypeKey, CachedEntry<TypeValue>> candidate = null;
        for (Entry<TypeKey, CachedEntry<TypeValue>> e : cache_.entrySet()) {
            if (candidate == null ||
                    e.getValue().lastUsed_.before(candidate.getValue().lastUsed_)) {
                candidate = e;
            }
        }

        Entry<TypeKey, TypeValue> e = new LruEntryImpl<TypeKey, TypeValue>(
                candidate.getKey(), candidate.getValue().object_);
        cache_.remove(candidate.getKey());

        if (logger_.isLoggable(Level.FINE)) {
            logger_.fine("evicted entry with key=" + e.getKey() + " and value=" + e.getValue() + 
                    " from LruMap=" + this);
        }

        return e;
    }

    /**
     * Returns the capacity of the LruMap.
     * @return the capacity of the LruMap.
     */
    public int getCapacity() {
        return capacity_;
    }

    /**
     * Sets the capacity of this LruMap. If the new capacity is less than the
     * current size, elements will be evicted to bring the size of the LruMap
     * down to the new capacity.
     * @param capacity the capacity to set.
     * @return a set of the {@link Entry}'s that were evicted. The Set will be
     *         empty if the resizing did not cause any Entries to be evicted.
     */
    public Collection<Entry<TypeKey, TypeValue>> setCapacity(int capacity) {
        Collection<Entry<TypeKey, TypeValue>> evicted = new ArrayList<Entry<TypeKey, TypeValue>>();
        capacity_ = capacity;
        // Evict elements until the new capacity is reached.
        while (cache_.size() > capacity) {
            evicted.add(evict());
        }
        return evicted;
    }

    /**
     * Reaps entries that have not been accessed within the a specified time.
     * 
     * @param seconds
     *            any entry not accessed within the specified number of seconds
     *            will be reaped from the LruMap. If seconds is not positive, no
     *            action is taken.
     * @return returns any values that were removed from the LruMap as a set of
     *         {@link Entry}s.
     */
    public Collection<Entry<TypeKey, TypeValue>> reap(long seconds) {
        if (logger_.isLoggable(Level.FINE)) {
            logger_.fine("reap called LruMap=" + this + " seconds=" + seconds);
        }
        // The reaped objects to return.
        Collection<Entry<TypeKey, TypeValue>> reaped = new ArrayList<Entry<TypeKey, TypeValue>>();

        // If seconds is not positive, return immediately.
        if (seconds <= 0) {
            return reaped;
        }
        
        // Any entries not accessed since the reap time will be evicted.
        long reapTime = new Date().getTime() - (seconds * 1000);

        Iterator<Entry<TypeKey, CachedEntry<TypeValue>>> itr = cache_.entrySet().iterator();
        while (itr.hasNext()) {
            Entry<TypeKey, CachedEntry<TypeValue>> e = itr.next();

            long lastUsed =  e.getValue().lastUsed_.getTime();
            if (lastUsed <= reapTime) {
                if (logger_.isLoggable(Level.FINE)) {
                    logger_.fine("reaping object with key=" + e.getKey());
                }

                Entry<TypeKey, TypeValue> reapedEntry = 
                        new LruEntryImpl<TypeKey, TypeValue>(e.getKey(), e.getValue().object_);
                reaped.add(reapedEntry);
                itr.remove();
            }
        }

        return reaped;
    }

}
