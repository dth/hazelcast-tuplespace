package com.hazelcast.hibernate.provider;

import java.util.Map;

import org.hibernate.cache.Cache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.Timestamper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.hibernate.HazelcastCacheRegionFactory;

/**
 * Implementation of (deprecated) Hibernate <code>Cache</code> interface for compatibility with pre-Hibernate 3.3.x
 * code.
 * 
 * @author Leo Kim (lkim@limewire.com)
 * @see HazelcastCacheProvider
 * @see HazelcastCacheRegionFactory
 */
public final class HazelcastCache implements Cache {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastCache.class);
    private final IMap cache;
    private final String regionName;

    public HazelcastCache(final String regionName) {
        LOG.info("Creating new HazelcastCache with region name: {}", regionName);
        cache = Hazelcast.getMap(regionName);
        this.regionName = regionName;
    }

    public void clear() throws CacheException {
        LOG.info("Calling clear() on {}", regionName);
        cache.clear();
    }

    public void destroy() throws CacheException {
        LOG.info("Calling destroy() on {}", regionName);
        cache.destroy();
    }

    public long getElementCountInMemory() {
        return cache.size();
    }

    public long getElementCountOnDisk() {
        return -1L;
    }

    public String getRegionName() {
        return regionName;
    }

    /**
     * @return a rough estimate of number of bytes used by this region.
     */
    public long getSizeInMemory() {
        long size = 0;
        for (final Object key : cache.keySet()) {
            final MapEntry entry = cache.getMapEntry(key);
            if (entry != null) {
                size += entry.getCost();
            }
        }
        return size;
    }

    public int getTimeout() {
        return Timestamper.ONE_MS * 60000;
    }

    public long nextTimestamp() {
        return System.currentTimeMillis() / 100;
    }

    public void lock(final Object key) throws CacheException {
        LOG.debug("Locking region {}", regionName);
        cache.lock(key);
    }

    public void unlock(final Object key) throws CacheException {
        LOG.debug("Unlocking region {}", regionName);
        cache.unlock(key);
    }

    public Object get(final Object key) throws CacheException {
        LOG.debug("Getting key {} from region {}", key, regionName);
        return cache.get(key);
    }

    public Object read(final Object key) throws CacheException {
        return get(key);
    }

    public void put(final Object key, final Object value) throws CacheException {
        LOG.debug("Putting object into {}", regionName);
        LOG.debug("Using key {}, value {}", key, value);
        cache.put(key, value);
    }

    public void remove(final Object key) throws CacheException {
        LOG.debug("Removing key {} from region {}", key, regionName);
        cache.remove(key);
    }

    public void update(final Object key, final Object value) throws CacheException {
        LOG.debug("Called update in {}", regionName);
        put(key, value);
    }

    /**
     * @return the internal <code>IMap</code> used for this cache.
     */
    public Map toMap() {
        return cache;
    }
}
