package com.hazelcast.hibernate.provider;

import java.util.Properties;

import org.hibernate.cache.Cache;
import org.hibernate.cache.CacheException;
import org.hibernate.cache.CacheProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.hibernate.HazelcastCacheRegionFactory;

/**
 * Implementation of (deprecated) Hibernate <code>CacheProvider</code> interface for compatibility with pre-Hibernate
 * 3.3.x code.
 * <p>
 * To enable, <code>hibernate.cache.provider_class=com.hazelcast.hibernate.provider.HazelcastCacheProvider</code>. This
 * cache provider relies on <code>hazelcast.xml</code> for cache configuration.
 * 
 * @author Leo Kim (lkim@limewire.com)
 * @see HazelcastCache
 * @see HazelcastCacheRegionFactory
 */
public final class HazelcastCacheProvider implements CacheProvider {

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastCacheProvider.class);
    private final IdGenerator idGenerator;

    public HazelcastCacheProvider() {
        idGenerator = Hazelcast.getIdGenerator("HazelcastCacheProviderTimestampIdGenerator");
    }

    /**
     * We ignore the <code>Properties</code> passed in here in favor of the <code>hazelcast.xml</code> file.
     */
    public Cache buildCache(final String name, final Properties properties) throws CacheException {
        return new HazelcastCache(name);
    }

    /**
     * From what I can tell from the <code>{@link org.hibernate.cache.CacheCurrencyStrategy}</code>s implemented in
     * Hibernate, the return value "false" will mean an object will be replaced in a cache if it already exists there,
     * and "true" will not replace it.
     * 
     * @return true - for a large cluster, unnecessary puts will most likely slow things down.
     */
    public boolean isMinimalPutsEnabledByDefault() {
        return true;
    }

    /**
     * @return Output of <code>{@link Hazelcast#getIdGenerator}</code> and <code>{@link IdGenerator#newId()}</code>
     */
    public long nextTimestamp() {
        final long id = idGenerator.newId();
        LOG.info("Got next timestamp ID: {}", id);
        return id;
    }

    public void start(final Properties arg0) throws CacheException {
        LOG.info("Starting up HazelcastCacheProvider...");
    }

    /**
     * Calls <code>{@link Hazelcast#shutdown()}</code>.
     */
    public void stop() {
        LOG.info("Shutting down HazelcastCacheProvider...");
        Hazelcast.shutdown();
    }

}
