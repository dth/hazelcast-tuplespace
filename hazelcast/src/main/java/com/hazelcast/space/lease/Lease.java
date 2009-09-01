package com.hazelcast.space.lease;

/**
 */
public interface Lease {

    public static final long FOREVER = Long.MAX_VALUE;

    /**
     * Gets the expiration of the current lease
     *
     * @return the current expiration in seconds from current time
     */
    public long getExpiration();

    /**
     * Cancels the lease. This effectively expires the SpaceEntry
     */
    public void cancel();

    /**
     * Renew the lease for the SpaceEntry by increasing expiry
     *
     * @param duration duration to increase lease by
     */
    public void renew(long duration);
}
