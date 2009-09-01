package com.hazelcast.space.lease;

/**
 * TODO
 */
public class SpaceEventListenerLease implements Lease {
    public long getExpiration() {
        return 0;
    }

    public void cancel() {
    }

    public void renew(long duration) {
    }
}
