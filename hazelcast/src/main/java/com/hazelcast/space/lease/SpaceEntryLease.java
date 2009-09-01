package com.hazelcast.space.lease;

import com.hazelcast.space.TupleSpaceImpl;

import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class SpaceEntryLease implements Lease {

    private final Logger logger = Logger.getLogger(SpaceEntryLease.class.getName());

    private String className;
    private long spaceEntryId;
    private long expiry;
    private TupleSpaceImpl space;

    public SpaceEntryLease(TupleSpaceImpl space, String className, long spaceEntryId, long timeToLive) {
        this.space = space;
        this.className = className;
        this.spaceEntryId = spaceEntryId;
        this.expiry = System.currentTimeMillis() + timeToLive;
        if (this.expiry < 0) {
            this.expiry = Long.MAX_VALUE;
        }
        logger.log(Level.INFO, "Setting expiry to " + new Date(this.expiry));
    }

    public long getExpiration() {
        return expiry - System.currentTimeMillis();
    }

    public void cancel() {
        logger.log(Level.INFO, "Cancelling lease for " + spaceEntryId);
        space.updateLease(className, spaceEntryId, 0);
    }

    public void renew(long delta) {
        this.expiry += delta;
        logger.log(Level.INFO, "Renewing lease for " + spaceEntryId + " by " + delta + "ms to " + new Date(this.expiry));
        space.updateLease(className, spaceEntryId, expiry);
    }
}
