package com.hazelcast.space;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by IntelliJ IDEA.
 * User: dth
 * Date: 22/08/2009
 * Time: 1:30:45 AM
 * To change this template use File | Settings | File Templates.
 */
public class BlockingSpaceEventListener<E> implements EntryListener {

    private final Logger logger = Logger.getLogger(BlockingSpaceEventListener.class.getName());

    private SpaceEntry entry;
    private boolean notified = false;
    private long msToBlock;
    private CountDownLatch latch;
    private Object template;
    private TupleSpaceImpl space;

    public BlockingSpaceEventListener(TupleSpaceImpl space, Object template, long duration) {
        this.space = space;
        this.template = template;
        this.msToBlock = duration;
        this.latch = new CountDownLatch(1);
    }

    private boolean match(SpaceEntry item) {
        if (item == null) {
            return false;
        }
        return SpaceHelper.elementsMatch(SpaceHelper.extractElements(template), item.getElements());
    }

    public void entryAdded(EntryEvent event) {
        Long spaceEntryId = (Long) event.getKey();
        SpaceEntry se = space.readById(template.getClass().getName(), spaceEntryId);
        if (match(se)) {
            notified = true;
            this.entry = se;
            unblock();
        }
    }

    public void entryRemoved(EntryEvent event) {
    }

    public void entryUpdated(EntryEvent event) {
    }

    public void entryEvicted(EntryEvent event) {
    }

    public boolean isNotified() {
        return notified;
    }

    private void unblock() {
        logger.log(Level.INFO, "Unblocking");
        latch.countDown();
    }

    public void block() {
        try {
            logger.log(Level.INFO, "Blocking for " + msToBlock + "ms");
            latch.await(msToBlock, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
            logger.log(Level.INFO, "interrupted..");
            // unblocked
        }
    }

    public SpaceEntry getEntry() {
        return entry;
    }
}