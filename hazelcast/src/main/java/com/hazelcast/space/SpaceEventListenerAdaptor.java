package com.hazelcast.space;

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.space.event.SpaceEvent;
import com.hazelcast.space.event.SpaceEventListener;
import com.hazelcast.space.event.SpaceEventType;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 */
public class SpaceEventListenerAdaptor<T> implements EntryListener {

    private final Logger logger = Logger.getLogger(SpaceEventListenerAdaptor.class.getName());

    private T template;
    private SpaceEventListener listener;
    private TupleSpaceImpl space;

    public SpaceEventListenerAdaptor(TupleSpaceImpl space, T template, long duration, SpaceEventListener listener) {
        this.space = space;
        this.template = template;
        this.listener = listener;
    }

    // verify our entry matches the template
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
            listener.notify(new SpaceEvent(se.getId(), SpaceEventType.ADDED));
        }
    }

    public void entryRemoved(EntryEvent event) {
        Long spaceEntryId = (Long) event.getKey();
        SpaceEntry se = space.readById(template.getClass().getName(), spaceEntryId);
        if (match(se)) {
            listener.notify(new SpaceEvent(se.getId(), SpaceEventType.TAKEN));
        }
    }

    public void entryUpdated(EntryEvent event) {
        Long spaceEntryId = (Long) event.getKey();
        SpaceEntry se = space.readById(template.getClass().getName(), spaceEntryId);
        if (match(se)) {
            listener.notify(new SpaceEvent(se.getId(), SpaceEventType.RENEWED));
        }
    }

    public void entryEvicted(EntryEvent event) {
        logger.log(Level.INFO, "evicted");
        /*
        SpaceEntry se = (SpaceEntry)event.getValue();
        if(match(se)) {
            listener.notify(new SpaceEvent(se.getId(), SpaceEventType.TAKEN));
        }
        */
    }
}
