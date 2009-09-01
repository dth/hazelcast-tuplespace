package com.hazelcast.space;

import com.hazelcast.core.IMap;
import com.hazelcast.core.IdGenerator;
import com.hazelcast.impl.FactoryImpl;
import com.hazelcast.space.event.SpaceEventListener;
import com.hazelcast.space.lease.Lease;
import com.hazelcast.space.lease.SpaceEntryLease;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TODO
 */
public class TupleSpaceImpl implements TupleSpace {

    // our space is really just a distributed MultiMap keyed by classname
//    private MultiMap<String, SpaceEntry> entries = Hazelcast.getMultiMap("_jspace");
//    private IMap<String, Set<SpaceEntry>> entries = Hazelcast.getMap("_jspace");

    private final FactoryImpl factory;
    private IdGenerator idGenerator;

    private final Logger logger = Logger.getLogger(TupleSpaceImpl.class.getName());

    public TupleSpaceImpl(FactoryImpl factory) {
        this.factory = factory;
        this.idGenerator = factory.getIdGenerator("_space_ids");
    }

    /**
     * Our TupleSpace is really just a Set, one per class/template.
     */
//    private ISet<SpaceEntry> getSpace(String className) {
    private IMap getSpace(String className) {
//        return factory.getSet("_space_"+className);
        return factory.getMap("_space_" + className);
    }

    /*
        public void update(SpaceEntry se) {
            String className = se.getClass().getName();
    //        IMap<Long, SpaceEntry> m = getSpace(className);
            ConcurrentMap map = getSpace(className);
            SpaceEntry s2 = (SpaceEntry)map.get(se.getId());
            System.out.println("Read in update: " + s2);
            SpaceEntry s = (SpaceEntry)map.replace(se.getId(), se);
    //        SpaceEntry s = ((ConcurrentMap<Long, SpaceEntry>) m).replace(se.getId(), se);
            if (s != null) {
                logger.info("Old: " + s.toString());
            }
            logger.info("New: " + se.toString());
    //        set.remove(se);
    //        set.add(se);
        }
    */
    public SpaceEntry readById(String className, long id) {
        if (className == null) {
            return null;
        }
        ConcurrentMap<Long, SpaceEntry> c = getSpace(className);
        SpaceEntry found = c.get(id);
        return found;
    }

    public SpaceEntry updateLease(String className, long id, long expiry) {
        SpaceEntry found = null;
        ConcurrentMap<Long, SpaceEntry> c = getSpace(className);
        found = c.get(id);
        if (found != null) {
            logger.log(Level.INFO, "1 Found SpaceEntry: " + found);
            found.setExpiration(expiry);
            c.replace(found.getId(), found);
        }
        found = c.get(id);
        if (found != null) {
            logger.log(Level.INFO, "2 Found SpaceEntry: " + found);
        }
        return found;
    }

    public <T> T read(T template, long duration) {
        return readOrTake(template, duration, false);
    }

    private SpaceEntry readOrTakeImmediate(Object template, boolean remove) {
        String className = template.getClass().getName();
        Collection<SpaceEntry> c = getSpace(className).values();
        logger.log(Level.INFO, "Size of space for " + className + ": " + c.size());
        SpaceEntry found = null;
        List<SpaceEntry> expired = new ArrayList<SpaceEntry>();
        if (c != null && !c.isEmpty()) {
            Map templateElements = extractElements(template);
            for (SpaceEntry e : c) {
                if (hasExpired(e)) {
                    expired.add(e);
                } else {
                    if (elementsMatch(templateElements, e.getElements())) {
                        found = e;
                        if (remove) {
                            logger.log(Level.INFO, "Removing " + className + " entry with id " + found.getId());
                            getSpace(className).remove(found.getId());
                        }
                        break;
                    }
                }
            }
        }

        // TODO: expire objects in a separate thread
        if (expired.size() > 0) {
            logger.log(Level.INFO, expired.size() + " elements have expired and need to be purged");
        }
        return found;
    }

    private boolean hasExpired(SpaceEntry se) {
        logger.log(Level.FINE, "SpaceEntry " + se.getId() + " Expiry: " + new Date(se.getExpiration()));
        return se.getExpiration() < System.currentTimeMillis();
    }

    private <T> T readOrTake(T template, long duration, boolean take) {
        T found = null;
        SpaceEntry entry = readOrTakeImmediate(template, take);
        String className = template.getClass().getName();
        if (entry == null && duration > 0) {
            BlockingSpaceEventListener<SpaceEntry> l = new BlockingSpaceEventListener<SpaceEntry>(this, template, duration);
            getSpace(className).addEntryListener(l, false);
            // wait until duration...
            l.block();
            if (l.isNotified()) {
                entry = l.getEntry();
            }
            if (take && entry != null) {
                logger.log(Level.INFO, "Removing " + className + " entry with id " + entry.getId());
                getSpace(className).remove(entry);
            }
        }
        if (entry != null) {
            found = (T) entry.getEntry();
        }
        return found;
    }

    public <T> T readIfExists(T template) {
        return read(template, 0);
    }

    public <T> T take(T template, long duration) {
        return readOrTake(template, duration, true);
    }

    public <T> T takeIfExists(T template) {
        return take(template, 0);
    }

    public Lease write(Object o, long duration) {
        if (o == null) {
            return null;
        }
        String className = o.getClass().getName();
        // TODO: problem here with the idgenerator blocking subsequent vm's?
        long id = idGenerator.newId();
        SpaceEntry se = new SpaceEntry(id, o, extractElements(o), duration);
//        getSpace(className).add(se);
        getSpace(className).put(id, se);
        // TODO: do we need to keep track of leases?
        Lease lease = new SpaceEntryLease(this, className, id, duration);
        logger.log(Level.INFO, "Added " + className + " entry with id " + id);
        return lease;
    }

    // TODO: cancel listeners after duration expires
    public void notify(Object template, SpaceEventListener listener, long duration) {
        SpaceEventListenerAdaptor l = new SpaceEventListenerAdaptor(this, template, duration, listener);
//        Lease l = new SpaceEventListenerLease(this, );
//        getSpace(template.getClass().getName()).addItemListener(l, true);
        getSpace(template.getClass().getName()).addEntryListener(l, false);
    }

    private Map extractElements(Object o) {
        Map<String, String> m = extractMethods(o);
        m.putAll(extractFields(o));
        return m;
    }

    private boolean elementsMatch(Map templateElements, Map entryElements) {
        Set templateSet = templateElements.entrySet();
        Set<Map.Entry> entrySet = entryElements.entrySet();
        return entrySet.containsAll(templateSet);
    }

    /**
     * Extract public getXxx'ers
     */
    private Map<String, String> extractMethods(Object o) {
        Method[] methods = o.getClass().getMethods();
        Map<String, String> map = new HashMap<String, String>();
        for (Method m : methods) {
            String name = m.getName();
            if (name.startsWith("get") && m.getTypeParameters().length == 0) {
                try {
                    Object v = m.invoke(o, null);
                    if (v != null) {
                        // TODO: remove "class" from getClass output
                        map.put(name, String.valueOf(v));
                    }
                } catch (IllegalAccessException e) {
                    // TODO
                } catch (InvocationTargetException e) {
                    // TODO
                }
            }
        }
        return map;
    }

    /**
     * Extract public fields from the object.
     */
    private Map<String, String> extractFields(Object o) {
        Field[] fields = o.getClass().getFields();
        Map<String, String> m = new HashMap<String, String>();
        for (Field f : fields) {
            try {
                String name = f.getName();
                Object value = f.get(o);
                if (value != null) {
                    m.put(name, String.valueOf(value));
                }
            } catch (IllegalAccessException e) {
                // TODO
            }
        }
        return m;
    }
}
