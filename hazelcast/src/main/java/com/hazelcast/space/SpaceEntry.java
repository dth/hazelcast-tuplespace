package com.hazelcast.space;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 */
public class SpaceEntry implements Serializable {

    private final static Logger logger = Logger.getLogger(SpaceEntry.class.getName());

    private long id;
    private Map elements;
    private Object entry;
    private long expiry;

    public SpaceEntry(long id, Object entry, Map elements, long timeToLive) {
        this.id = id;
        this.entry = entry;
        this.elements = elements;
        this.expiry = System.currentTimeMillis() + timeToLive;
        if (this.expiry < 0) {
            this.expiry = Long.MAX_VALUE;
        }
        logger.log(Level.INFO, "Setting expiry to " + new Date(this.expiry));
    }

    public Map getElements() {
        return elements;
    }

    public void setElements(Map elements) {
        this.elements = elements;
    }

    public Object getEntry() {
        return entry;
    }

    public void setEntry(Object entry) {
        this.entry = entry;
    }

    public long getExpiration() {
        return expiry;
    }

    public void setExpiration(long expiry) {
        this.expiry = expiry;
        logger.log(Level.INFO, "Setting expiry to " + new Date(this.expiry));
    }

    public long getId() {
        return id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SpaceEntry that = (SpaceEntry) o;

        if (id != that.id) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }

    @Override
    public String toString() {
        return "SpaceEntry{" +
                "id=" + id +
                ", entryClass=" + entry.getClass().getName() +
                ", expiry=" + new Date(expiry) +
                '}';
    }
}
