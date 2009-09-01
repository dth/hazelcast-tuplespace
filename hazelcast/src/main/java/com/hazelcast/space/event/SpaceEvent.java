package com.hazelcast.space.event;

/**
 * Created by IntelliJ IDEA.
 * User: dth
 * Date: 29/08/2009
 * Time: 11:06:59 PM
 * To change this template use File | Settings | File Templates.
 */
public class SpaceEvent {

    private long id;
    private SpaceEventType type;

    public SpaceEvent(long id, SpaceEventType type) {
        this.id = id;
        this.type = type;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public SpaceEventType getType() {
        return type;
    }

    public void setType(SpaceEventType type) {
        this.type = type;
    }
}
