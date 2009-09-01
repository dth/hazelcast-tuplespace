package com.hazelcast.space;

import com.hazelcast.space.event.SpaceEventListener;
import com.hazelcast.space.lease.Lease;

/**
 * Created by IntelliJ IDEA.
 * User: dharris
 * Date: 19/08/2009
 * Time: 3:24:44 PM
 * To change this template use File | Settings | File Templates.
 */
public interface TupleSpace {
    public <T> T read(T template, long duration);

    public <T> T readIfExists(T template);

    public <T> T take(T template, long duration);

    public <T> T takeIfExists(T template);

    public Lease write(Object o, long duration);

    public void notify(Object template, SpaceEventListener listener, long duration);
}
