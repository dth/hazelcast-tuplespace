package com.hazelcast.space.event;

/**
 * Created by IntelliJ IDEA.
 * User: dharris
 * Date: 19/08/2009
 * Time: 3:34:23 PM
 * To change this template use File | Settings | File Templates.
 */
public interface SpaceEventListener {
    // TODO do we ship around the Entry or just an event with the id of the matching entry. It's then up to listeners to
    // TODO call space.take() or space.read() to retrieve it. ?
//    public void entryAdded(SpaceEntry entry);
//    public void entryTaken(SpaceEntry entry);
//    public void entryExpired(SpaceEntry entry);     // TODO

    //    public void entryRenewed(SpaceEntry entry);     // TODO
    public void notify(SpaceEvent event);
}
