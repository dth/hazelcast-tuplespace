package com.hazelcast.space;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.Transaction;
import com.hazelcast.space.event.SpaceEvent;
import com.hazelcast.space.event.SpaceEventListener;
import com.hazelcast.space.lease.Lease;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 */
public class TupleSpaceTest {

    private static long TEN_MINUTES = 10 * 60 * 1000;
    private static long TEN_SECONDS = 10 * 1000;
    private static long ONE_MINUTE = 1 * 60 * 1000;


    private Object generateObject() {
        return new PublicTestSpaceObject();
    }

    @Test
    public void testTransactionalOperations() throws Exception {
        TupleSpace space = Hazelcast.getSpace();
        Transaction t = Hazelcast.getTransaction();
        t.begin();
        space.write(generateObject(), TEN_MINUTES);
        Thread.sleep(TEN_SECONDS);
        t.commit();
        System.out.println("Commited");
    }

    @Test
    public void testSpaceInstance() {
        TupleSpace space = Hazelcast.getSpace();
        assertNotNull(space);
    }

    @Test
    public void testSpaceWrite() {
        TupleSpace space = Hazelcast.getSpace();
        PublicTestSpaceObject o = new PublicTestSpaceObject();
        o.arg0 = "foo";
        space.write(o, TEN_MINUTES);
        assertNotNull(o);
        assertEquals(o.arg0, "foo");
    }

    @Test
    public void testContinuousWrite() throws Exception {
        TupleSpace space = Hazelcast.getSpace();
        while (true) {
            space.write(generateObject(), TEN_MINUTES);
            Thread.sleep(1000);
        }
    }

    @Test
    public void testContinuousTake() throws Exception {
        TupleSpace space = Hazelcast.getSpace();
        while (true) {
            space.take(generateObject(), TEN_MINUTES);
            Thread.sleep(1000);
        }
    }

    @Test
    public void testSpaceReadWrite() {
        TupleSpace space = Hazelcast.getSpace();
        PublicTestSpaceObject w = new PublicTestSpaceObject();
        w.arg0 = "foo";
        space.write(w, TEN_MINUTES);
        assertNotNull(w);
        PublicTestSpaceObject r = space.read(new PublicTestSpaceObject(), 100);
        assertNotNull(r);
        assertEquals(r.arg0, "foo");
    }

    @Test
    public void testMultipleRead() {
        TupleSpace space = Hazelcast.getSpace();
        PublicTestSpaceObject w = new PublicTestSpaceObject();
        w.arg0 = "foo";
        space.write(w, TEN_MINUTES);
        assertNotNull(w);
        for (int i = 1; i < 1000; i++) {
            PublicTestSpaceObject r = space.read(new PublicTestSpaceObject(), 0);
            assertNotNull(r);
            assertEquals(r.arg0, "foo");
        }
    }

    @Test
    public void testSingleTake() {
        TupleSpace space = Hazelcast.getSpace();
        PublicTestSpaceObject w = new PublicTestSpaceObject();
        w.arg0 = "foo";
        space.write(w, TEN_MINUTES);
        assertNotNull(w);
        PublicTestSpaceObject r = space.take(new PublicTestSpaceObject(), 0);
        assertNotNull(r);
        assertEquals(r.arg0, "foo");
    }

    @Test
    public void testMultipleTake() {
        TupleSpace space = Hazelcast.getSpace();
        PublicTestSpaceObject w = new PublicTestSpaceObject();
        w.arg0 = "foo";
        space.write(w, TEN_MINUTES);
        assertNotNull(w);
        PublicTestSpaceObject r = space.take(new PublicTestSpaceObject(), 0);
        assertNotNull(r);
        assertEquals(r.arg0, "foo");
        for (int i = 0; i < 1000; i++) {
            PublicTestSpaceObject r2 = space.take(new PublicTestSpaceObject(), 0);
            assertNull(r2);
        }
    }

    @Test
    public void testReadWithTimeout() {
        TupleSpace space = Hazelcast.getSpace();
        PublicTestSpaceObject r = space.read(new PublicTestSpaceObject(), Long.MAX_VALUE);
        assertNotNull(r);
        assertEquals(r.arg0, "foo");
    }

    @Test
    public void testListener() throws Exception {
        final TupleSpace space = Hazelcast.getSpace();
        final PublicTestSpaceObject template = new PublicTestSpaceObject();
        space.notify(template, new SpaceEventListener() {
            public void notify(SpaceEvent event) {
//                PublicTestSpaceObject x = space.readIfExists(template);
                PublicTestSpaceObject x = space.takeIfExists(template);
                if (x != null) {
                    System.out.println("Read item! " + x);
                }
            }
        }, Lease.FOREVER);
        System.out.println("Listening for any events");
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void testExpiration() throws Exception {
        TupleSpace space = Hazelcast.getSpace();
        PublicTestSpaceObject o = new PublicTestSpaceObject();
        space.write(o, Lease.FOREVER);
        Thread.sleep(1000);
        PublicTestSpaceObject r = space.read(new PublicTestSpaceObject(), Long.MAX_VALUE);
        assertNotNull(r);
    }

    @Test
    public void printExpiry() throws Exception {
        TupleSpace space = Hazelcast.getSpace();
        while (true) {
            PublicTestSpaceObject r = space.read(new PublicTestSpaceObject(), Long.MAX_VALUE);
            Thread.sleep(1000);
        }
    }

    @Test
    public void increaseLease() throws Exception {
        TupleSpace space = Hazelcast.getSpace();
        PublicTestSpaceObject o = new PublicTestSpaceObject();
        Lease l = space.write(o, ONE_MINUTE);
        while (true) {
            System.out.println("Increasing lease by 1000ms");
            l.renew(1000);
            Thread.sleep(1000);
        }
    }
}
