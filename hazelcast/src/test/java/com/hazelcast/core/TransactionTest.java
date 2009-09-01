package com.hazelcast.core;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.*;

public class TransactionTest {
    @Test
    @Ignore
    public void testMapPutSimple() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        txnMap.begin();
        txnMap.put("1", "value");
        txnMap.commit();
    }

    @Test
    public void testMapIterateEntries() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        txnMap.put("1", "value1");
        assertEquals(1, txnMap.size());
        txnMap.begin();
        txnMap.put("2", "value2");
        assertEquals(2, txnMap.size());
        Set<Map.Entry> entries = txnMap.entrySet();
        for (Map.Entry entry : entries) {
            if ("1".equals(entry.getKey())) {
                assertEquals("value1", entry.getValue());
            } else if ("2".equals(entry.getKey())) {
                assertEquals("value2", entry.getValue());
            } else throw new RuntimeException ("cannot contain another entry with key " + entry.getKey());
        }
        txnMap.commit();
        assertEquals(2, txnMap.size());
    }

    @Test
    public void testMapIterateEntries2() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        assertEquals(0, txnMap.size());
        txnMap.begin();
        txnMap.put("1", "value1");
        txnMap.put("2", "value2");
        assertEquals(2, txnMap.size());
        Set<Map.Entry> entries = txnMap.entrySet();
        for (Map.Entry entry : entries) {
            if ("1".equals(entry.getKey())) {
                assertEquals("value1", entry.getValue());
            } else if ("2".equals(entry.getKey())) {
                assertEquals("value2", entry.getValue());
            } else throw new RuntimeException ("cannot contain another entry with key " + entry.getKey());
        }
        txnMap.commit();
        assertEquals(2, txnMap.size());
    }

    @Test
    public void testMapIterateEntries3() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        txnMap.put("1", "value1");
        assertEquals(1, txnMap.size());
        txnMap.begin();
        txnMap.put("1", "value2");
        assertEquals(1, txnMap.size());
        Set<Map.Entry> entries = txnMap.entrySet();
        for (Map.Entry entry : entries) {
            if ("1".equals(entry.getKey())) {
                assertEquals("value2", entry.getValue());
            } else throw new RuntimeException ("cannot contain another entry with key " + entry.getKey());
        }
        txnMap.rollback();
        assertEquals(1, txnMap.size());
        entries = txnMap.entrySet();
        for (Map.Entry entry : entries) {
            if ("1".equals(entry.getKey())) {
                assertEquals("value1", entry.getValue());
            } else throw new RuntimeException ("cannot contain another entry with key " + entry.getKey());
        }
    }


    @Test
    public void testMapPutCommitSize() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        IMap imap = newMapProxy("testMap");
        txnMap.put("1", "item");
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.begin();
        txnMap.put(2, "newone");
        assertEquals(2, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.commit();
        assertEquals(2, txnMap.size());
        assertEquals(2, imap.size());
    }

    @Test
    public void testMapPutRollbackSize() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        IMap imap = newMapProxy("testMap");
        txnMap.put("1", "item");
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.begin();
        txnMap.put(2, "newone");
        assertEquals(2, txnMap.size());
        assertEquals(1, imap.size());
        txnMap.rollback();
        assertEquals(1, txnMap.size());
        assertEquals(1, imap.size());
    }

    @Test
    public void testSetAddWithTwoTxn() {
        Hazelcast.getSet("test").add("1");
        Hazelcast.getSet("test").add("1");
        TransactionalSet set = newTransactionalSetProxy("test");
        TransactionalSet set2 = newTransactionalSetProxy("test");
        assertEquals(1, set.size());
        assertEquals(1, set2.size());
        set.begin();
        set.add("2");
        assertEquals(2, set.size());
        assertEquals(1, set2.size());
        set.commit();
        assertEquals(2, set.size());
        assertEquals(2, set2.size());
        set2.begin();
        assertEquals(2, set.size());
        assertEquals(2, set2.size());
        set2.remove("1");
        assertEquals(2, set.size());
        assertEquals(1, set2.size());
        set2.commit();
        assertEquals(1, set.size());
        assertEquals(1, set2.size());
    }

    @Test
    public void testMapPutWithTwoTxn() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.begin();
        txnMap.put("1", "value");
        txnMap.commit();
        txnMap2.begin();
        txnMap2.put("1", "value2");
        txnMap2.commit();
    }

    @Test
    public void testMapRemoveWithTwoTxn() {
        Hazelcast.getMap("testMap").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.begin();
        txnMap.remove("1");
        txnMap.commit();
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
    }

    @Test
    public void testTryLock() {
        Hazelcast.getMap("testMap").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.lock("1");
        long start = System.currentTimeMillis();
        assertFalse(txnMap2.tryLock("1", 2, TimeUnit.SECONDS));
        long end = System.currentTimeMillis();
        long took = (end - start);
        assertTrue((took > 1000) ? (took < 4000) : false);
        assertFalse(txnMap2.tryLock("1"));
        txnMap.unlock("1");
        assertTrue(txnMap2.tryLock("1", 2, TimeUnit.SECONDS));
    }

    @Test
    public void testMapRemoveRollback() {
        Hazelcast.getMap("testMap").put("1", "value");
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.begin();
        assertEquals(1, txnMap.size());
        txnMap.remove("1");
        assertEquals(0, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap.rollback();
        assertEquals(1, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
        assertEquals(0, txnMap.size());
        assertEquals(0, txnMap2.size());
    }

    @Test
    public void testMapRemoveWithTwoTxn2() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.begin();
        txnMap.remove("1");
        txnMap.commit();
        txnMap2.begin();
        txnMap2.remove("1");
        txnMap2.commit();
    }

    @Test
    public void testMapRemoveWithTwoTxn3() {
        TransactionalMap txnMap = newTransactionalMapProxy("testMap");
        TransactionalMap txnMap2 = newTransactionalMapProxy("testMap");
        txnMap.put("1", "value1");
        assertEquals(1, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap.begin();
        txnMap.remove("1");
        assertEquals(0, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap.commit();
        assertEquals(0, txnMap.size());
        assertEquals(0, txnMap2.size());
        txnMap.put("1", "value1");
        assertEquals(1, txnMap.size());
        assertEquals(1, txnMap2.size());
        txnMap2.begin();
        txnMap2.remove("1");
        assertEquals(1, txnMap.size());
        assertEquals(0, txnMap2.size());
        txnMap2.commit();
        assertEquals(0, txnMap.size());
        assertEquals(0, txnMap2.size());
    }

    @Test
    public void testQueueOfferCommitSize() {
        TransactionalQueue txnq = newTransactionalQueueProxy("test");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("test");
        txnq.begin();
        txnq.offer("item");
        assertEquals(1, txnq.size());
        assertEquals(0, txnq2.size());
        txnq.commit();
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
    }

    @Test
    public void testQueueOfferRollbackSize() {
        TransactionalQueue txnq = newTransactionalQueueProxy("test");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("test");
        txnq.begin();
        txnq.offer("item");
        assertEquals(1, txnq.size());
        assertEquals(0, txnq2.size());
        txnq.rollback();
        assertEquals(0, txnq.size());
        assertEquals(0, txnq2.size());
    }

    @Test
    public void testQueueOfferCommitIterator() {
        TransactionalQueue txnq = newTransactionalQueueProxy("test");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("test");
        assertEquals(0, txnq.size());
        assertEquals(0, txnq2.size());
        txnq.begin();
        txnq.offer("item");
        Iterator it = txnq.iterator();
        int size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(0, size);

        txnq.commit();
        it = txnq.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);

        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
    }

    @Test
    public void testQueueOfferCommitIterator2() {
        TransactionalQueue txnq = newTransactionalQueueProxy("test");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("test");
        txnq.offer("item0");
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
        txnq.begin();
        txnq.offer("item");
        Iterator it = txnq.iterator();
        int size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(2, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);

        txnq.commit();
        it = txnq.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(2, size);

        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(2, size);
        assertEquals(2, txnq.size());
        assertEquals(2, txnq2.size());
    }


    @Test
    public void testQueueOfferRollbackIterator2() {
        TransactionalQueue txnq = newTransactionalQueueProxy("test");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("test");
        txnq.offer("item0");
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
        txnq.begin();
        txnq.offer("item");
        Iterator it = txnq.iterator();
        int size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(2, size);
        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);

        txnq.rollback();

        it = txnq.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);

        it = txnq2.iterator();
        size = 0;
        while (it.hasNext()) {
            assertNotNull(it.next());
            size++;
        }
        assertEquals(1, size);
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
    }

    @Test
    public void testQueuePollCommitSize() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testPoll");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testPoll");
        txnq.offer("item1");
        txnq.offer("item2");

        assertEquals(2, txnq.size());
        assertEquals(2, txnq2.size());

        txnq.begin();

        assertEquals("item1", txnq.poll());

        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());

        txnq.commit();
        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());
    }

    @Test
    public void testQueuePollRollbackSize() {
        TransactionalQueue txnq = newTransactionalQueueProxy("testPoll");
        TransactionalQueue txnq2 = newTransactionalQueueProxy("testPoll");
        txnq.offer("item1");
        txnq.offer("item2");

        assertEquals(2, txnq.size());
        assertEquals(2, txnq2.size());

        txnq.begin();

        assertEquals("item1", txnq.poll());

        assertEquals(1, txnq.size());
        assertEquals(1, txnq2.size());

        txnq.rollback();
        assertEquals(2, txnq.size());
        assertEquals(2, txnq2.size());
    }


    @After
    public void cleanUp() {
        Iterator<Instance> it = mapsUsed.iterator();
        while (it.hasNext()) {
            Instance instance = it.next();
            instance.destroy();
        }
        mapsUsed.clear();
    }

    List<Instance> mapsUsed = new CopyOnWriteArrayList<Instance>();

    TransactionalMap newTransactionalMapProxy(String name) {
        IMap imap = Hazelcast.getMap(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{TransactionalMap.class};
        Object proxy = Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(imap));
        TransactionalMap txnalMap = (TransactionalMap) proxy;
        mapsUsed.add(txnalMap);
        return txnalMap;
    }

    TransactionalQueue newTransactionalQueueProxy(String name) {
        IQueue q = Hazelcast.getQueue(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{TransactionalQueue.class};
        Object proxy = Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(q));
        TransactionalQueue txnalQ = (TransactionalQueue) proxy;
        mapsUsed.add(txnalQ);
        return txnalQ;
    }

    TransactionalSet newTransactionalSetProxy(String name) {
        ISet s = Hazelcast.getSet(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{TransactionalSet.class};
        Object proxy = Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(s));
        TransactionalSet txnSet = (TransactionalSet) proxy;
        mapsUsed.add(txnSet);
        return txnSet;
    }

    IMap newMapProxy(String name) {
        IMap imap = Hazelcast.getMap(name);
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        Class[] interfaces = new Class[]{IMap.class};
        IMap proxy = (IMap) Proxy.newProxyInstance(classLoader, interfaces, new ThreadBoundInvocationHandler(imap));
        mapsUsed.add(proxy);
        return proxy;
    }

    interface TransactionalMap extends IMap {
        void begin();

        void commit();

        void rollback();
    }

    interface TransactionalQueue extends IQueue {
        void begin();

        void commit();

        void rollback();
    }

    interface TransactionalSet extends ISet {
        void begin();

        void commit();

        void rollback();
    }


    public static class ThreadBoundInvocationHandler implements InvocationHandler {
        final Object target;
        final ExecutorService es = Executors.newSingleThreadExecutor();
        final static Object NULL_OBJECT = new Object();

        public ThreadBoundInvocationHandler(Object target) {
            this.target = target;
        }

        public Object invoke(final Object o, final Method method, final Object[] objects) throws Throwable {
            final String name = method.getName();
            final BlockingQueue resultQ = new ArrayBlockingQueue(1);
            if (name.equals("begin") || name.equals("commit") || name.equals("rollback")) {
                es.execute(new Runnable() {
                    public void run() {
                        try {
                            Transaction txn = Hazelcast.getTransaction();
                            if (name.equals("begin")) {
                                txn.begin();
                            } else if (name.equals("commit")) {
                                txn.commit();
                            } else if (name.equals("rollback")) {
                                txn.rollback();
                            }
                            resultQ.put(NULL_OBJECT);
                        } catch (Exception e) {
                            try {
                                resultQ.put(e);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                });
            } else {
                es.execute(new Runnable() {
                    public void run() {
                        try {
                            Object result = method.invoke(target, objects);
                            resultQ.put((result == null) ? NULL_OBJECT : result);
                        } catch (Exception e) {
                            try {
                                resultQ.put(e);
                            } catch (InterruptedException ignored) {
                            }
                        }

                    }
                });
            }

            Object result = resultQ.poll(5, TimeUnit.SECONDS);
            if (result == null) throw new RuntimeException("Method [" + name + "] took more than 5 seconds!");
            if (name.equals("destroy")) {
                es.shutdown();
            }
            if (result instanceof Throwable) {
                throw ((Throwable) result);
            }
            return (result == NULL_OBJECT) ? null : result;

        }
    }


}
