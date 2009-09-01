package com.hazelcast.core;

import static junit.framework.Assert.*;
import static org.junit.Assert.assertEquals;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;

public class HazelcastTest {
    @Test
    @Ignore
    public void testGetInstances() {
        /**@todo need to rethink this test so that it runs in isolation*/
        Hazelcast.getList("A");
        Hazelcast.getMap("A");
        Hazelcast.getMultiMap("A");
        Hazelcast.getQueue("A");
        Hazelcast.getSet("A");
        Hazelcast.getTopic("A");
        Collection<Instance> caches = Hazelcast.getInstances();
        assertEquals(6, caches.size());
    }

    @Test
    public void testGetCluster() {
        Cluster cluster = Hazelcast.getCluster();
        Set<Member> members = cluster.getMembers();
        //Tests are run with only one member in the cluster, this may change later
        assertEquals(1, members.size());
    }

    @Test
    public void testMapGetName() {
        IMap<String, String> map = Hazelcast.getMap("testMapGetName");
        assertEquals("testMapGetName", map.getName());
    }

    @Test
    public void testMapValuesSize() {
        Map<String, String> map = Hazelcast.getMap("testMapValuesSize");
        map.put("Hello", "World");
        assertEquals(1, map.values().size());
    }

    @Test
    public void testMapPutAndGet() {
        IMap<String, String> map = Hazelcast.getMap("testMapPutAndGet");
        String value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertNull(value);

        value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertEquals("World", value);

        value = map.put("Hello", "New World");
        assertEquals("New World", map.get("Hello"));
        assertEquals(1, map.size());
        assertEquals("World", value);
    }

    @Test
    public void testMapContainsKey() {
        IMap<String, String> map = Hazelcast.getMap("testMapContainsKey");
        map.put("Hello", "World");
        assertTrue(map.containsKey("Hello"));
    }

    @Test
    public void testMapContainsValue() {
        IMap<String, String> map = Hazelcast.getMap("testMapContainsValue");
        map.put("Hello", "World");
        assertTrue(map.containsValue("World"));
    }

    @Test
    public void testMapClear() {
        IMap<String, String> map = Hazelcast.getMap("testMapClear");
        String value = map.put("Hello", "World");
        assertEquals(null, value);
        map.clear();
        assertEquals(0, map.size());

        value = map.put("Hello", "World");
        assertEquals(null, value);
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());

        map.remove("Hello");
        assertEquals(0, map.size());
    }

    @Test
    public void testMapRemove() {
        IMap<String, String> map = Hazelcast.getMap("testMapRemove");
        map.put("Hello", "World");
        map.remove("Hello");
        assertEquals(0, map.size());
    }

    @Test
    public void testMapPutAll() {
        IMap<String, String> map = Hazelcast.getMap("testMapPutAll");
        Map<String, String> m = new HashMap<String, String>();
        m.put("Hello", "World");
        m.put("hazel", "cast");
        map.putAll(m);
        assertEquals(2, map.size());
        assertTrue(map.containsKey("Hello"));
        assertTrue(map.containsKey("hazel"));
    }

    @Test
    public void testMapEntrySet() {
        IMap<String, String> map = Hazelcast.getMap("testMapEntrySet");
        map.put("Hello", "World");
        Set<IMap.Entry<String, String>> set = map.entrySet();
        for (IMap.Entry<String, String> e : set) {
            assertEquals("Hello", e.getKey());
            assertEquals("World", e.getValue());
        }
    }

    @Test
    public void testMapEntryListener() {
        IMap<String, String> map = Hazelcast.getMap("testMapEntrySet");
        map.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent event) {
                assertEquals("world", event.getValue());
                assertEquals("hello", event.getKey());
            }

            public void entryRemoved(EntryEvent event) {
                assertEquals("hello", event.getKey());
                assertEquals("new world", event.getValue());
            }

            public void entryUpdated(EntryEvent event) {
                assertEquals("new world", event.getValue());
                assertEquals("hello", event.getKey());
            }

            public void entryEvicted(EntryEvent event) {
                entryRemoved (event);
            }
        }, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
    }


    @Test
    public void testListAdd() {
        IList<String> list = Hazelcast.getList("testListAdd");
        list.add("Hello World");
        assertEquals(1, list.size());
        assertEquals("Hello World", list.iterator().next());
    }

    @Test
    public void testListContains() {
        IList<String> list = Hazelcast.getList("testListContains");
        list.add("Hello World");
        assertTrue(list.contains("Hello World"));
    }

    @Test
    public void testListGet() {
        // Unsupported
        //IList<String> list = Hazelcast.getList("testListGet");
        //list.add("Hello World");
        //assertEquals("Hello World", list.get(0));
    }

    @Test
    public void testListIterator() {
        IList<String> list = Hazelcast.getList("testListIterator");
        list.add("Hello World");
        assertEquals("Hello World", list.iterator().next());
    }

    @Test
    public void testListListIterator() {
        // Unsupported
        //IList<String> list = Hazelcast.getList("testListListIterator");
        //list.add("Hello World");
        //assertEquals("Hello World", list.listIterator().next());
    }

    @Test
    public void testListIndexOf() {
        // Unsupported
        //IList<String> list = Hazelcast.getList("testListIndexOf");
        //list.add("Hello World");
        //assertEquals(0, list.indexOf("Hello World"));
    }

    @Test
    public void testListIsEmpty() {
        IList<String> list = Hazelcast.getList("testListIsEmpty");
        assertTrue(list.isEmpty());
        list.add("Hello World");
        assertFalse(list.isEmpty());
    }

    @Test
    public void testSetAdd() {
        ISet<String> set = Hazelcast.getSet("testSetAdd");
        boolean added = set.add("HelloWorld");
        assertEquals(true, added);
        added = set.add("HelloWorld");
        assertFalse(added);
        assertEquals(1, set.size());
    }

    @Test
    public void testSetIterator() {
        ISet<String> set = Hazelcast.getSet("testSetIterator");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        assertEquals("HelloWorld", set.iterator().next());
    }

    @Test
    public void testSetContains() {
        ISet<String> set = Hazelcast.getSet("testSetContains");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        boolean contains = set.contains("HelloWorld");
        assertTrue(contains);
    }

    @Test
    public void testSetClear() {
        ISet<String> set = Hazelcast.getSet("testSetClear");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        set.clear();
        assertEquals(0, set.size());
    }

    @Test
    public void testSetRemove() {
        ISet<String> set = Hazelcast.getSet("testSetRemove");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        set.remove("HelloWorld");
        assertEquals(0, set.size());
    }

    @Test
    public void testSetGetName() {
        ISet<String> set = Hazelcast.getSet("testSetGetName");
        assertEquals("testSetGetName", set.getName());
    }

    @Test
    public void testSetAddAll() {
        ISet<String> set = Hazelcast.getSet("testSetAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        set.addAll(Arrays.asList(items));
        assertEquals(4, set.size());

        items = new String[]{"four", "five"};
        set.addAll(Arrays.asList(items));
        assertEquals(5, set.size());
    }

    @Test
    public void testTopicGetName() {
        ITopic<String> topic = Hazelcast.getTopic("testTopicGetName");
        assertEquals("testTopicGetName", topic.getName());
    }

    @Test
    public void testTopicPublish() {
        ITopic<String> topic = Hazelcast.getTopic("testTopicPublish");
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(String msg) {
                /*@todo Exceptions on failure are not correctly propagated and test is marked as passing, despite
                * reporting the Exception*/
                assertEquals("Hello World", msg);
            }
        });
        topic.publish("Hello World");
    }

    @Test
    public void testQueueAdd() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueAdd");
        queue.add("Hello World");
        assertEquals(1, queue.size());
    }

    @Test
    public void testQueueAddAll() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertEquals(4, queue.size());
        queue.addAll(Arrays.asList(items));
        assertEquals(8, queue.size());
    }

    @Test
    public void testQueueContains() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueContains");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertTrue(queue.contains("one"));
        assertTrue(queue.contains("two"));
        assertTrue(queue.contains("three"));
        assertTrue(queue.contains("four"));
    }

    @Test
    public void testQueueContainsAll() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueContainsAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        List<String> list = Arrays.asList(items);
        queue.addAll(list);
        assertTrue(queue.containsAll(list));
    }

    @Test
    public void testMultiMapPutAndGet() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapPutAndGet");
        map.put("Hello", "World");
        Collection<String> values = map.get("Hello");
        assertEquals("World", values.iterator().next());

        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");

        values = map.get("Hello");
        assertEquals(7, values.size());
    }

    @Test
    public void testMultiMapGetNameAndType() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapGetNameAndType");
        assertEquals("testMultiMapGetNameAndType", map.getName());
        Instance.InstanceType type = map.getInstanceType();
        assertEquals(Instance.InstanceType.MULTIMAP, type);
    }

    @Test
    public void testMultiMapClear() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapClear");
        map.put("Hello", "World");
        assertEquals(1, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    public void testMultiMapContainsKey() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapContainsKey");
        map.put("Hello", "World");
        assertTrue(map.containsKey("Hello"));
    }

    @Test
    public void testMultiMapContainsValue() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapContainsValue");
        map.put("Hello", "World");
        assertTrue(map.containsValue("World"));
    }

    @Test
    public void testMultiMapContainsEntry() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapContainsEntry");
        map.put("Hello", "World");
        assertTrue(map.containsEntry("Hello", "World"));
    }

    @Test
    public void testMultiMapKeySet() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapKeySet");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");

        Set<String> keys = map.keySet();
        assertEquals(1, keys.size());
    }

    @Test
    public void testMultiMapValues() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapValues");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");

        Collection<String> values = map.values();
        assertEquals(7, values.size());
    }

    @Test
    public void testMultiMapRemove() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapRemove");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        Collection<String> values = map.remove("Hello");
        assertEquals(7, values.size());
        assertEquals(0, map.size());
    }

    @Test
    public void testMultiMapRemoveEntries() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapRemoveEntries");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        boolean removed = map.remove("Hello", "World");
        assertTrue(removed);
        assertEquals(6, map.size());
    }

    @Test
    public void testMultiMapEntrySet() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapEntrySet");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        Set<Map.Entry<String, String>> entries = map.entrySet();
        assertEquals(7, entries.size());
        for (Map.Entry<String, String> entry : entries) {
            assertEquals("Hello", entry.getKey());
        }
    }

    @Test
    public void testMultiMapValueCount() {
        MultiMap<Integer, String> map = Hazelcast.getMultiMap("testMultiMapValueCount");
        map.put(1, "World");
        map.put(2, "Africa");
        map.put(1, "America");
        map.put(2, "Antartica");
        map.put(1, "Asia");
        map.put(1, "Europe");
        map.put(2, "Australia");

        assertEquals(4, map.valueCount(1));
        assertEquals(3, map.valueCount(2));
    }

    @Test
    public void testIdGenerator() {
        IdGenerator id = Hazelcast.getIdGenerator("testIdGenerator");
        assertEquals(1, id.newId());
        assertEquals(2, id.newId());
        assertEquals("testIdGenerator", id.getName());
    }

    @Test
    public void testLock() {
        ILock lock = Hazelcast.getLock("testLock");
        assertTrue(lock.tryLock());
        lock.unlock();
    }

    @Test
    public void testGetMapEntryHits() {
        IMap<String, String> map = Hazelcast.getMap("testGetMapEntryHits");
        map.put("Hello", "World");
        MapEntry me = map.getMapEntry("Hello");
        assertEquals(0, me.getHits());
        map.get("Hello");
        map.get("Hello");
        map.get("Hello");
        me = map.getMapEntry("Hello");
        assertEquals(3, me.getHits());
    }

    @Test
    public void testGetMapEntryVersion() {
        IMap<String, String> map = Hazelcast.getMap("testGetMapEntryVersion");
        map.put("Hello", "World");
        MapEntry me = map.getMapEntry("Hello");
        assertEquals(0, me.getVersion());
        map.put("Hello", "1");
        map.put("Hello", "2");
        map.put("Hello", "3");
        me = map.getMapEntry("Hello");
        assertEquals(3, me.getVersion());
    }

    @Test
    public void testMapInstanceDestroy() {
        IMap<String, String> map = Hazelcast.getMap("testMap");
        Collection<Instance> instances = Hazelcast.getInstances();
        boolean found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.MAP) {
                IMap imap = (IMap) instance;
                if (imap.getName().equals("testMap")) {
                    found = true;
                }
            }
        }
        assertTrue(found);
        map.destroy();
        found = false;
        instances = Hazelcast.getInstances();
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.MAP) {
                IMap imap = (IMap) instance;
                if (imap.getName().equals("testMap")) {
                    found = true;
                }
            }
        }
        assertFalse(found);
    }

    @Test
    public void testLockInstance() {
        ILock lock = Hazelcast.getLock("testLock");
        lock.lock();
        Collection<Instance> instances = Hazelcast.getInstances();
        boolean found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.LOCK) {
                ILock lockInstance = (ILock) instance;
                if (lockInstance.getLockObject().equals("testLock")) {
                    found = true;
                }
            }
        }
        assertTrue(found);

        instances = Hazelcast.getInstances();
        found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.LOCK) {
                ILock lockInstance = (ILock) instance;
                if (lockInstance.getLockObject().equals("testLock2")) {
                    found = true;
                }
            }
        }
        assertFalse(found);

        Hazelcast.getLock("testLock2");
        instances = Hazelcast.getInstances();
        found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.LOCK) {
                ILock lockInstance = (ILock) instance;
                if (lockInstance.getLockObject().equals("testLock2")) {
                    found = true;
                }
            }
        }
        assertTrue(found);
    }
}