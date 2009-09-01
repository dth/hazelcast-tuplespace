/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.hazelcast.impl;

import com.hazelcast.cluster.ClusterImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.*;
import com.hazelcast.impl.BaseManager.Processable;
import com.hazelcast.impl.BlockingQueueManager.Offer;
import com.hazelcast.impl.BlockingQueueManager.Poll;
import com.hazelcast.impl.BlockingQueueManager.QIterator;
import com.hazelcast.impl.ConcurrentMapManager.*;
import com.hazelcast.jmx.ManagementService;
import static com.hazelcast.nio.BufferUtil.*;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.space.TupleSpace;
import com.hazelcast.space.TupleSpaceImpl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FactoryImpl implements HazelcastInstance {

    private final Logger logger = Logger.getLogger(FactoryImpl.class.getName());

    private final ConcurrentMap<String, Instance> proxiesByName = new ConcurrentHashMap<String, Instance>(1000);

    private final ConcurrentMap<ProxyKey, Instance> proxies = new ConcurrentHashMap<ProxyKey, Instance>(1000);

    private final MProxy locksMapProxy;

    private final MProxy idGeneratorMapProxy;

    private final MProxy globalProxies;

    private final ExecutorServiceProxy executorServiceImpl;

    private final TupleSpace tupleSpaceImpl;

    private final CopyOnWriteArrayList<InstanceListener> lsInstanceListeners = new CopyOnWriteArrayList<InstanceListener>();

    private final static ConcurrentMap<String, FactoryImpl> factories = new ConcurrentHashMap<String, FactoryImpl>(5);

    private final String name;

    private final TransactionFactory transactionFactory;

    private int startCount = 0;

    public final Node node;

    volatile boolean inited = false;

    public static FactoryImpl getFactory(String name) {
        FactoryImpl factory = factories.get(name);
        if (factory == null) {
            synchronized (FactoryImpl.class) {
                factory = factories.get(name);
                if (factory == null) {
                    factory = new FactoryImpl(name, Config.get());
                    factories.put(name, factory);
                }
            }
        }
        return factory;
    }

    public FactoryImpl(String name, Config config) {
        this.name = name;
        node = new Node(this, config);
        executorServiceImpl = new ExecutorServiceProxy(node);
        transactionFactory = new TransactionFactory(this);
        node.start();
        locksMapProxy = new MProxyImpl("c:__hz_Locks", this);
        idGeneratorMapProxy = new MProxyImpl("c:__hz_IdGenerator", this);
        globalProxies = new MProxyImpl("c:__hz_Proxies", this);
        tupleSpaceImpl = new TupleSpaceImpl(this);
        inited = true;
        startCount++;
        ManagementService.register(node.getClusterImpl());
        globalProxies.addEntryListener(new EntryListener() {
            public void entryAdded(EntryEvent event) {
                final ProxyKey proxyKey = (ProxyKey) event.getKey();
                if (!proxies.containsKey(proxyKey)) {
                    logger.log(Level.FINEST, "Instance created " + proxyKey);
                    node.clusterService.enqueueAndReturn(new Processable() {
                        public void process() {
                            createProxy(proxyKey);
                        }
                    });
                }
            }

            public void entryRemoved(EntryEvent event) {
                final ProxyKey proxyKey = (ProxyKey) event.getKey();
                if (proxies.containsKey(proxyKey)) {
                    logger.log(Level.FINEST, "Instance removed " + proxyKey);
                    node.clusterService.enqueueAndReturn(new Processable() {
                        public void process() {
                            destroyProxy(proxyKey);
                        }
                    });
                }
            }

            public void entryUpdated(EntryEvent event) {
                logger.log(Level.FINEST, "Instance updated " + event.getKey());
            }

            public void entryEvicted(EntryEvent event) {
                // should not happen!
                logger.log(Level.FINEST, "Instance evicted " + event.getKey());
            }
        }, false);
        if (node.getClusterImpl().getMembers().size() > 1) {
            Set<ProxyKey> proxyKeys = globalProxies.allKeys();
            for (final ProxyKey proxyKey : proxyKeys) {
                if (!proxies.containsKey(proxyKey)) {
                    node.clusterService.enqueueAndReturn(new Processable() {
                        public void process() {
                            createProxy(proxyKey);
                        }
                    });
                }
            }
        }
    }

    public String getName() {
        return name;
    }

    public void shutdown() {
        ManagementService.shutdown();
        node.shutdown();
    }

    public Collection<Instance> getInstances() {
        final int totalSize = proxies.size();
        List<Instance> lsProxies = new ArrayList<Instance>(totalSize);
        lsProxies.addAll(proxies.values());
        return lsProxies;
    }

    public Collection<Instance> getProxies() {
        initialChecks();
        return proxies.values();
    }

    public ExecutorService getExecutorService() {
        initialChecks();
        return executorServiceImpl;
    }

    public ClusterImpl getCluster() {
        initialChecks();
        return node.getClusterImpl();
    }

    public IdGenerator getIdGenerator(String name) {
        return (IdGenerator) getProxyByName("i:" + name);
    }

    public Transaction getTransaction() {
        initialChecks();
        ThreadContext threadContext = ThreadContext.get();
        TransactionImpl txn = threadContext.txn;
        if (txn == null) {
            txn = transactionFactory.newTransaction();
            threadContext.setTransaction(txn);
        }
        return txn;
    }

    public <K, V> IMap<K, V> getMap(String name) {
        name = "c:" + name;
        return (IMap<K, V>) getProxyByName(name);
    }

    public <E> IQueue<E> getQueue(String name) {
        name = "q:" + name;
        return (IQueue) getProxyByName(name);
    }

    public <E> ITopic<E> getTopic(String name) {
        name = "t:" + name;
        return (ITopic) getProxyByName(name);
    }

    public <E> ISet<E> getSet(String name) {
        name = "m:s:" + name;
        return (ISet) getProxyByName(name);
    }

    public <E> IList<E> getList(String name) {
        name = "m:l:" + name;
        return (IList) getProxyByName(name);
    }

    public <K, V> MultiMap<K, V> getMultiMap(String name) {
        name = "m:u:" + name;
        return (MultiMap<K, V>) getProxyByName(name);
    }

    public ILock getLock(Object key) {
        return (ILock) getProxy(new ProxyKey("lock", key));
    }

    public Object getProxyByName(final String name) {
        Object proxy = proxiesByName.get(name);
        if (proxy == null) {
            proxy = getProxy(new ProxyKey(name, null));
        }
        return proxy;
    }

    public Object getProxy(final ProxyKey proxyKey) {
        initialChecks();
        Object proxy = proxies.get(proxyKey);
        if (proxy == null) {
            proxy = createInstanceClusterwide(proxyKey);
        }
        return proxy;
    }

    public static void initialChecks() {

    }

    public void destroyProxy(final ProxyKey proxyKey) {
        proxiesByName.remove(proxyKey.name);
        Instance proxy = proxies.remove(proxyKey);
        if (proxy != null) {
            String name = proxyKey.name;
            if (name.startsWith("q:")) {
                node.blockingQueueManager.destroy(name);
            } else if (name.startsWith("c:")) {
                node.concurrentMapManager.destroy(name);
            } else if (name.startsWith("m:")) {
                node.concurrentMapManager.destroy(name);
            } else if (name.startsWith("t:")) {
                node.topicManager.destroy(name);
            }
            fireInstanceDestroyEvent(proxy);
        }
    }

    // should only be called from service thread!!
    public Object createProxy(ProxyKey proxyKey) {
        boolean created = false;
        Instance proxy = proxies.get(proxyKey);
        if (proxy == null) {
            created = true;
            String name = proxyKey.name;
            if (name.startsWith("q:")) {
                proxy = new QProxyImpl(name, this);
            } else if (name.startsWith("t:")) {
                proxy = new TopicProxyImpl(name, this);
            } else if (name.startsWith("c:")) {
                proxy = new MProxyImpl(name, this);
            } else if (name.startsWith("m:")) {
                if (BaseManager.getInstanceType(name) == Instance.InstanceType.MULTIMAP) {
                    proxy = new MultiMapProxy(name, this);
                } else {
                    proxy = new CollectionProxyImpl(name, this);
                }
            } else if (name.startsWith("i:")) {
                proxy = new IdGeneratorProxy(name, this);
            } else if (name.equals("lock")) {
                proxy = new LockProxy(this, proxyKey.key);
            }
            proxies.put(proxyKey, proxy);
            if (proxyKey.key == null) {
                proxiesByName.put(proxyKey.name, proxy);
            }
        }
        if (created) {
            fireInstanceCreateEvent(proxy);
        }
        return proxy;
    }

    public void addInstanceListener(InstanceListener instanceListener) {
        lsInstanceListeners.add(instanceListener);
    }

    public void removeInstanceListener(InstanceListener instanceListener) {
        lsInstanceListeners.remove(instanceListener);
    }

    void fireInstanceCreateEvent(Instance instance) {
        if (lsInstanceListeners.size() > 0) {
            final InstanceEvent instanceEvent = new InstanceEvent(InstanceEvent.InstanceEventType.CREATED, instance);
            for (final InstanceListener instanceListener : lsInstanceListeners) {
                node.executorManager.executeLocaly(new Runnable() {
                    public void run() {
                        instanceListener.instanceCreated(instanceEvent);
                    }
                });
            }
        }
    }

    void fireInstanceDestroyEvent(Instance instance) {
        if (lsInstanceListeners.size() > 0) {
            final InstanceEvent instanceEvent = new InstanceEvent(InstanceEvent.InstanceEventType.DESTROYED, instance);
            for (final InstanceListener instanceListener : lsInstanceListeners) {
                node.executorManager.executeLocaly(new Runnable() {
                    public void run() {
                        instanceListener.instanceDestroyed(instanceEvent);
                    }
                });
            }
        }
    }

    public TupleSpace getTupleSpace() {
        initialChecks();
        return tupleSpaceImpl;
    }


    public static class LockProxy implements ILock, DataSerializable {

        private Object key = null;
        private transient ILock base = null;
        private transient FactoryImpl factory = null;

        public LockProxy() {
        }

        public LockProxy(FactoryImpl factory, Object key) {
            super();
            this.key = key;
            base = new LockProxyBase();
            this.factory = factory;
        }

        private void ensure() {
            initialChecks();
            if (base == null) {
                base = factory.getLock(key);
            }
        }

        @Override
        public String toString() {
            return "ILock [" + key + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LockProxy lockProxy = (LockProxy) o;

            return !(key != null ? !key.equals(lockProxy.key) : lockProxy.key != null);

        }

        @Override
        public int hashCode() {
            return key != null ? key.hashCode() : 0;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(factory.getName());
            Data data = doHardCopy(toData(key));
            data.writeData(out);
        }

        public void readData(DataInput in) throws IOException {
            Data data = new Data();
            data.readData(in);
            key = toObject(data);
        }

        public void lock() {
            ensure();
            base.lock();
        }

        public void lockInterruptibly() throws InterruptedException {
            ensure();
            base.lockInterruptibly();
        }

        public boolean tryLock() {
            ensure();
            return base.tryLock();
        }

        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            ensure();
            return base.tryLock(time, unit);
        }

        public void unlock() {
            ensure();
            base.unlock();
        }

        public Condition newCondition() {
            ensure();
            return base.newCondition();
        }

        public InstanceType getInstanceType() {
            ensure();
            return InstanceType.LOCK;
        }

        public void destroy() {
            ensure();
            base.destroy();
        }

        public Object getLockObject() {
            return key;
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        private class LockProxyBase implements ILock {
            public void lock() {
                factory.locksMapProxy.lock(key);
            }

            public void lockInterruptibly() throws InterruptedException {
            }

            public Condition newCondition() {
                return null;
            }

            public boolean tryLock() {
                return factory.locksMapProxy.tryLock(key);
            }

            public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
                return factory.locksMapProxy.tryLock(key, time, unit);
            }

            public void unlock() {
                factory.locksMapProxy.unlock(key);
            }

            public void destroy() {
                factory.destroyInstanceClusterwide("lock", key);
            }

            public InstanceType getInstanceType() {
                return InstanceType.LOCK;
            }

            public Object getLockObject() {
                return key;
            }

            public Object getId() {
                return new ProxyKey("lock", key);
            }
        }
    }

    Object createInstanceClusterwide(final ProxyKey proxyKey) {
        final BlockingQueue result = new ArrayBlockingQueue(1);
        node.clusterService.enqueueAndReturn(new Processable() {
            public void process() {
                try {
                    result.put(createProxy(proxyKey));
                } catch (InterruptedException e) {
                }
            }
        });
        Object proxy = null;
        try {
            proxy = result.take();
        } catch (InterruptedException e) {
        }
        globalProxies.put(proxyKey, Constants.IO.EMPTY_DATA);
        return proxy;
    }

    void destroyInstanceClusterwide(String name, Object key) {
        final ProxyKey proxyKey = new ProxyKey(name, key);
        if (proxies.containsKey(proxyKey)) {
            if (name.equals("lock")) {
                locksMapProxy.remove(key);
            } else if (name.startsWith("i:")) {
                idGeneratorMapProxy.remove(name);
            }
            globalProxies.remove(proxyKey);

            final BlockingQueue result = new ArrayBlockingQueue(1);
            node.clusterService.enqueueAndReturn(new Processable() {
                public void process() {
                    try {
                        destroyProxy(proxyKey);
                        result.put(Boolean.TRUE);
                    } catch (Exception e) {
                    }
                }
            });
            try {
                result.take();
            } catch (InterruptedException e) {
            }
        }
    }


    public static class ProxyKey implements Serializable {
        String name;
        Object key;

        public ProxyKey() {
        }

        public ProxyKey(String name, Object key) {
            this.name = name;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ProxyKey proxyKey = (ProxyKey) o;

            if (name != null ? !name.equals(proxyKey.name) : proxyKey.name != null) return false;
            return !(key != null ? !key.equals(proxyKey.key) : proxyKey.key != null);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("ProxyKey");
            sb.append("{name='").append(name).append('\'');
            sb.append(", key=").append(key);
            sb.append('}');
            return sb.toString();
        }
    }

    interface TopicProxy extends ITopic, Instance {

    }

    public static class TopicProxyImpl extends FactoryAwareNamedProxy implements TopicProxy, DataSerializable {
        private transient TopicProxy base = null;
        private TopicManager topicManager = null;
        private ListenerManager listenerManager = null;

        public TopicProxyImpl() {
        }

        public TopicProxyImpl(String name, FactoryImpl factory) {
            this.name = name;
            base = new TopicProxyReal();
            setFactory(factory);
        }

        private void ensure() {
            initialChecks();
            if (base == null) {
                base = (TopicProxy) factory.getProxyByName(name);
            }
        }

        public void setFactory(FactoryImpl factory) {
            this.factory = factory;
            topicManager = factory.node.topicManager;
            listenerManager = factory.node.listenerManager;
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            return "Topic [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TopicProxyImpl that = (TopicProxyImpl) o;

            return !(name != null ? !name.equals(that.name) : that.name != null);

        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public void publish(Object msg) {
            ensure();
            base.publish(msg);
        }

        public void addMessageListener(MessageListener listener) {
            ensure();
            base.addMessageListener(listener);
        }

        public void removeMessageListener(MessageListener listener) {
            ensure();
            base.removeMessageListener(listener);
        }

        public void destroy() {
            Instance instance = factory.proxies.remove(name);
            if (instance != null) {
                ensure();
                base.destroy();
            }
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        class TopicProxyReal implements TopicProxy {

            public void publish(Object msg) {
                topicManager.doPublish(name, msg);
            }

            public void addMessageListener(MessageListener listener) {
                listenerManager.addListener(name, listener, null, true,
                        ListenerManager.Type.Message);
            }

            public void removeMessageListener(MessageListener listener) {
                listenerManager.removeListener(name, listener, null);
            }

            public void destroy() {
                factory.destroyInstanceClusterwide(name, null);
            }

            public Instance.InstanceType getInstanceType() {
                return Instance.InstanceType.TOPIC;
            }

            public String getName() {
                return name.substring(2);
            }

            public Object getId() {
                return name;
            }
        }
    }

    interface CollectionProxy extends IRemoveAwareProxy, ISet, IList {

    }

    public static class CollectionProxyImpl extends BaseCollection implements CollectionProxy, DataSerializable {
        String name = null;
        private transient CollectionProxy base = null;
        private transient FactoryImpl factory = null;

        public CollectionProxyImpl() {
        }

        public CollectionProxyImpl(String name, FactoryImpl factory) {
            this.name = name;
            this.factory = factory;
            this.base = new CollectionProxyReal();
        }

        private void ensure() {
            initialChecks();
            if (base == null) {
                base = (CollectionProxy) factory.getProxyByName(name);
            }
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            ensure();
            if (getInstanceType() == InstanceType.SET) {
                return "Set [" + getName() + "]";
            } else {
                return "List [" + getName() + "]";
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CollectionProxyImpl that = (CollectionProxyImpl) o;

            return !(name != null ? !name.equals(that.name) : that.name != null);

        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public int size() {
            ensure();
            return base.size();
        }

        public boolean contains(Object o) {
            ensure();
            return base.contains(o);
        }

        public Iterator iterator() {
            ensure();
            return base.iterator();
        }

        public boolean add(Object o) {
            ensure();
            return base.add(o);
        }

        public boolean remove(Object o) {
            ensure();
            return base.remove(o);
        }

        public void clear() {
            ensure();
            base.clear();
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public void destroy() {
            factory.destroyInstanceClusterwide(name, null);
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(factory.getName());
            out.writeUTF(name);
        }

        public void readData(DataInput in) throws IOException {
            factory = getFactory(in.readUTF());
            name = in.readUTF();
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public void addItemListener(ItemListener itemListener, boolean includeValue) {
            ensure();
            base.addItemListener(itemListener, includeValue);
        }

        public void removeItemListener(ItemListener itemListener) {
            ensure();
            base.removeItemListener(itemListener);
        }

        public boolean removeKey(Object key) {
            ensure();
            return base.removeKey(key);
        }

        class CollectionProxyReal extends BaseCollection implements CollectionProxy {

            final MProxy mapProxy;

            public CollectionProxyReal() {
                mapProxy = new MProxyImpl(name, factory);
            }

            public Object getId() {
                return name;
            }

            @Override
            public boolean equals(Object o) {
                return CollectionProxyImpl.this.equals(o);
            }

            @Override
            public int hashCode() {
                return CollectionProxyImpl.this.hashCode();
            }

            public InstanceType getInstanceType() {
                return BaseManager.getInstanceType(name);
            }

            public void addItemListener(ItemListener listener, boolean includeValue) {
                mapProxy.addGenericListener(listener, null, includeValue,
                        ListenerManager.Type.Item);
            }

            public void removeItemListener(ItemListener listener) {
                mapProxy.removeGenericListener(listener, null);
            }

            public String getName() {
                return name.substring(4);
            }

            @Override
            public boolean add(Object obj) {
                return mapProxy.add(obj);
            }

            @Override
            public boolean remove(Object obj) {
                return mapProxy.removeKey(obj);
            }

            public boolean removeKey(Object obj) {
                return mapProxy.removeKey(obj);
            }

            @Override
            public boolean contains(Object obj) {
                return mapProxy.containsKey(obj);
            }

            @Override
            public Iterator iterator() {
                return mapProxy.keySet().iterator();
            }

            @Override
            public int size() {
                return mapProxy.size();
            }

            public MProxy getCProxy() {
                return mapProxy;
            }

            public void destroy() {
                factory.destroyInstanceClusterwide(name, null);
            }
        }
    }

    public static abstract class BaseCollection extends AbstractCollection implements List {

        public void add(int index, Object element) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(int index, Collection c) {
            throw new UnsupportedOperationException();
        }

        public Object get(int index) {
            throw new UnsupportedOperationException();
        }

        public int indexOf(Object o) {
            throw new UnsupportedOperationException();
        }

        public int lastIndexOf(Object o) {
            throw new UnsupportedOperationException();
        }

        public ListIterator listIterator() {
            throw new UnsupportedOperationException();
        }

        public ListIterator listIterator(int index) {
            throw new UnsupportedOperationException();
        }

        public Object remove(int index) {
            throw new UnsupportedOperationException();
        }

        public Object set(int index, Object element) {
            throw new UnsupportedOperationException();
        }

        public List subList(int fromIndex, int toIndex) {
            throw new UnsupportedOperationException();
        }
    }

    interface QProxy extends IQueue {

        boolean offer(Object obj);

        boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException;

        void put(Object obj) throws InterruptedException;

        Object peek();

        Object poll();

        Object poll(long timeout, TimeUnit unit) throws InterruptedException;

        Object take() throws InterruptedException;

        int remainingCapacity();

        Iterator iterator();

        int size();

        void addItemListener(ItemListener listener, boolean includeValue);

        void removeItemListener(ItemListener listener);

        String getName();

        boolean remove(Object obj);

        int drainTo(Collection c);

        int drainTo(Collection c, int maxElements);

        void destroy();

        InstanceType getInstanceType();
    }

    public static class QProxyImpl extends AbstractQueue implements QProxy, DataSerializable {
        private transient QProxy qproxyReal = null;
        private transient FactoryImpl factory = null;
        private String name = null;
        private BlockingQueueManager blockingQueueManager = null;
        private ListenerManager listenerManager = null;

        public QProxyImpl() {
        }

        private QProxyImpl(String name, FactoryImpl factory) {
            this.name = name;
            qproxyReal = new QProxyReal();
            setFactory(factory);
        }

        void setFactory(FactoryImpl factory) {
            this.factory = factory;
            this.blockingQueueManager = factory.node.blockingQueueManager;
            this.listenerManager = factory.node.listenerManager;
        }

        private void ensure() {
            initialChecks();
            if (qproxyReal == null) {
                qproxyReal = (QProxy) factory.getProxyByName(name);
            }
        }

        public Object getId() {
            ensure();
            return qproxyReal.getId();
        }

        @Override
        public String toString() {
            return "Queue [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            QProxyImpl qProxy = (QProxyImpl) o;

            return !(name != null ? !name.equals(qProxy.name) : qProxy.name != null);

        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(factory.getName());
            out.writeUTF(name);
        }

        public void readData(DataInput in) throws IOException {
            setFactory(getFactory(in.readUTF()));
            name = in.readUTF();
        }

        public Iterator iterator() {
            ensure();
            return qproxyReal.iterator();
        }

        public int size() {
            ensure();
            return qproxyReal.size();
        }

        public void addItemListener(ItemListener listener, boolean includeValue) {
            ensure();
            qproxyReal.addItemListener(listener, includeValue);
        }

        public void removeItemListener(ItemListener listener) {
            ensure();
            qproxyReal.removeItemListener(listener);
        }

        public String getName() {
            ensure();
            return qproxyReal.getName();
        }

        public int drainTo(Collection c) {
            ensure();
            return qproxyReal.drainTo(c);
        }

        public int drainTo(Collection c, int maxElements) {
            ensure();
            return qproxyReal.drainTo(c, maxElements);
        }

        public void destroy() {
            ensure();
            qproxyReal.destroy();
        }

        public InstanceType getInstanceType() {
            ensure();
            return qproxyReal.getInstanceType();
        }

        public boolean offer(Object o) {
            ensure();
            return qproxyReal.offer(o);
        }

        public boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException {
            ensure();
            return qproxyReal.offer(obj, timeout, unit);
        }

        public void put(Object obj) throws InterruptedException {
            ensure();
            qproxyReal.put(obj);
        }

        public Object poll() {
            ensure();
            return qproxyReal.poll();
        }

        public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
            ensure();
            return qproxyReal.poll(timeout, unit);
        }

        public Object take() throws InterruptedException {
            ensure();
            return qproxyReal.take();
        }

        public int remainingCapacity() {
            ensure();
            return qproxyReal.remainingCapacity();
        }

        public Object peek() {
            ensure();
            return qproxyReal.peek();
        }

        private class QProxyReal extends AbstractQueue implements QProxy {


            public QProxyReal() {
            }

            public boolean offer(Object obj) {
                Offer offer = blockingQueueManager.new Offer();
                return offer.offer(name, obj, 0);
            }

            public boolean offer(Object obj, long timeout, TimeUnit unit) throws InterruptedException {
                if (timeout < 0) {
                    timeout = 0;
                }
                Offer offer = blockingQueueManager.new Offer();
                return offer.offer(name, obj, unit.toMillis(timeout));
            }

            public void put(Object obj) throws InterruptedException {
                Offer offer = blockingQueueManager.new Offer();
                offer.offer(name, obj, -1);
            }

            public Object peek() {
                Poll poll = blockingQueueManager.new Poll();
                return poll.peek(name);
            }

            public Object poll() {
                Poll poll = blockingQueueManager.new Poll();
                return poll.poll(name, 0);
            }

            public Object poll(long timeout, TimeUnit unit) throws InterruptedException {
                if (timeout < 0) {
                    timeout = 0;
                }
                Poll poll = blockingQueueManager.new Poll();
                return poll.poll(name, unit.toMillis(timeout));
            }

            public Object take() throws InterruptedException {
                Poll poll = blockingQueueManager.new Poll();
                return poll.poll(name, -1);
            }

            public int remainingCapacity() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Iterator iterator() {
                QIterator iterator = blockingQueueManager.new QIterator();
                iterator.set(name);
                return iterator;
            }

            @Override
            public int size() {
                BlockingQueueManager.QSize qsize = blockingQueueManager.new QSize(name);
                return qsize.getSize();
            }

            public void addItemListener(ItemListener listener, boolean includeValue) {
                listenerManager.addListener(name, listener, null, includeValue,
                        ListenerManager.Type.Item);
            }

            public void removeItemListener(ItemListener listener) {
                listenerManager.removeListener(name, listener, null);
            }

            public String getName() {
                return name.substring(2);
            }

            @Override
            public boolean remove(Object obj) {
                throw new UnsupportedOperationException();
            }

            public int drainTo(Collection c) {
                throw new UnsupportedOperationException();
            }

            public int drainTo(Collection c, int maxElements) {
                throw new UnsupportedOperationException();
            }

            public void destroy() {
                factory.destroyInstanceClusterwide(name, null);
            }

            public Instance.InstanceType getInstanceType() {
                return Instance.InstanceType.QUEUE;
            }

            public Object getId() {
                return name;
            }
        }
    }

    public static class MultiMapProxy extends FactoryAwareNamedProxy implements MultiMap, DataSerializable, IGetAwareProxy {

        private transient MultiMapBase base = null;

        public MultiMapProxy() {
        }

        public MultiMapProxy(String name, FactoryImpl factory) {
            this.name = name;
            setFactory(factory);
            this.base = new MultiMapBase();
        }

        private void ensure() {
            initialChecks();
            if (base == null) {
                base = (MultiMapBase) factory.getProxyByName(name);
            }
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            return "MultiMap [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MultiMapProxy that = (MultiMapProxy) o;

            return !(name != null ? !name.equals(that.name) : that.name != null);

        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public void destroy() {
            Instance instance = factory.proxies.remove(name);
            if (instance != null) {
                ensure();
                base.destroy();
            }
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public boolean put(Object key, Object value) {
            ensure();
            return base.put(key, value);
        }

        public Collection get(Object key) {
            ensure();
            return base.get(key);
        }

        public boolean remove(Object key, Object value) {
            ensure();
            return base.remove(key, value);
        }

        public Collection remove(Object key) {
            ensure();
            return base.remove(key);
        }

        public Set keySet() {
            ensure();
            return base.keySet();
        }

        public Collection values() {
            ensure();
            return base.values();
        }

        public Set entrySet() {
            ensure();
            return base.entrySet();
        }

        public boolean containsKey(Object key) {
            ensure();
            return base.containsKey(key);
        }

        public boolean containsValue(Object value) {
            ensure();
            return base.containsValue(value);
        }

        public boolean containsEntry(Object key, Object value) {
            ensure();
            return base.containsEntry(key, value);
        }

        public int size() {
            ensure();
            return base.size();
        }

        public void clear() {
            ensure();
            base.clear();
        }

        public int valueCount(Object key) {
            ensure();
            return base.valueCount(key);
        }

        public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
            ensure();
            base.addEntryListener(listener, key, includeValue);
        }

        public void removeEntryListener(EntryListener listener, Object key) {
            ensure();
            base.removeEntryListener(listener, key);
            // TODO
        }

        private class MultiMapBase implements MultiMap, IGetAwareProxy {
            final MProxy mapProxy;

            private MultiMapBase() {
                mapProxy = new MProxyImpl(name, factory);
            }

            public String getName() {
                return name.substring(4);
            }

            public void clear() {
                mapProxy.clear();
            }

            public boolean containsEntry(Object key, Object value) {
                return mapProxy.containsEntry(key, value);
            }

            public boolean containsKey(Object key) {
                return mapProxy.containsKey(key);
            }

            public boolean containsValue(Object value) {
                return mapProxy.containsValue(value);
            }

            public Collection get(Object key) {
                return (Collection) mapProxy.get(key);
            }

            public boolean put(Object key, Object value) {
                return mapProxy.putMulti(key, value);
            }

            public boolean remove(Object key, Object value) {
                return mapProxy.removeMulti(key, value);
            }

            public Collection remove(Object key) {
                return (Collection) mapProxy.remove(key);
            }

            public int size() {
                return mapProxy.size();
            }

            public Set keySet() {
                return mapProxy.keySet();
            }

            public Collection values() {
                return mapProxy.values();
            }

            public Set entrySet() {
                return mapProxy.entrySet();
            }

            public int valueCount(Object key) {
                return mapProxy.valueCount(key);
            }

            public InstanceType getInstanceType() {
                return InstanceType.MULTIMAP;
            }

            public void destroy() {
                mapProxy.destroy();
            }

            public Object getId() {
                return name;
            }

            public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
                mapProxy.addEntryListener(listener, key, includeValue);
            }

            public void removeEntryListener(EntryListener listener, Object key) {
                mapProxy.removeEntryListener(listener, key);
            }
        }
    }

    interface IRemoveAwareProxy {

        boolean removeKey(Object key);
    }

    interface IGetAwareProxy {

        Object get(Object key);
    }

    private static void check(Object obj) {
        if (obj == null)
            throw new RuntimeException("Object cannot be null.");

        if (!(obj instanceof Serializable)) {
            throw new IllegalArgumentException(obj.getClass().getName() + " is not Serializable.");
        }
    }

    interface MProxy extends IMap, IRemoveAwareProxy, IGetAwareProxy {
        String getLongName();

        void addGenericListener(Object listener, Object key, boolean includeValue, ListenerManager.Type listenerType);

        void removeGenericListener(Object listener, Object key);

        boolean containsEntry(Object key, Object value);

        boolean putMulti(Object key, Object value);

        boolean removeMulti(Object key, Object value);

        boolean add(Object value);

        int valueCount(Object key);

        Set allKeys();
    }


    public static class MProxyImpl extends FactoryAwareNamedProxy implements MProxy, DataSerializable {

        private transient MProxy mproxyReal = null;

        private ConcurrentMapManager concurrentMapManager = null;

        private ListenerManager listenerManager = null;

        public MProxyImpl() {
        }

        private MProxyImpl(String name, FactoryImpl factory) {
            this.name = name;
            mproxyReal = new MProxyReal();
            setFactory(factory);
        }

        public void setFactory(FactoryImpl factory) {
            super.setFactory(factory);
            this.concurrentMapManager = factory.node.concurrentMapManager;
            this.listenerManager = factory.node.listenerManager;
        }

        private void ensure() {
            initialChecks();
            if (mproxyReal == null) {
                mproxyReal = (MProxy) factory.getProxyByName(name);
            }
        }

        public Object getId() {
            ensure();
            return mproxyReal.getId();
        }

        @Override
        public String toString() {
            return "Map [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MProxyImpl mProxy = (MProxyImpl) o;

            return !(name != null ? !name.equals(mProxy.name) : mProxy.name != null);

        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public void destroy() {
            ensure();
            mproxyReal.destroy();
        }

        public Instance.InstanceType getInstanceType() {
            ensure();
            return mproxyReal.getInstanceType();
        }

        public boolean removeKey(Object key) {
            ensure();
            return mproxyReal.removeKey(key);
        }

        public int size() {
            ensure();
            return mproxyReal.size();
        }

        public boolean isEmpty() {
            ensure();
            return mproxyReal.isEmpty();
        }

        public boolean containsKey(Object key) {
            ensure();
            return mproxyReal.containsKey(key);
        }

        public boolean containsValue(Object value) {
            ensure();
            return mproxyReal.containsValue(value);
        }

        public MapEntry getMapEntry(Object key) {
            ensure();
            return mproxyReal.getMapEntry(key);
        }

        public Object get(Object key) {
            ensure();
            return mproxyReal.get(key);
        }

        public Object put(Object key, Object value) {
            ensure();
            return mproxyReal.put(key, value);
        }

        public Object remove(Object key) {
            ensure();
            return mproxyReal.remove(key);
        }

        public void putAll(Map t) {
            ensure();
            mproxyReal.putAll(t);
        }

        public void clear() {
            ensure();
            mproxyReal.clear();
        }

        public int valueCount(Object key) {
            ensure();
            return mproxyReal.valueCount(key);
        }


        public Set allKeys() {
            ensure();
            return mproxyReal.allKeys();
        }

        public Set keySet() {
            ensure();
            return mproxyReal.keySet();
        }

        public Collection values() {
            ensure();
            return mproxyReal.values();
        }

        public Set entrySet() {
            ensure();
            return mproxyReal.entrySet();
        }

        public Object putIfAbsent(Object key, Object value) {
            ensure();
            return mproxyReal.putIfAbsent(key, value);
        }

        public boolean remove(Object key, Object value) {
            ensure();
            return mproxyReal.remove(key, value);
        }

        public boolean replace(Object key, Object oldValue, Object newValue) {
            ensure();
            return mproxyReal.replace(key, oldValue, newValue);
        }

        public Object replace(Object key, Object value) {
            ensure();
            return mproxyReal.replace(key, value);
        }

        public String getName() {
            ensure();
            return mproxyReal.getName();
        }

        public void lock(Object key) {
            ensure();
            mproxyReal.lock(key);
        }

        public boolean tryLock(Object key) {
            ensure();
            return mproxyReal.tryLock(key);
        }

        public boolean tryLock(Object key, long time, TimeUnit timeunit) {
            ensure();
            return mproxyReal.tryLock(key, time, timeunit);
        }

        public void unlock(Object key) {
            ensure();
            mproxyReal.unlock(key);
        }

        public String getLongName() {
            ensure();
            return mproxyReal.getLongName();
        }

        public void addGenericListener(Object listener, Object key, boolean includeValue,
                                       ListenerManager.Type listenerType) {
            ensure();
            mproxyReal.addGenericListener(listener, key, includeValue, listenerType);
        }

        public void removeGenericListener(Object listener, Object key) {
            ensure();
            mproxyReal.removeGenericListener(listener, key);
        }

        public void addEntryListener(EntryListener listener, boolean includeValue) {
            ensure();
            mproxyReal.addEntryListener(listener, includeValue);
        }

        public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
            ensure();
            mproxyReal.addEntryListener(listener, key, includeValue);
        }

        public void removeEntryListener(EntryListener listener) {
            ensure();
            mproxyReal.removeEntryListener(listener);
        }

        public void removeEntryListener(EntryListener listener, Object key) {
            ensure();
            mproxyReal.removeEntryListener(listener, key);
        }

        public boolean containsEntry(Object key, Object value) {
            ensure();
            return mproxyReal.containsEntry(key, value);
        }

        public boolean putMulti(Object key, Object value) {
            ensure();
            return mproxyReal.putMulti(key, value);
        }

        public boolean removeMulti(Object key, Object value) {
            ensure();
            return mproxyReal.removeMulti(key, value);
        }

        public boolean add(Object value) {
            ensure();
            return mproxyReal.add(value);
        }

        private class MProxyReal implements MProxy {

            final InstanceType instanceType;

            public MProxyReal() {
                super();
                this.instanceType = BaseManager.getInstanceType(name);
            }

            @Override
            public String toString() {
                return "Map [" + getName() + "]";
            }

            public InstanceType getInstanceType() {
                return instanceType;
            }

            public Object getId() {
                return name;
            }

            @Override
            public boolean equals(Object o) {
                return MProxyImpl.this.equals(o);
            }

            @Override
            public int hashCode() {
                return MProxyImpl.this.hashCode();
            }

            public String getLongName() {
                return name;
            }

            public String getName() {
                return name.substring(2);
            }

            public MapEntry getMapEntry(Object key) {
                check(key);
                MGetMapEntry mgetMapEntry = concurrentMapManager.new MGetMapEntry();
                return mgetMapEntry.get(name, key);
            }

            public boolean putMulti(Object key, Object value) {
                check(key);
                check(value);
                MPutMulti mput = concurrentMapManager.new MPutMulti();
                return mput.put(name, key, value);
            }

            public Object put(Object key, Object value) {
                check(key);
                check(value);
                MPut mput = ThreadContext.get().getCallCache(factory).getMPut();
                return mput.put(name, key, value, -1);
            }

            public Object get(Object key) {
                check(key);
                MGet mget = ThreadContext.get().getCallCache(factory).getMGet();
                return mget.get(name, key, -1);
            }

            public Object remove(Object key) {
                check(key);
                MRemove mremove = ThreadContext.get().getCallCache(factory).getMRemove();
                return mremove.remove(name, key, -1);
            }

            public int size() {
                MSize msize = concurrentMapManager.new MSize(name);
                return msize.getSize();
            }

            public int valueCount(Object key) {
                int count;
                MValueCount mcount = concurrentMapManager.new MValueCount();
                count = ((Number) mcount.count(name, key, -1)).intValue();
                return count;
            }

            public Object putIfAbsent(Object key, Object value) {
                check(key);
                check(value);
                MPut mput = concurrentMapManager.new MPut();
                return mput.putIfAbsent(name, key, value, -1);
            }

            public boolean removeMulti(Object key, Object value) {
                check(key);
                check(value);
                MRemoveMulti mremove = concurrentMapManager.new MRemoveMulti();
                return mremove.remove(name, key, value);
            }

            public boolean remove(Object key, Object value) {
                check(key);
                check(value);
                MRemove mremove = concurrentMapManager.new MRemove();
                return (mremove.removeIfSame(name, key, value, -1) != null);
            }

            public Object replace(Object key, Object value) {
                check(key);
                check(value);
                MPut mput = concurrentMapManager.new MPut();
                return mput.replace(name, key, value, -1);
            }

            public boolean replace(Object key, Object oldValue, Object newValue) {
                check(key);
                check(newValue);
                throw new UnsupportedOperationException();
            }

            public void lock(Object key) {
                check(key);
                MLock mlock = concurrentMapManager.new MLock();
                mlock.lock(name, key, -1);
            }

            public boolean tryLock(Object key) {
                check(key);
                MLock mlock = concurrentMapManager.new MLock();
                return mlock.lock(name, key, 0);
            }

            public boolean tryLock(Object key, long time, TimeUnit timeunit) {
                check(key);
                if (time < 0)
                    throw new IllegalArgumentException("Time cannot be negative. time = " + time);
                MLock mlock = concurrentMapManager.new MLock();
                return mlock.lock(name, key, timeunit.toMillis(time));
            }

            public void unlock(Object key) {
                check(key);
                MLock mlock = concurrentMapManager.new MLock();
                boolean unlocked = mlock.unlock(name, key, 0);
//                if (! unlocked) throw new IllegalMonitorStateException();
            }

            public void addGenericListener(Object listener, Object key, boolean includeValue,
                                           ListenerManager.Type listenerType) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                listenerManager.addListener(name, listener, key, includeValue, listenerType);
            }

            public void removeGenericListener(Object listener, Object key) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                listenerManager.removeListener(name, listener, key);
            }

            public void addEntryListener(EntryListener listener, boolean includeValue) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                addGenericListener(listener, null, includeValue, ListenerManager.Type.Map);
            }

            public void addEntryListener(EntryListener listener, Object key, boolean includeValue) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                check(key);
                addGenericListener(listener, key, includeValue, ListenerManager.Type.Map);
            }

            public void removeEntryListener(EntryListener listener) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                removeGenericListener(listener, null);
            }

            public void removeEntryListener(EntryListener listener, Object key) {
                if (listener == null)
                    throw new IllegalArgumentException("Listener cannot be null");
                check(key);
                removeGenericListener(listener, key);
            }

            public boolean containsEntry(Object key, Object value) {
                check(key);
                check(value);
                TransactionImpl txn = ThreadContext.get().txn;
                if (txn != null) {
                    if (txn.has(name, key)) {
                        Object v = txn.get(name, key);
                        return v != null;
                    }
                }
                MContainsKey mContainsKey = concurrentMapManager.new MContainsKey();
                return mContainsKey.containsEntry(name, key, value);
            }

            public boolean containsKey(Object key) {
                check(key);
                TransactionImpl txn = ThreadContext.get().txn;
                if (txn != null) {
                    if (txn.has(name, key)) {
                        Object value = txn.get(name, key);
                        return value != null;
                    }
                }
                MContainsKey mContainsKey = concurrentMapManager.new MContainsKey();
                return mContainsKey.containsKey(name, key);
            }

            public boolean containsValue(Object value) {
                check(value);
                TransactionImpl txn = ThreadContext.get().txn;
                if (txn != null) {
                    if (txn.containsValue(name, value))
                        return true;
                }
                MContainsValue mContainsValue = concurrentMapManager.new MContainsValue(name, value);
                return (Boolean) mContainsValue.call();
            }

            public boolean isEmpty() {
                return (size() == 0);
            }

            public void putAll(Map map) {
                Set<Entry> entries = map.entrySet();
                for (Entry entry : entries) {
                    put(entry.getKey(), entry.getValue());
                }
            }

            public boolean add(Object value) {
                if (value == null)
                    throw new NullPointerException();
                MAdd madd = concurrentMapManager.new MAdd();
                if (instanceType == InstanceType.LIST) {
                    return madd.addToList(name, value);
                } else {
                    return madd.addToSet(name, value);
                }
            }

            public boolean removeKey(Object key) {
                if (key == null)
                    throw new NullPointerException();
                MRemoveItem mRemoveItem = concurrentMapManager.new MRemoveItem();
                return mRemoveItem.removeItem(name, key);
            }

            public void clear() {
                Set keys = keySet();
                for (Object key : keys) {
                    removeKey(key);
                }
            }

            public Set entrySet() {
                return (Set) iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_ENTRIES);
            }

            public Set keySet() {
                return (Set) iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS);
            }

            public Set allKeys() {
                return (Set) iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_KEYS_ALL);
            }

            public Collection values() {
                return iterate(ClusterOperation.CONCURRENT_MAP_ITERATE_VALUES);
            }

            private Collection iterate(ClusterOperation iteratorType) {
                MIterate miterate = concurrentMapManager.new MIterate(name, iteratorType);
                return (Collection) miterate.call();
            }

            public void destroy() {
                factory.destroyInstanceClusterwide(name, null);
            }
        }
    }

    static class FactoryAwareNamedProxy implements DataSerializable {
        protected FactoryImpl factory = null;
        protected String name = null;

        public void setFactory(FactoryImpl factory) {
            this.factory = factory;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(factory.getName());
            out.writeUTF(name);
        }

        public void readData(DataInput in) throws IOException {
            setFactory(getFactory(in.readUTF()));
            name = in.readUTF();
        }
    }

    public static class IdGeneratorProxy extends FactoryAwareNamedProxy implements IdGenerator, DataSerializable {

        private transient IdGenerator base = null;

        public IdGeneratorProxy() {
        }

        public IdGeneratorProxy(String name, FactoryImpl factory) {
            this.name = name;
            base = new IdGeneratorBase();
            setFactory(factory);
        }

        private void ensure() {
            initialChecks();
            if (base == null) {
                base = factory.getIdGenerator(name);
            }
        }

        public Object getId() {
            ensure();
            return base.getId();
        }

        @Override
        public String toString() {
            return "IdGenerator [" + getName() + "]";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IdGeneratorProxy that = (IdGeneratorProxy) o;

            return !(name != null ? !name.equals(that.name) : that.name != null);

        }

        @Override
        public int hashCode() {
            return name != null ? name.hashCode() : 0;
        }

        public Instance.InstanceType getInstanceType() {
            ensure();
            return base.getInstanceType();
        }

        public void destroy() {
            ensure();
            base.destroy();
        }

        public String getName() {
            ensure();
            return base.getName();
        }

        public long newId() {
            ensure();
            return base.newId();
        }


        private class IdGeneratorBase implements IdGenerator {

            private static final long MILLION = 1000000;

            AtomicLong million = new AtomicLong(-1);

            AtomicLong currentId = new AtomicLong(2 * MILLION);

            AtomicBoolean fetching = new AtomicBoolean(false);


            public String getName() {
                return name.substring(2);
            }

            public long newId() {
                long millionNow = million.get();
                long idAddition = currentId.incrementAndGet();
                if (idAddition >= MILLION) {
                    synchronized (this) {
                        try {
                            millionNow = million.get();
                            idAddition = currentId.incrementAndGet();
                            if (idAddition >= MILLION) {
                                Long idMillion = getNewMillion();
                                long newMillion = idMillion * MILLION;
                                million.set(newMillion);
                                currentId.set(0);
                            }
                            millionNow = million.get();
                            idAddition = currentId.incrementAndGet();
                        } catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }

                }
                return millionNow + idAddition;
            }

            private Long getNewMillion() {
                try {
                    DistributedTask<Long> task = new DistributedTask<Long>(new IncrementTask(name, factory.getName()));
                    factory.executorServiceImpl.execute(task);
                    return task.get();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            public InstanceType getInstanceType() {
                return InstanceType.ID_GENERATOR;
            }

            public void destroy() {
                factory.destroyInstanceClusterwide(name, null);
            }

            public Object getId() {
                return name;
            }
        }
    }

    public static class IncrementTask implements Callable<Long>, Serializable {
        String name = null;
        String factoryName = null;

        public IncrementTask() {
            super();
        }

        public IncrementTask(String uuidName, String factoryName) {
            super();
            this.name = uuidName;
            this.factoryName = factoryName;
        }

        public Long call() {
            FactoryImpl factory = getFactory(factoryName);
            MProxy map = factory.idGeneratorMapProxy;
            map.lock(name);
            try {
                Long max = (Long) map.get(name);
                if (max == null) {
                    max = 0L;
                    map.put(name, 0L);
                    return max;
                } else {
                    Long newMax = max + 1;
                    map.put(name, newMax);
                    return newMax;
                }
            } finally {
                map.unlock(name);
            }
        }
    }

}
