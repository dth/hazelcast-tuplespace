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

package com.hazelcast.nio;

import com.hazelcast.cluster.Bind;
import com.hazelcast.cluster.ClusterManager;
import com.hazelcast.impl.Node;

import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConnectionManager {

    protected Logger logger = Logger.getLogger(ConnectionManager.class.getName());

    private final Map<Address, Connection> mapConnections = new ConcurrentHashMap<Address, Connection>(
            100);

    private volatile boolean live = true;

    private final Set<Address> setConnectionInProgress = new CopyOnWriteArraySet<Address>();

    private final Set<ConnectionListener> setConnectionListeners = new CopyOnWriteArraySet<ConnectionListener>();

    private boolean acceptTypeConnection = false;

    final Node node;

    public ConnectionManager(Node node) {
        this.node = node;
    }


    public void addConnectionListener(final ConnectionListener listener) {
        setConnectionListeners.add(listener);
    }

    public synchronized boolean bind(final Address endPoint, final Connection connection,
                                     final boolean accept) {
        connection.setEndPoint(endPoint);
        final Connection connExisting = mapConnections.get(endPoint);
        if (connExisting != null && connExisting != connection) {
            final String msg = "Two connections from the same endpoint " + endPoint
                    + ", acceptTypeConnection=" + acceptTypeConnection + ",  now accept="
                    + accept;
            logger.log(Level.FINEST, msg);
            return true;
        }
        if (!endPoint.equals(node.getThisAddress())) {
            acceptTypeConnection = accept;
            if (!accept) {
                //make sure bind packet is the first packet sent to the end point.
                ClusterManager clusterManager = node.clusterManager;
                Packet bindPacket = clusterManager.createRemotelyProcessablePacket(new Bind(clusterManager.getThisAddress()));
                connection.writeHandler.enqueuePacket(bindPacket);
                //now you can send anything...
            }
            mapConnections.put(endPoint, connection);
            setConnectionInProgress.remove(endPoint);
            for (final ConnectionListener listener : setConnectionListeners) {
                listener.connectionAdded(connection);
            }
        } else {
            return false;
//            throw new RuntimeException("ConnMan setting self!!");
        }
        return true;
    }

    public synchronized Connection createConnection(final SocketChannel socketChannel,
                                                    final boolean acceptor) {
        final Connection connection = new Connection(this, socketChannel);
        try {
            if (acceptor) {
                // do nothing. you will be registering for the
                // write operation when you have something to
                // write already in the outSelector thread.
            } else {
                node.inSelector.addTask(connection.getReadHandler());
                // socketChannel.register(inSelector.selector,
                // SelectionKey.OP_READ, readHandler);
            }
        } catch (final Exception e) {
            e.printStackTrace();
        }
        return connection;
    }

    public synchronized void failedConnection(final Address address) {
        setConnectionInProgress.remove(address);
        if (!node.joined()) {
            node.failedConnection(address);
        }
    }

    public Connection getConnection(final Address address) {
        return mapConnections.get(address);
    }

    public Connection[] getConnections() {
        final Object[] connObjs = mapConnections.values().toArray();
        final Connection[] conns = new Connection[connObjs.length];
        for (int i = 0; i < conns.length; i++) {
            conns[i] = (Connection) connObjs[i];
        }
        return conns;
    }

    public synchronized Connection getOrConnect(final Address address) {
        if (address.equals(node.getThisAddress()))
            throw new RuntimeException("Connecting to self! " + address);
        final Connection connection = mapConnections.get(address);
        if (connection == null) {
            if (setConnectionInProgress.add(address)) {
                if (! node.clusterManager.shouldConnectTo(address))
                    throw new RuntimeException("Should not connect to " + address);

                node.outSelector.connect(address);
            }
        }
        return connection;
    }

    public synchronized void remove(final Connection connection) {
        if (connection == null)
            return;
        if (connection.getEndPoint() != null) {
            mapConnections.remove(connection.getEndPoint());
            setConnectionInProgress.remove(connection.getEndPoint());
            for (final ConnectionListener listener : setConnectionListeners) {
                listener.connectionRemoved(connection);
            }
        }
        if (connection.live())
            connection.close();
    }

    public void start() {
        live = true;
    }

    public synchronized void shutdown() {
        live = false;
        for (final Connection conn : mapConnections.values()) {
            try {
                remove(conn);
            } catch (final Exception e) {
            }
        }
        setConnectionInProgress.clear();
        mapConnections.clear();
    }

    @Override
    public synchronized String toString() {
        final StringBuffer sb = new StringBuffer("Connections {");
        for (final Connection conn : mapConnections.values()) {
            sb.append("\n" + conn);
        }
        sb.append("\nlive=" + live);
        sb.append("\n}");
        return sb.toString();
    }
}
