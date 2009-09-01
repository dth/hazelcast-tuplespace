/* 
 * Copyright (c) 2007-2009, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.DistributedTask;
import com.hazelcast.impl.BaseManager.Processable;

import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * Implements a distributed @link java.util.concurrent.ExecutorService.
 * 
 * Hazelcast to execute your code:
 * <ul> 
	<li>on a specific cluster member you choose</li> 
	<li>on the member owning the key you choose</li>
	<li>on the member Hazelcast will pick</li>
	<li>on all or subset of the cluster members</li>
 * </ul>
 * see @link com.hazelcast.DistributedTask
 * 
 * Note on finalization
 * 
 * The Hazelcast ExecutorService is a facade implementing @link java.util.concurrent.ExecutorService
 * but is not a separate component of the Hazelcast cluster and cannot be
 * finalized. Shutdown the entire cluster instead.
 * Methods invoking finalization have no effect, while methods checking
 * the terminated status return the status of the cluser (isTerminated() return
 * true after an @link com.hazelcast.Hazelcast#shutdown). 
 */
public class ExecutorServiceProxy implements ExecutorService, Constants {
    
    
    final Node node;

    public ExecutorServiceProxy(Node node) {
        this.node = node;
    }

    /**
     * Hazelcast ExecutorService cannot be really shut down.
     * The method return always false immeditely.
     * 
     * @return always false
     */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public List invokeAll(Collection tasks) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public List invokeAll(Collection tasks, long timeout, TimeUnit unit)
            throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    public Object invokeAny(Collection tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException();
    }

    public Object invokeAny(Collection tasks, long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        throw new UnsupportedOperationException();
    }

    /**
     * Hazelcast ExecutorService cannot be really shut down.
     * The method returns the status of the current node.
     * 
     * @return the status of the current node
     */
    public boolean isShutdown() {
        return !node.executorManager.isStarted();
    }

    /**
     * Hazelcast ExecutorService cannot be really shut down.
     * The method returns the status of the current node.
     * 
     * @return the status of the current node
     */
    public boolean isTerminated() {
        return !node.executorManager.isStarted();
    }

    /**
     * Hazelcast ExecutorService cannot be really shut down.
     * The method has no effect.
     * 
     * @link com.hazelcast.Hazelcast#shutdown
     */
    public void shutdown() {
    	return;
    }

    /**
     * Hazelcast ExecutorService cannot be really shut down.
     * The method always return an empty list.
     * 
     * @return an empty list
     */
    public List<Runnable> shutdownNow() {
        return new ArrayList<Runnable>();
    }

    public <T> Future<T> submit(Callable<T> task) {
        DistributedTask dtask = new DistributedTask(task);
        Processable action = node.executorManager.createNewExecutionAction(dtask);
        ClusterService clusterService = node.clusterService;
        clusterService.enqueueAndReturn(action);
        return dtask;
    }

    public Future<?> submit(Runnable task) {
        DistributedTask dtask = null;
        if (task instanceof DistributedTask) {
            dtask = (DistributedTask) task;
        } else {
            dtask = new DistributedTask(task, null);
        }
        Processable action = node.executorManager.createNewExecutionAction(dtask);
        ClusterService clusterService = node.clusterService;
        clusterService.enqueueAndReturn(action);
        return dtask;
    }

    public <T> Future<T> submit(Runnable task, T result) {
        DistributedTask dtask = null;
        if (task instanceof DistributedTask) {
            dtask = (DistributedTask) task;
        } else {
            dtask = new DistributedTask(task, result);
        }
        Processable action = node.executorManager.createNewExecutionAction(dtask);
        ClusterService clusterService = node.clusterService;
        clusterService.enqueueAndReturn(action);
        return dtask;
    }

    public void execute(Runnable command) {
        DistributedTask dtask = null;
        if (command instanceof DistributedTask) {
            dtask = (DistributedTask) command;
        } else {
            dtask = new DistributedTask(command, null);
        }
        Processable action = node.executorManager.createNewExecutionAction(dtask);
        node.clusterService.enqueueAndReturn(action);
    }
}
