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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TopicManager extends BaseManager {
    
    TopicManager(Node node) {
        super (node);
    }

    Map<String, TopicInstance> mapTopics = new HashMap<String, TopicInstance>();

    public TopicInstance getTopicInstance(String name) {
        TopicInstance ti = mapTopics.get(name);
        if (ti == null) {
            ti = new TopicInstance(name);
            mapTopics.put(name, ti);
        }
        return ti;
    }

    public void syncForDead(Address deadAddress) {
        Collection<TopicInstance> instances = mapTopics.values();
        for (TopicInstance instance : instances) {
            instance.removeListener(deadAddress);
        }
    }

    public void syncForAdd() {
    }

    @Override
    void handleListenerRegisterations(boolean add, String name, Data key, Address address,
                                      boolean includeValue) {
        TopicInstance instance = getTopicInstance(name);
        if (add) {
            instance.addListener(address, includeValue);
        } else {
            instance.removeListener(address);
        }
    }

    void destroy(String name) {
        TopicInstance instance = mapTopics.remove(name);
        if (instance != null) {
            instance.mapListeners.clear();
        }
    }

    void doPublish(String name, Object msg) {
        Data dataMsg = null;
        try {
            dataMsg = ThreadContext.get().toData(msg);
        } catch (Exception e) {
            e.printStackTrace();
        }
        enqueueAndReturn(new TopicPublishProcess(name, dataMsg));
    }

    class TopicPublishProcess implements Processable {
        final Data dataMsg;
        final String name;

        public TopicPublishProcess(String name, Data dataMsg) {
            super();
            this.dataMsg = dataMsg;
            this.name = name;
        }

        public void process() {
            getTopicInstance(name).publish(dataMsg);
        }
    }

    class TopicInstance {
        String name;

        Map<Address, Boolean> mapListeners = new HashMap<Address, Boolean>(1);

        public TopicInstance(String name) {
            super();
            this.name = name;
        }

        public void addListener(Address address, boolean includeValue) {
            mapListeners.put(address, includeValue);
        }

        public void removeListener(Address address) {
            mapListeners.remove(address);
        }

        public void publish(Data msg) {
            fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, msg);
        }

    }
}
