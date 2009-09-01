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
package com.hazelcast.jmx;

import java.net.InetAddress;

import javax.management.ObjectName;

import com.hazelcast.core.Member;

/**
 * The instrumentation MBean for a member.
 * 
 * @author Marco Ferrante, DISI - University of Genoa
 */
@JMXDescription("A member of the cluster")
public class MemberMBean extends AbstractMBean<Member> {

	private ObjectName name;
	
	public MemberMBean(Member managedObject) {
		super(managedObject);
	}

	public ObjectName getObjectName() throws Exception {
		// A JVM can host only one cluster, so names are can be hardcoded.
		// Multiple clusters for JVM (see Hazelcast issue 78) need a
		// different naming schema
		if (name == null) {
			String memberName = "Local";
	    	if (!getManagedObject().localMember()) {
	    		// String concatenation is not a performance issue,
	    		// used only during registration
	    		memberName =  '"' + getManagedObject().getInetAddress().getHostAddress()
	    				+ ":" + getManagedObject().getPort() + '"';
	    	}
			name = MBeanBuilder.buildObjectName("type", "Cluster", "name", memberName); 
		}
		return name;
	}

	@JMXDescription("The network address")
	@JMXAttribute("Port")
	public int getPort() {
		return getManagedObject().getPort();
	}

	@JMXAttribute("InetAddress")
	@JMXDescription("The network port")
	public InetAddress getInetAddress() {
		return getManagedObject().getInetAddress();
	}

	@JMXAttribute("SuperClient")
	@JMXDescription("The member is a superclient")
	public boolean isSuperClient() {
		return getManagedObject().isSuperClient();
	}

}
