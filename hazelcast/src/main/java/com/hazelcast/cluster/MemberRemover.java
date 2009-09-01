/**
 * 
 */
package com.hazelcast.cluster;

import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MemberRemover extends AbstractRemotelyProcessable {
    private Address deadAddress = null;

    public MemberRemover() {
    }

    public MemberRemover(Address deadAddress) {
        super();
        this.deadAddress = deadAddress;
    }

    public void process() {
        getNode().clusterManager.doRemoveAddress(deadAddress);
    }

    public void setConnection(Connection conn) {
    }

    public void readData(DataInput in) throws IOException {
        deadAddress = new Address();
        deadAddress.readData(in);
    }

    public void writeData(DataOutput out) throws IOException {
        deadAddress.writeData(out);
    }
}