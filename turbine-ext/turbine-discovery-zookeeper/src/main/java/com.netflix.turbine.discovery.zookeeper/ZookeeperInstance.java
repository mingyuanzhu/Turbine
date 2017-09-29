package com.netflix.turbine.discovery.zookeeper;

import static com.netflix.turbine.discovery.zookeeper.ZookeeperInstance.Status.DOWN;
import static com.netflix.turbine.discovery.zookeeper.ZookeeperInstance.Status.UP;

/**
 * Created by zhumingyuan on 22/8/17.
 */
public class ZookeeperInstance {

    public enum Status {
        UP, DOWN
    }

    private final Status status;
    private final String address;

    private ZookeeperInstance(Status status, String address) {
        this.status = status;
        this.address = address;
    }

    public Status getStatus() {
        return status;
    }

    public String getAddress() {
        return address;
    }

    public static ZookeeperInstance create(String address) {
        return new ZookeeperInstance(UP, address);
    }

    public static ZookeeperInstance create(Status status, String address) {
        return new ZookeeperInstance(status, address);
    }

    public static ZookeeperInstance nullObject() {
        return new ZookeeperInstance(DOWN,"");
    }

    @Override
    public String toString() {
        return "ZookeeperInstance{" + "status=" + status + ", address='" + address + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ZookeeperInstance that = (ZookeeperInstance) o;

        return address != null ? address.equals(that.address) : that.address == null;
    }

    @Override
    public int hashCode() {
        return address != null ? address.hashCode() : 0;
    }
}
