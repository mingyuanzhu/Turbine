package com.netflix.turbine.discovery.zookeeper;

import com.netflix.turbine.discovery.StreamAction;
import com.netflix.turbine.discovery.StreamDiscovery;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import rx.Observable;

import java.net.URI;

/**
 * Created by zhumingyuan on 22/8/17.
 */
public class ZookeeperStreamDiscovery implements StreamDiscovery {

    public final static String HOSTNAME = "{HOSTNAME}";

    private final String path;
    private final String connectionString;
    private final String urlTemplate;
    private final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

    public static ZookeeperStreamDiscovery create(String path, String connectionString, String urlTemplate) {
        return new ZookeeperStreamDiscovery(path, connectionString, urlTemplate);
    }

    private ZookeeperStreamDiscovery(String path, String connectionString, String urlTemplate) {
        this.urlTemplate = urlTemplate;
        this.path = path;
        this.connectionString = connectionString;
    }

    @Override
    public Observable<StreamAction> getInstanceList() {
        return new ZookeeperInstanceDiscovery(CuratorFrameworkFactory.newClient(connectionString, retryPolicy))
            .getInstanceEvents(path)
            .map(zi -> {
                URI uri;
                try {
                    uri = new URI(urlTemplate.replace(HOSTNAME, zi.getAddress()));
                } catch (Exception e) {
                    throw new RuntimeException("Invalid URI", e);
                }
                if (zi.getStatus() == ZookeeperInstance.Status.UP) {
                    return StreamAction.create(StreamAction.ActionType.ADD, uri);
                } else {
                    return StreamAction.create(StreamAction.ActionType.REMOVE, uri);
                }
            });
    }

}
