package com.netflix.turbine.discover.zookeeper;

import com.netflix.turbine.discovery.zookeeper.ZookeeperInstance;
import com.netflix.turbine.discovery.zookeeper.ZookeeperInstanceDiscovery;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import rx.observers.TestSubscriber;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhumingyuan on 22/8/17.
 */
public class ZkInstanceDiscoverTest {

    private String upAddress1;
    private String downAddress1;
    private String upAddress2;

    private CuratorFramework zkClient;
    private ZookeeperInstanceDiscovery zookeeperInstanceDiscovery;
    private TestingServer zkServer;

    private String rootPath;

    @Before
    public void setUp() throws Exception {
        upAddress1 = "127.0.0.1:8080";
        downAddress1 = "127.0.0.1:8080";
        upAddress2 = "127.0.0.1:8081";

        rootPath = "/service";

        zkClient = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5));
        zookeeperInstanceDiscovery = new ZookeeperInstanceDiscovery(zkClient);

        zkServer = new TestingServer(2181, true);
    }


    @After
    public void tearDown() throws Exception {
        zkClient.close();
        zkServer.stop();
    }

    @Test
    public void testUpdateToSameInstance() throws Exception {
        ZookeeperInstance a = ZookeeperInstance.create(ZookeeperInstance.Status.UP, upAddress1);
        ZookeeperInstance b = ZookeeperInstance.create(ZookeeperInstance.Status.DOWN, downAddress1);

        Assert.assertEquals(ZookeeperInstance.Status.UP, a.getStatus());
        Assert.assertEquals(ZookeeperInstance.Status.DOWN, b.getStatus());

        TestSubscriber<ZookeeperInstance> ts = new TestSubscriber<>();

        zookeeperInstanceDiscovery.getInstanceEvents(rootPath).subscribe(ts);
        TimeUnit.SECONDS.sleep(2);
        zkClient.create().creatingParentContainersIfNeeded().forPath(rootPath + "/" + upAddress1, new byte[]{});
        TimeUnit.SECONDS.sleep(2);
        zkClient.delete().forPath(rootPath + "/" + downAddress1);
        TimeUnit.SECONDS.sleep(2);
        ts.assertReceivedOnNext(Arrays.asList(a, b));
    }


    @Test
    public void testMultipleInstances() throws Exception {
        ZookeeperInstance a = ZookeeperInstance.create(ZookeeperInstance.Status.UP, upAddress1);
        ZookeeperInstance b = ZookeeperInstance.create(ZookeeperInstance.Status.DOWN, downAddress1);
        ZookeeperInstance c = ZookeeperInstance.create(ZookeeperInstance.Status.UP, upAddress2);

        Assert.assertEquals(ZookeeperInstance.Status.UP, a.getStatus());
        Assert.assertEquals(ZookeeperInstance.Status.DOWN, b.getStatus());
        Assert.assertEquals(ZookeeperInstance.Status.UP, c.getStatus());

        TestSubscriber<ZookeeperInstance> ts = new TestSubscriber<>();
        zookeeperInstanceDiscovery.getInstanceEvents(rootPath).subscribe(ts);
        TimeUnit.SECONDS.sleep(2);
        zkClient.create().creatingParentContainersIfNeeded().forPath(rootPath + "/" + upAddress1, new byte[]{});
        TimeUnit.SECONDS.sleep(2);
        zkClient.delete().forPath(rootPath + "/" + downAddress1);
        TimeUnit.SECONDS.sleep(2);
        zkClient.create().creatingParentContainersIfNeeded().forPath(rootPath + "/" + upAddress2, new byte[]{});
        TimeUnit.SECONDS.sleep(2);
        ts.assertReceivedOnNext(Arrays.asList(a, b, c));
    }

}
