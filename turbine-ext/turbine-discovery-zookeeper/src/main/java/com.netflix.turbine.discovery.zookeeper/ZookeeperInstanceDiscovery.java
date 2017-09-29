package com.netflix.turbine.discovery.zookeeper;

import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import rx.schedulers.Schedulers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhumingyuan on 22/8/17.
 */
public class ZookeeperInstanceDiscovery {

    private static final Logger LOGGER = LoggerFactory.getLogger(ZookeeperInstanceDiscovery.class);

    private final CuratorFramework curatorClient;

    private final Set<PathChildrenCache> memberPathCaches = Sets.newHashSet();

    private final SynchronousQueue<ZookeeperInstance> blockQueue = new SynchronousQueue<>();

    public static void main(String[] args) throws Exception {
        String path = "/dynamodb/proxy/member";
        ZookeeperInstanceDiscovery discovery = new ZookeeperInstanceDiscovery(CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 5)));
        try {
            Observable<ZookeeperInstance> observable = discovery.getInstanceEvents(path);
            observable.subscribe(zookeeperInstance -> LOGGER.info("instance {}", zookeeperInstance));
            TimeUnit.MINUTES.sleep(100);
        } finally {
            discovery.close();
        }
    }

    public void close() {
        memberPathCaches.stream().forEach(pathChildrenCache -> {
            try {
                pathChildrenCache.close();
            } catch (IOException e) {
                LOGGER.error("close cache listener error", e);
            }
        });
        if (curatorClient != null) {
            curatorClient.close();
        }
    }

    public ZookeeperInstanceDiscovery(CuratorFramework zkClient) {
        curatorClient = zkClient;
        curatorClient.start();
    }

    private void initZkListener(String memberPath) {
        addStateChangeListener(memberPath);
        LOGGER.info("Init the zk state change {} listener", memberPath);
        addMemberCacheListener(memberPath);
        LOGGER.info("Init the memberPath {} listener", memberPath);
    }

    public Observable<ZookeeperInstance> getInstanceEvents(String memberPath) {
        Observable observable = Observable.
            create((Subscriber<? super ZookeeperInstance> subscriber) -> {
                try {
                    List<String> endpoint = curatorClient.getChildren().forPath(memberPath);
                    LOGGER.info("Fetching instance list for path {} result {}", memberPath, endpoint.size());
                    endpoint.stream().forEach(s -> subscriber.onNext(ZookeeperInstance.create(s)));
                    subscriber.onCompleted();
                } catch (Throwable e) {
                    subscriber.onError(e);
                }
            })
            .subscribeOn(Schedulers.io())
            .toList()
            .repeatWhen(a -> a.flatMap(o -> {
                try {
                    // prevent the zk event miss
                    return Observable.just(blockQueue.poll(10, TimeUnit.MINUTES));
                } catch (InterruptedException e) {
                    LOGGER.warn("Waiting for the block queue error, path " + memberPath);
                    return Observable.just(ZookeeperInstance.nullObject());
                }
            }))
            .startWith(new ArrayList<ZookeeperInstance>())
            .buffer(2, 1)
            .filter(l -> l.size() == 2)
            .flatMap(ZookeeperInstanceDiscovery::delta);

        initZkListener(memberPath);

        return observable;
    }

    private void addStateChangeListener(String memberPath) {
        curatorClient.getConnectionStateListenable().addListener((curatorFramework, newState) -> {
                        LOGGER.info("Zookeeper connection state changed to {}.", newState);
            if (newState == ConnectionState.CONNECTED || newState == ConnectionState.RECONNECTED) {
                addMemberCacheListener(memberPath);
            }
        });
    }

    private void addMemberCacheListener(String memberPath) {
        PathChildrenCache memberPathCache = new PathChildrenCache(curatorClient, memberPath, true);
        try {
            memberPathCache.start();
            memberPathCache.getListenable().addListener(new MemberCacheListener(memberPath));
            memberPathCaches.add(memberPathCache);
            LOGGER.info("add the listener {}", memberPath);
        } catch (Exception e) {
            LOGGER.error("event error", e);
        }
    }

    private class MemberCacheListener implements PathChildrenCacheListener {

        private String rootPath;

        MemberCacheListener(String rootPath) {
            this.rootPath = rootPath;
        }

        private String getEndpoint(PathChildrenCacheEvent event) {
            return event.getData().getPath().replace(rootPath, "");
        }

        @Override
        public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event)
            throws Exception {
            LOGGER.debug("receive the event {}", event);
            switch (event.getType()) {
                case CHILD_ADDED: {
                    String endpoint = getEndpoint(event);
                    ZookeeperInstance.create(ZookeeperInstance.Status.UP, endpoint);
                    blockQueue.offer(ZookeeperInstance.create(ZookeeperInstance.Status.UP, endpoint));
                    break;
                }
                case CHILD_UPDATED: {
                    break;
                }
                case CHILD_REMOVED: {
                    String endpoint = getEndpoint(event);
                    blockQueue.offer(ZookeeperInstance.create(ZookeeperInstance.Status.DOWN, endpoint));
                    break;
                }
                default:
                    break;
            }
        }
    }

    static Observable<ZookeeperInstance> delta(List<List<ZookeeperInstance>> listOfLists) {
        if (listOfLists.size() == 1) {
            return Observable.from(listOfLists.get(0));
        } else {
            // diff the two
            List<ZookeeperInstance> newList = listOfLists.get(1);
            List<ZookeeperInstance> oldList = new ArrayList<>(listOfLists.get(0));

            Set<ZookeeperInstance> delta = new LinkedHashSet<>();
            delta.addAll(newList);
            // remove all that match in old
            delta.removeAll(oldList);

            // filter oldList to those that aren't in the newList
            oldList.removeAll(newList);

            // for all left in the oldList we'll create DROP events
            for (ZookeeperInstance old : oldList) {
                delta.add(ZookeeperInstance.create(ZookeeperInstance.Status.DOWN, old.getAddress()));
            }

            return Observable.from(delta);
        }
    }

}
