package com.nsj.ZKDemo;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class ZookeeperTest {

    static final String address = "192.168.1.24:2181,192.168.1.25:2181,192.168.1.26:2181";
    static final int session_timeout = 5000;
    static final CountDownLatch countDownLatch = new CountDownLatch(1);
    public static void main(String[] args) throws Exception {
        ZooKeeper zk = new ZooKeeper(address,session_timeout,new Watcher() {


            public void process(WatchedEvent event) {
                KeeperState keeperState =  event.getState();
                EventType eventType = event.getType();
                if(KeeperState.SyncConnected == keeperState) {
                    if(EventType.None == eventType) {
                        countDownLatch.countDown();
                        System.out.println("zk 建立连接");
                    }
                }
            }

        });
        countDownLatch.await();
        //同步创建
        String returnStr = zk.create("/test", "hello".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(returnStr);


        //查询数据
        System.out.println(zk.getData("/test", false, null).toString());

        //异步删除
        zk.delete("/test", -1,new AsyncCallback.VoidCallback() {


            public void processResult(int rc, String path, Object ctx) {
                System.out.println(rc);
                System.out.println(path);
                System.out.println(ctx);

            }
        }, "a");

        Thread.sleep(5000);
        zk.close();
    }
}