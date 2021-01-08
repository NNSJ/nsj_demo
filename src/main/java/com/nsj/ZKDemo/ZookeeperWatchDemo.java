package com.nsj.ZKDemo;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperWatchDemo {
    ZooKeeper zk = null;

    @Before
    public void init() throws Exception{

        zk = new ZooKeeper("192.168.146.4:2181",2000,new Watcher(){

            public void process(WatchedEvent event) {

                if(event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeDataChanged){
                    System.out.println(event.getPath());
                    System.out.println(event.getType());
                    System.out.println("test event ");

                    try{
                        System.out.println(new String(zk.getData(event.getPath(), true, null),"UTF-8"));
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }else if(event.getState() == KeeperState.SyncConnected && event.getType() == EventType.NodeChildrenChanged){

                    System.out.println("子节点变化了");
                }
            }

        });

    }

    @Test
    public void testGetWatch() throws Exception{
        while(true){
            byte[] data = zk.getData("/nsj", true, null);
            System.out.println(new String (data,"UTF-8"));
            Thread.sleep(30000);
        }
    }
}
