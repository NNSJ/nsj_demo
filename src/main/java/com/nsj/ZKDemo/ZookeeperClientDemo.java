package com.nsj.ZKDemo;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperClientDemo {
    ZooKeeper zk = null;

    @Before
    public void setUp() throws Exception{
        zk = new ZooKeeper("192.168.146.4:2181",1000,null);
    }

    @Test
    public void testCreate() throws Exception{
        String create = zk.create("/nsj/a", "hanyuli".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(create);
    }

    @Test
    public void testUpdate() throws Exception{
        zk.setData("/nsj", "my girl:hanyu li".getBytes(), -1);
        zk.close();
    }

    @Test
    public void testGet() throws Exception{
        byte[] data = zk.getData("/nsj", false, null);
        System.out.println(new String(data,"UTF-8"));
        zk.close();
    }

    @Test
    public void testListChildren() throws Exception{
        List<String> children = zk.getChildren("/nsj", false);

        for(String child:children){
            System.out.println(child);
        }
        zk.close();
    }

    @Test
    public void testRm() throws Exception{
        zk.delete("/nsj", 1);
        zk.close();
    }
}
