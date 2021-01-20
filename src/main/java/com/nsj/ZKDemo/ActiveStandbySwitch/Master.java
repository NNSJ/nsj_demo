package com.nsj.ZKDemo.ActiveStandbySwitch;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * 主备节点的切换，是分布式应用的基本要求。现在用 Zookeeper 实现主备节点自动切换功能。
 基本思路：
 1 多个服务启动后，都尝试在 Zookeeper中创建一个  EPHEMERAL 类型的节点，Zookeeper本身会保证，只有一个 服务 会创建成功，其他 服务 抛出异常。
 2 成功创建节点的 服务 ，作为主节点，继续运行
 3 其他 服务 设置一个Watcher监控节点状态，
 4 如果主节点消失，其他 服务会接到通知，再次尝试创建 EPHEMERAL 类型的节点。

 使用方法：该类的主方法 执行两次，然后关闭一个，测试另一个会不会去重新创建/master临时节点
 */
public class Master implements Watcher {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);
    enum MasterStates {RUNNING, ELECTED, NOTELECTED};
    private volatile MasterStates state = MasterStates.RUNNING;

    MasterStates getState() {
        return state;
    }

    private static final int SESSION_TIMEOUT = 5000;
    private static final String CONNECTION_STRING = "192.168.146.4:2181";
    private static final String ZNODE_NAME = "/master";
    private Random random = new Random(System.currentTimeMillis());
    private ZooKeeper zk;
    private String serverId = Integer.toHexString(random.nextInt());

    private volatile boolean connected = false;
    private volatile boolean expired = false;

    public void startZk() throws IOException {
        zk = new ZooKeeper(CONNECTION_STRING, SESSION_TIMEOUT, this);
    }

    public void stopZk() {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn("Interrupted while closing ZooKeeper session.", e);
            }
        }
    }

    /**
     * 抢注节点
     */
    public void enroll() {
        zk.create(ZNODE_NAME,
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallBack, null);
    }

    AsyncCallback.StringCallback masterCreateCallBack = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    //网络问题，需要检查节点是否创建成功，连接丢失
                    System.out.println("CONNECTIONLOSS");
                    checkMaster();
                    return;
                case OK:
                    state = MasterStates.ELECTED;
                    System.out.println("OK");

                    break;
                case NODEEXISTS:
                    System.out.println("NODEEXISTS");
                    //节点存在，不用选举，监听那个临时节点
                    state = MasterStates.NOTELECTED;
                    // 添加Watcher
                    addMasterWatcher();
                    break;
                default:
                    state = MasterStates.NOTELECTED;
                    LOG.error("Something went wrong when running for master.",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
            LOG.info("I'm " + (state == MasterStates.ELECTED ? "" : "not ") + "the leader " + serverId);
        }
    };

    public void checkMaster() {
        zk.getData(ZNODE_NAME, false, masterCheckCallBack, null);

    }

    AsyncCallback.DataCallback masterCheckCallBack = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NONODE:
                    // 节点未创建，再次注册
                    enroll();
                    return;
                case OK:
                    if (serverId.equals(new String(data))) {
                        state = MasterStates.ELECTED;
                    } else {
                        state = MasterStates.NOTELECTED;
                        addMasterWatcher();
                    }
                    break;
                default:
                    LOG.error("Error when reading data.",KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    void addMasterWatcher() {
        zk.exists(ZNODE_NAME,
                masterExistsWatcher,
                masterExistsCallback,
                null);
    }
    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType() == Event.EventType.NodeDeleted){
                System.out.println("节点删除");
                assert ZNODE_NAME.equals(event.getPath());
                enroll();
            }
        }
    };
    AsyncCallback.StatCallback masterExistsCallback = new AsyncCallback.StatCallback() {
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    System.out.println("连接丢失CONNECTIONLOSS");
                    addMasterWatcher();
                    break;
                case OK:
                    break;
                case NONODE:
                    state = MasterStates.RUNNING;
                    enroll();
                    LOG.info("It sounds like the previous master is gone, " +
                            "so let's run for master again.");
                    break;
                default:
                    checkMaster();
                    break;
            }
        }
    };

    public static void main(String[] args) throws InterruptedException, IOException {
        Master m = new Master( );
        m.startZk();
        System.out.println("第一次"+m.isConnected());
        while (!m.isConnected()) {
            Thread.sleep(100);
        }
        m.enroll();
        while (!m.isExpired()) {
            Thread.sleep(1000);
        }
        System.out.println("停止前");
        m.stopZk();
        System.out.println("停止后");

    }

    boolean isConnected() {
        return connected;
    }

    boolean isExpired() {
        return expired;
    }

    //Master 实现Watch的回调，创建zk连接之后，调用此方法
    @Override
    public void process(WatchedEvent e) {
        LOG.info("Processing event: " + e.toString());
        System.out.println("Processing event: " + e.toString());
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    connected = true;
                    System.out.println("connected = true");
                    break;
                case Disconnected:
                    connected = false;
                    System.out.println("connected = false");
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    System.out.println("Session expiration");
                    LOG.error("Session expiration");
                    break;
                default:
                    break;
            }
        }
    }
}