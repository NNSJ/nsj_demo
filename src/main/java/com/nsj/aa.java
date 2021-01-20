package com.nsj;

import org.apache.zookeeper.server.NIOServerCnxnFactory;

import java.util.Random;

public class aa {

    public static void main(String[] args) {
         Random random = new Random(System.currentTimeMillis());
        int numCores = Runtime.getRuntime().availableProcessors();
        // 32 cores sweet spot seems to be 4 selector threads
        int numSelectorThreads = Integer.getInteger(
                "aa",
                Math.max((int) Math.sqrt((float) numCores/2), 1));
        System.out.println("numCore==="+numCores);
        System.out.println("numSelectorThreads==="+numSelectorThreads);
        String serverCnxnFactoryName =
                System.getProperty("zookeeper.serverCnxnFactory");
        System.out.println(serverCnxnFactoryName);
        System.out.println( NIOServerCnxnFactory.class.getName());
        System.out.println("==========");
        System.out.println(System.getProperty("porc_datanode"));

        System.out.println("random==="+Integer.toHexString(random.nextInt()));
    }
}
