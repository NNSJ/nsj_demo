package com.nsj.threadDemo.demo;

/**
 *下面举一个小例子，我们在建立一些网络长连接的时候，通常需要定时性的发送一些心跳包来确保连接的正常。而当这个网络长 连接关闭的时候，我们需要这个发送心跳包的线程也一起关闭。这个时候守护线程就起到了至关重要的作用。
 *  假如innerThread线程不设置Daemon属性则t线程执行完毕以后，innerThread依然会一直执行
 */
public class DeamonThread3 {
    public static void main(String[] args) {
        // t 线程模拟网络长连接
        Thread t = new Thread(() -> {
            // innerThread 线程模拟发送心跳的线程
            Thread  innerThread = new Thread(() -> {
                try {
                    while(true) {
                        System.out.println("发送心跳");
                        Thread.sleep(1_000);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            innerThread.setDaemon(true);
            innerThread.start();
            try {
                Thread.sleep(10000);// 模拟长连接1秒以后就退出
                System.out.println("T Thread done");
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.start();
    }

}
