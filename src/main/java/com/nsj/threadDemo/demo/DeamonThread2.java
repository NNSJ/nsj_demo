package com.nsj.threadDemo.demo;

/**
 * 设置了Daemon这个属性t就会变成一个守护线程，当main执行结束以后t也不会再继续执行
 */
public class DeamonThread2 {
    public static void main(String[] args) {
        Thread t = new Thread() {
            @Override
            public void run() {
                try {
                    System.out.println(Thread.currentThread().getName() + "running");
                    Thread.sleep(10_000);
                    System.out.println(Thread.currentThread().getName() + "done");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        t.setDaemon(true);
        t.start();
        System.out.println("main done");
    }

}
