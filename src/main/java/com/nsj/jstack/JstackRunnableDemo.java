package com.nsj.jstack;

import java.lang.management.ManagementFactory;

public class JstackRunnableDemo {
    public static void main(String[] args) {
        System.out.println(JstackRunnableDemo.pid());
      //  runnable();     // 1
   //   blocked();      // 2
        timedWaiting();
   //  waiting();      // 3
//     timedWaiting(); // 4
    }

    public static String pid() {
        String name = ManagementFactory.getRuntimeMXBean().getName();
        return name.split("@")[0];
    }
    public static void runnable() {
        long i = 0;
        while (true) {
            i++;
        }
    }

    public static void timedWaiting() {
        final Object lock = new Object();
        synchronized (lock) {
            try {
                lock.wait(30 * 1000);
            } catch (InterruptedException e) {
            }
        }
    }

    public static void blocked() {
        System.out.println("block演示");
        final Object lock = new Object();
        new Thread() {
            public void run() {
                synchronized (lock) {
                    System.out.println("i got lock, but don't release");
                    try {
                        Thread.sleep(1000L * 1000);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }.start();

        try {
            Thread.sleep(100);
        }
        catch (InterruptedException e) {

        }

        synchronized (lock) {
            try {
                System.out.println("主线程");
                Thread.sleep(30 * 1000);
            } catch (InterruptedException e) {
            }
        }
    }

}
