package com.nsj.threadDemo.demo;

/**
 * 这段代码定义一个线程t，执行main函数的时候，main线程会先退出，而t线程因为定义了休眠10s所以main线程执行完退出的时候它还是在继续执行。直到t执行完成整个application才会完全退出
 */
public class DeamonThread {
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
        t.start();
        System.out.println("main done");
    }

        }
