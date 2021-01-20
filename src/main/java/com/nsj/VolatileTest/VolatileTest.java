package com.nsj.VolatileTest;

/**
 * https://www.cnblogs.com/xd502djj/p/9873067.html
 */
public class VolatileTest extends Thread {

   volatile boolean flag = false;
    int i = 0;

    public void run() {
        while (!flag) {
            i++;
            //System.out.println("=========="+i);
        }
    }

    public static void main(String[] args) throws Exception {
        VolatileTest vt = new VolatileTest();
        vt.start();
        Thread.sleep(2000);
        vt.flag = true;
        System.out.println("stope" + vt.i);
    }
}