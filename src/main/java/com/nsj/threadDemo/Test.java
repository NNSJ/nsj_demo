package com.nsj.threadDemo;

public class Test {

    static int j = 0;

    public static void main(String[] args) {
        Runnbale1 runnbale1 = new Runnbale1();
        new Thread(runnbale1).start();
        new Thread(runnbale1).start();
        new Thread(runnbale1).start();
        new Thread(runnbale1).start();
        new Thread(runnbale1).start();

    }

    private static class Runnbale1 implements Runnable{

        public void run() {
            j++;
            System.out.println(Thread.currentThread().getName()+"---"+j);
        }
    }
}
