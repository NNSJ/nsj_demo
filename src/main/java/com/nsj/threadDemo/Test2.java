package com.nsj.threadDemo;

/**
 * 如果每个线程执行的代码的代码不同，通过 内部类 实现多个线程共享数据
 */
public class Test2 {
    public static void main(String[] args) {
        Data data = new Data();
        Runnbale1 runnbale1 = new Runnbale1(data);
        Runnbale2 runnbale2 = new Runnbale2(data);
        new Thread(runnbale2).start();
        new Thread(runnbale1).start();

    }

    private static class Runnbale1 implements Runnable {
        private Data data;

        public Runnbale1(Data data) {
            this.data = data;
        }


        public void run() {
            int plus = data.plus();
            System.out.println(Thread.currentThread().getName() + "====" + plus);
        }
    }

    private static class Runnbale2 implements Runnable {

        private Data data;

        public Runnbale2(Data data) {
            this.data = data;
        }

        public void run() {
            int reduce = data.reduce();
            System.out.println(Thread.currentThread().getName() + "---" + reduce);
        }
    }

    /**
     * 共享数据内部类方式：注意加锁
     */
    private static class Data {

        private int i = 50;

        public synchronized int plus(){
            return i=i+1;
        }


        public synchronized int reduce(){
            return i=i-1;
        }
    }

}
