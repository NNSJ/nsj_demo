package com.nsj.threadDemo;

public class SellTicket {
    /**
     * 线程共享实例： 每个线程执行的代码相同，可以使用同一个Runnable对象
     * @param args
     */
    public static void main(String[] args) {
        Ticket t = new Ticket();
        new Thread(t).start();
        new Thread(t).start();

//        Ticket t = new Ticket();
//        new Thread(t).start();
//        new Thread(t).start();
    }
}
class Ticket implements Runnable{

    private int ticket = 10;

    public   void run() {
        while(ticket>0){

            System.out.println(Thread.currentThread().getName()+"=========="+"当前票数为："+ticket);
            ticket--;
        }

    }
}
