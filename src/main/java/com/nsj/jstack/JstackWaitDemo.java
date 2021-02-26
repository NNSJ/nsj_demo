package com.nsj.jstack;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class JstackWaitDemo {
    public static Executor executor = Executors.newFixedThreadPool(5);
    public static Object lock = new Object();

    public static void main(String[] args) {

        JstackWaitDemo test = new JstackWaitDemo();
        System.out.println("debug...........");
        try
        {
            synchronized (test)
            {
                test.wait();
            }
        }
        catch (InterruptedException e)
        {
            e.printStackTrace();
        }


//     Task task1 = new Task();
//     Task task2 = new Task();
//     executor.execute(task1);
//     executor.execute(task2);
    }


    static class Task implements Runnable{
        @Override
        public void run(){
         synchronized (lock){
          calculate();
}
        }
        public void calculate(){
            int i = 0;
            while(true){
                i++;
                if(i % 10000 ==0){
                System.out.println("i==========="+i);
                }

            }
        }
    }





}
