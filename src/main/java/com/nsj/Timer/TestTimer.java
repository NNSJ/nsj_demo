package com.nsj.Timer;

import java.util.*;
import java.text.SimpleDateFormat;

public class TestTimer {
    private int count=0;
    private ArrayList<String> userBox=new ArrayList<String>();
    {
        userBox.add("a");
        userBox.add("b");
        userBox.add("c");
        userBox.add("d");
    }

    public void test() throws Exception{
        Timer time=new Timer();//记录
        SimpleDateFormat s=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date firstTime=s.parse("2020-06-05 22:36:00");
        time.schedule(new TimerTask(){
            public void run(){
                for(int i=0;i<userBox.size();i++){
                    System.out.println("给"+userBox.get(i)+"发送消息：垃圾;第"+(++count)+"条");
                }
                System.out.println("做坏事了");
            }
        },firstTime,10000);
    }

    public static void main(String[] args){
        try{
            TestTimer tt=new TestTimer();
            tt.test();
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
