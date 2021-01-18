package com.nsj.hbaseDemo;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @Author: lih
 * @Date: 2019/5/16 9:31 AM
 * @Version 1.0
 * 自定义协处理器
 */
public class MyRegionProcessor extends BaseRegionObserver {

    public MyRegionProcessor() {
        log("new MyRegionProcessor");
    }

    public void start(CoprocessorEnvironment e)  {
        log("start:" + this.toString());
    }

    public void stop(CoprocessorEnvironment e)  {
        log("stop:" + this.toString());
    }


    public void log(String log) {
        try {
            FileOutputStream fos = new FileOutputStream("/opt/apps/hbase_logs/hbase.log", true);
            fos.write((log + "\r\n").getBytes());
            fos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {

        }
    }

    public static void main(String[] args) {
        System.out.println("协处理器实现");
    }

}