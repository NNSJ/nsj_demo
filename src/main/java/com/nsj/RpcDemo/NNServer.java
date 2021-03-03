package com.nsj.RpcDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/*
实现通信接口，然后new一个服务器的实例
 */
public class NNServer implements RPCProtocol {
    public static void main(String[] args) throws IOException {

        RPC.Server server=new RPC.Builder(new Configuration())
                .setBindAddress("localhost")
                .setPort(8888)
                .setProtocol(RPCProtocol.class)
                .setInstance(new NNServer()).build();
        server.start();


    }
    @Override
    public void mkdirs(String path) {
        System.out.println("服务器端收到了客户端的请求");
    }
}
