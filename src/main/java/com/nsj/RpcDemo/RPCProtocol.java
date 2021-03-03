package com.nsj.RpcDemo;

/**
 * RPC协议接口，versionID （版本号）必须有
 */
public interface RPCProtocol {
    long versionID=666;
    void mkdirs(String path);
}
