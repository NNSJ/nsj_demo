package com.nsj.hbaseDemo;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
public class TestCoprocessor extends BaseRegionObserver {
    static Configuration config = HBaseConfiguration.create();
    static HTable table = null;
    static{
        config.set("hbase.zookeeper.quorum",
                "192.168.146.4:2181");
        try {
            table = new HTable(config, "guanzhu");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e,
                       Put put, WALEdit edit, Durability durability) throws IOException {
// super.prePut(e, put, edit, durability);
        byte[] row = put.getRow();
        Cell cell = put.get("f1".getBytes(), "from".getBytes()).get(0);
        Put putIndex = new
                Put(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
        putIndex.addColumn("f1".getBytes(), "from".getBytes(), row);
        table.put(putIndex);
        table.close();
    }
    public static void main(String[] args) {
        System.out.println("二级索引11");
    }
}