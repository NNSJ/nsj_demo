package com.nsj.hbaseDemo;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

public class HbaseCoprocessor extends BaseRegionObserver {

    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e,
                       final Put put, final WALEdit edit, final boolean writeToWAL)
            throws IOException {
        // set configuration
        Configuration conf = new Configuration();
        // need conf.set...
        String colName = "columnName";
        HTable table = new HTable(conf, "indexTableName");
        List<Cell> kv = put.get("familyName".getBytes(), colName.getBytes());
        Iterator<Cell> kvItor = kv.iterator();
        while (kvItor.hasNext()) {
            Cell tmp = kvItor.next();
            Put indexPut = new Put(tmp.getValue());
            indexPut.add("familyName".getBytes(), "columnName".getBytes(),
                    tmp.getRow());
            table.put(indexPut);
        }
        table.close();
    }


    public static void main(String[] args) {
        System.out.println("二级索引");
    }
}
