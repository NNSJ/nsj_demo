package com.nsj.hyperdrive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hyperbase.client.HyperbaseAdmin;
import org.apache.hadoop.hyperbase.client.HyperbaseHTable;
import org.apache.hadoop.hyperbase.datatype.HDataType;
import org.apache.hadoop.hyperbase.datatype.PrimaryDataType;
import org.apache.hadoop.hyperbase.datatype.StructHDataType;
import org.apache.hadoop.hyperbase.metadata.HyperbaseMetadata;
import org.apache.hadoop.hyperbase.metadata.schema.HyperbaseTableSchema;
import org.apache.hadoop.hyperbase.util.SchemaUtil;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.List;

/**
 * Created by vincent on 18-4-12.
 */
public class HyperdriveReader {

    private Configuration conf;
    private HyperbaseAdmin admin;
    private HyperbaseHTable hyperbaseHTable;

    /**
     *
     create table testhyperdrive2(
     key struct<ti:tinyint,si:smallint,i:int,bi:bigint,f:float,dl:double,dc:decimal(20,10),str:varchar(30),ts:timestamp,tsgp:string,strChina:string,dt:date,dtgp:string,bl:boolean>,
     value struct<ti:tinyint,si:smallint,i:int,bi:bigint,f:float,dl:double,dc:decimal(20,10),str:varchar(30),ts:timestamp,tsgp:string,strChina:string,dt:date,dtgp:string,bl:boolean>,
     ti tinyint,
     si smallint,
     i int,
     bi bigint,
     f float,
     dl double,
     --dc double,
     dc decimal(20,10),
     str varchar(30),
     ts timestamp,
     tsgp string,
     strChina string,
     dt date,
     dtgp string,
     bl boolean
     )
     row format delimited collection items terminated by '|'
     stored by 'io.transwarp.hyperdrive.HyperdriveStorageHandler'
     with serdeproperties('hbase.columns.mapping'=':key,f:q1,f:q2,f:q3,f:q4,f:q5,f:q6,f:q7,f:q8,f:q9,f:q10,f:q11,f:q12,f:q13,f:q14,f:q15',
     'hyperdrive.structstring.length.default'='10',
     'hyperdrive.structstring.length.key.ts'='20',
     'hyperdrive.structstring.length.key.tsgp'='20',
     'hyperdrive.structstring.length.key.strChina'='10',
     'hyperdrive.structstring.length.key.dt'='10',
     'hyperdrive.structstring.length.key.dtgp'='10',
     'hyperdrive.structstring.length.value.ts'='20',
     'hyperdrive.structstring.length.value.tsgp'='20',
     'hyperdrive.structstring.length.value.strChina'='10',
     'hyperdrive.structstring.length.value.dt'='10',
     'hyperdrive.structstring.length.value.dtgp'='10'
     );
     *
     *
     insert into table testhyperdrive2 (key,value,ti,si,i,bi,f,dl,dc,str,ts,tsgp,strChina,dt,dtgp,bl) values (named_struct('ti', cast('0' as tinyint),'si', cast('0' as smallint),'i', 123,'bi', cast('418709503' as bigint),'f', cast('4567.0' as float),'dl', cast('3223.7090277422876' as double),'dc', cast('5270.0565989899' as decimal(20,10)),'str', cast('UGTvi' as varchar(30)),'ts', cast('2006-12-30 05:34:25' as timestamp),'tsgp', '2002-01-19 03:31:21','strChina', '时常挂着笑的人，心里也许有无声的泪；','dt', cast('2010-01-07' as date),'dtgp', '2002-01-19','bl', false),named_struct('ti', cast('0' as tinyint),'si', cast('0' as smallint),'i', 123,'bi', cast('418709503' as bigint),'f', cast('4567.0' as float),'dl', cast('3223.7090277422876' as double),'dc', cast('5270.0565989899' as decimal(20,10)),'str', cast('UGTvi' as varchar(30)),'ts', cast('2006-12-30 05:34:25' as timestamp),'tsgp', '2002-01-19 03:31:21','strChina', '时常挂着笑的人，心里也许有无声的泪；','dt', cast('2010-01-07' as date),'dtgp', '2002-01-19','bl', false),cast('0' as tinyint),cast('0' as smallint),123,cast('418709503' as bigint),cast('4567.0' as float),cast('3223.7090277422876' as double),cast('5270.0566' as double),cast('UGTvi' as varchar(30)),'2006-12-30 05:34:25','2002-01-19 03:31:21','时常挂着笑的人，心里也许有无声的泪；','2010-01-07','2002-01-19',false);
     */

    private final TableName tableName = TableName.valueOf("testhyperdrive4");
    private static final byte[] F_1 = Bytes.toBytes("f");
    private static final byte[] Q_1 = Bytes.toBytes("q1");
    private static final byte[] Q_2 = Bytes.toBytes("q2");
    private static final byte[] Q_3 = Bytes.toBytes("q3");
    private static final byte[] Q_4 = Bytes.toBytes("q4");
    private static final byte[] Q_5 = Bytes.toBytes("q5");
    private static final byte[] Q_6 = Bytes.toBytes("q6");
    private static final byte[] Q_7 = Bytes.toBytes("q7");
    private static final byte[] Q_8 = Bytes.toBytes("q8");
    private static final byte[] Q_9 = Bytes.toBytes("q9");
    private static final byte[] Q_10 = Bytes.toBytes("q10");
    private static final byte[] Q_11 = Bytes.toBytes("q11");
    private static final byte[] Q_12 = Bytes.toBytes("q12");
    private static final byte[] Q_13 = Bytes.toBytes("q13");
    private static final byte[] Q_14 = Bytes.toBytes("q14");
    private static final byte[] Q_15 = Bytes.toBytes("q15");

    public HyperdriveReader() throws IOException, KeeperException, InterruptedException {
        this.conf = HBaseConfiguration.create();
        this.admin = new HyperbaseAdmin(this.conf);
        this.hyperbaseHTable = new HyperbaseHTable(this.conf, this.tableName);


    }

    public void get(String rowKey) throws IOException {
        HyperbaseMetadata hyperbaseMetadata = this.admin.getTableMetadata(this.tableName);
        Result result = this.hyperbaseHTable.get(SchemaUtil.genGet(hyperbaseMetadata, rowKey));
        this.printResult(result, hyperbaseMetadata);
    }

    public void get(String[] rowKeyValues) throws IOException {
        HyperbaseMetadata hyperbaseMetadata = this.admin.getTableMetadata(this.tableName);
        Object row = convertStructType(hyperbaseMetadata, rowKeyValues, null, null);
        byte[] binRow = SchemaUtil.getBinValue(hyperbaseMetadata, null, null, row, true);
        Result result = this.hyperbaseHTable.get(new Get(binRow));
        this.printResult(result, hyperbaseMetadata);
    }

    public void scan(String startRow, String endRow) {

    }

    private static Object[] convertStructType(HyperbaseMetadata metadata, String[] values, byte[] family,
                                              byte[] qualifier) throws IOException {
        Object[] objs = new Object[values.length];
        HyperbaseTableSchema schema = metadata.getSchema();
        HDataType type = null;
        if (family != null && qualifier != null) {
            type = schema.getColumnType(family, qualifier);
        } else {
            type = schema.getRowKey();
        }

        List<Pair<PrimaryDataType, Integer>> types = null;
        if (type instanceof StructHDataType) {
            StructHDataType structHDataType = (StructHDataType)type;
            types = structHDataType.getDataTypes();
        } else {
            throw new IOException(Bytes.toString(family) + ":" + Bytes.toString(qualifier) +
                    " is not struct type in schema");
        }

        if (types != null && types.size() != values.length) {
            throw new IOException("value does not match schema of this struct type!");
        }

        for (int i = 0; i < values.length; i++) {
            objs[i] = toJavaObject(types.get(i).getFirst(), values[i].trim());
        }
        return objs;
    }

    private static Object toJavaObject(HDataType type, String value) throws IOException {
        try {
            switch (type.getDataType()) {
                case BOOLEAN:
                    if (value.equals("true") || value.equals("false")) {
                        return Boolean.valueOf(value);
                    } else {
                        throw new IOException("Invalid boolean type value, must be true or false.");
                    }
                case BINARY:
                    return Bytes.toBytes(value);
                case TINYINT:
                    return Byte.valueOf(value);
                case SMALLINT:
                    return Short.valueOf(value);
                case INTEGER:
                    return Integer.valueOf(value);
                case BIGINT:
                case DATE:
                case TIMESTAMP:
                    return Long.valueOf(value);
                case DOUBLE:
                    return Double.valueOf(value);
                case FLOAT:
                    return Float.valueOf(value);
                case STRING:
                    return String.valueOf(value);
                case VARCHAR:
                    return String.valueOf(value);
                case DECIMAL:
                    return new BigDecimal(value);
                default:
                    throw new UnknownError("Not found data type");
            }
        } catch (Exception e){
            throw new IOException("Invalid value, " + e.getClass());
        }
    }

    private static void printResult(Result result, HyperbaseMetadata hyperbaseMetadata) throws IOException {
        System.out.println(SchemaUtil.getValues(hyperbaseMetadata, null, null, result.getRow()));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_1), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_2), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_3), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_4), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_5), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_6), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_7), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_8), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_9), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_10), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_11), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_12), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_13), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_14), hyperbaseMetadata));
        System.out.println(SchemaUtil.toStringCell(result.getColumnLatestCell(F_1, Q_15), hyperbaseMetadata));
    }

    public void close() throws IOException {
        this.admin.close();
        this.hyperbaseHTable.close();
    }

    public static void main(String[] args) throws InterruptedException {
        try {
            HyperdriveReader hyperbaseReader = new HyperdriveReader();
            hyperbaseReader.get(new String[] {
                    "0", "0", "123", "418709503", "4567.0", "3223.7090277422876", "5270.0565989899", "UGTvi",
                    "2006-12-30 05:34:25", "2002-01-19 03:31:21", "时常挂着笑的人，心里也许有无声的泪；", "2010-01-07",
                    "2002-01-19", "false"
            });

            hyperbaseReader.close();
//            System.exit(0);
//            hyperbaseReader.scan("001", "005");
//            while(true) {
//                Thread.sleep(1000);
//            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

}
