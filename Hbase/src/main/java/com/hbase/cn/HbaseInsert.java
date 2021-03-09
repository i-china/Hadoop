package com.hbase.cn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * @author Hale Lv
 * @project Hadoop
 * @created 2021-03-08 17:29
 */
public class HbaseInsert {
    private HBaseAdmin admin = null;
    private HTable table = null;
    private String tableName = "tb_user";

    /**
     *   @Description: init
     *   @param: [] 
     *   @return: void
     */
    @Before
    public void init() throws IOException {
        // 配置
        Configuration configuration = new Configuration();
        // zookeeper 配置
        configuration.set("hbase.zookeeper.quorum","node3,node4,node5");

        admin = new HBaseAdmin(configuration);
        table = new HTable(configuration, TableName.valueOf(tableName));

    }

    @Test
    public void createTable() throws IOException {
        if(admin.tableExists(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf".getBytes());
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);
    }

    @Test
    public void testInsert() throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put("1001".getBytes());
        put.add("cf".getBytes(),"name".getBytes(),"Hale-first".getBytes());
        put.add("cf".getBytes(),"age".getBytes(), Bytes.toBytes(32));
        put.add("cf".getBytes(),"likes".getBytes(),"coding".getBytes());
        table.put(put);

        put = new Put("1003".getBytes());
        put.add("cf".getBytes(),"name".getBytes(),"Hale-last".getBytes());
        put.add("cf".getBytes(),"age".getBytes(),Bytes.toBytes(31));
        put.add("cf".getBytes(),"likes".getBytes(),"reading".getBytes());
        table.put(put);
    }

    @Test
    public void testScan() throws IOException {
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Result result = null;

        while((result = scanner.next()) != null){
            Cell nameCell = result.getColumnLatestCell("cf".getBytes(), "name".getBytes());
            Cell ageCell = result.getColumnLatestCell("cf".getBytes(), "age".getBytes());
            Cell addressCell = result.getColumnLatestCell("cf".getBytes(), "address".getBytes());
            Cell likesCell = result.getColumnLatestCell("cf".getBytes(), "likes".getBytes());

            System.out.println(Bytes.toString(CellUtil.cloneValue(nameCell)));
            System.out.println(Bytes.toInt(CellUtil.cloneValue(ageCell)));
            System.out.println(Bytes.toString(addressCell == null ? new byte[] {} : CellUtil.cloneValue(addressCell)));
            System.out.println(Bytes.toString(likesCell == null ? new byte[]{} : CellUtil.cloneValue(likesCell)));
        }
        scanner.close();
    }

    public void testGet() throws IOException {
        Get get = new Get("1001".getBytes());
        Result result = table.get(get);
//        printMsg(result);
    }

    @After
    public void destory() throws IOException {
        // 释放资源
        table.close();
        table = null;
        admin.close();
        admin = null;
    }
}
