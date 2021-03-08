package com.hbase.cn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


/**
 * @author Hale Lv
 * @created 2021-03-08 16:44
 * @project Hadoop
 */
public class HBaseDemo {
    private HBaseAdmin admin = null;
    private HTable table = null;
    private String tableName = "tb_user";

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","node3,node4,node5");

        admin = new HBaseAdmin(conf);
//        table = new HTable(conf, TableName);
//        table = new HTable(conf, TableName.getBytes());
        table = new HTable(conf, TableName.valueOf(tableName));
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







    @After
    public void destory() throws IOException {
        table.close();
        table = null;
        admin.close();
        admin = null;
    }
}
