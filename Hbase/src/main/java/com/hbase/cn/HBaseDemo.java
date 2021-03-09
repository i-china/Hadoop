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
    // 代表了对数据库的管理类
    private HBaseAdmin admin = null;
    // 对表的数据进行操作的类
    private HTable table = null;

    private String tableName = "tb_user";

    /**
     *   @Description: init
     *   @param: [] 
     *   @return: void
     */
    @Before
    public void init() throws IOException {

        Configuration conf = new Configuration();
        //  指定zookeeper的位置
        conf.set("hbase.zookeeper.quorum","node3,node4,node5");

        admin = new HBaseAdmin(conf);
//        table = new HTable(conf, TableName);
//        table = new HTable(conf, TableName.getBytes());
        table = new HTable(conf, TableName.valueOf(tableName));
    }

    /**
     *   @Description: createTable
     *   @param: [] 
     *   @return: void
     */
    @Test
    public void createTable() throws IOException {
        // 检查是否存在该表
        if(admin.tableExists(tableName)){
            // 如果存在该表，则禁用
            admin.disableTable(tableName);
            // 删除该表
            admin.deleteTable(tableName);
        }
        // 表的描述对象
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        // 列族的描述对象
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("cf".getBytes());
        // 给表设置列族
        tableDescriptor.addFamily(columnDescriptor);
        // 通过管理类，创建表
        admin.createTable(tableDescriptor);
    }

    /**
     *   @Description: destory
     *   @param: [] 
     *   @return: void
     */
    @After
    public void destory() throws IOException {
        table.close();
        table = null;
        admin.close();
        admin = null;
    }
}
