package com.hdfs.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author Hale Lv
 * @created 2021-02-16 11:04
 * @project Github
 */
public class createFile {
    public static final String HDFS_PATH = "hdfs://bigdata003:8020";
    public static final String HDFS_USER = "hdfs";
    public static FileSystem fileSystem;

    /**
     *   @Description: prepare
     *   @param: []
     *   @return: void
     */
    @Before
    public void prepare() {
        try{
            Configuration configuration = new Configuration();
            configuration.set("dfs.replication","1");
            fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (URISyntaxException e){
            e.printStackTrace();
        }
    }


    /**
     *   @Description: mkdir
     *   @param: []
     *   @return: void
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfs-api/hadoop/"));
    }


    /**
     *   @Description: create
     *   @param: []
     *   @return: void
     */
    @Test
    public void create() throws Exception {
        // 如果文件存在，默认会覆盖, 可以通过第二个参数进行控制。第三个参数可以控制使用缓冲区的大小
        FSDataOutputStream out = fileSystem.create(new Path("/hdfs-api/test/a.txt"),
                true, 4096);
        out.write("hello hadoop!".getBytes());
        out.write("hello spark!".getBytes());
        out.write("hello flink!".getBytes());
        // 强制将缓冲区中内容刷出
        out.flush();
        out.close();
    }


    /**
     *   @Description: exists
     *   @param: [] 
     *   @return: void
     */
    @Test
    public void exists() throws IOException {
        boolean exists = fileSystem.exists(new Path("/hdfs-test/"));
        System.out.println(exists);
    }

    /**
     *   @Description: destory
     *   @param: []
     *   @return: void
     */
    @After
    public void destory(){
        fileSystem = null;
    }

}
