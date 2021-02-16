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
    private static final String HDFS_PATH = "hdfs://ifaithfreedom.cn:8020";
    private static final String HDFS_USER = "hdfs";
    private static FileSystem fileSystem;

    /**
     *   @Description: prepare
     *   @param: []
     *   @return: void
     */
    @Before
    public void prepare() {
        try{
            Configuration configuration = new Configuration();
            configuration.set("dfs.replication","3");
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
     *   @Description: create
     *   @param: []
     *   @return: void
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream out = fileSystem.create(new Path("/hdfs-api/test/a.txt"));
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
