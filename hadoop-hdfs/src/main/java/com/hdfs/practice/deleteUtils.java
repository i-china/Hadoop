package com.hdfs.practice;

import org.apache.hadoop.conf.Configuration;
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
 * @created 2021-02-17 11:16
 * @project Github
 */
public class deleteUtils {
    private static final String HDFS_PATH = "hdfs://39.106.208.58:8020";
    private static final String HDFS_USER = "hdfs";
    private static FileSystem fileSystem;

    /**
     *   @Description: prepare
     *   @param: []
     *   @return: void
     */
    @Before
    public void prepare(){
        try{
            Configuration configuration = new Configuration();
            configuration.set("dfs.replication","1");
            fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, HDFS_USER);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }

    /**
     *   @Description: delete
     *   @param: [] 
     *   @return: void
     */
    @Test
    public void delete() throws Exception {
        boolean result = fileSystem.delete(new Path("/hdfs-api/test/a.txt"),true);
        System.out.println(result);
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
