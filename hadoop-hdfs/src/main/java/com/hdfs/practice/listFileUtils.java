package com.hdfs.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @Author: HaleLv
 * @Date: 2021-02-18
 * @ProjectName Hadoop
 */

public class listFileUtils {
    private static final String HDFS_PATH = "hdfs://ifaithfreedom.cn:8020";
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
     *   @Description: listFilesRecursive
     *   @param: [] 
     *   @return: void
     */
    @Test
    public void listFilesRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfs-api"),true);
        while(files.hasNext()){
            System.out.println(files.next());
        }
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
