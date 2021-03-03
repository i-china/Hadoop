package com.hdfs.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.sound.midi.Soundbank;
import javax.xml.bind.SchemaOutputResolver;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @Author: HaleLv
 * @Date: 2021-02-18
 * @ProjectName Hadoop
 */

public class getFileBlockLocation {
    private static final String HDFS_PATH = "hdfs://ifaithfreedom.cn:8020";
    private static final String HDFS_USER = "hdfs";
    private static FileSystem fileSystem;

    @Before
    public void prepare(){
        try {
            Configuration configuration = new Configuration();
            configuration.set("dfs.replication","1");
            fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,HDFS_USER);
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     *   @Description: listFiles
     *   @param: [] 
     *   @return: void
     */
    @Test
    public void listFiles() throws IOException {
        FileStatus[] status = fileSystem.listStatus(new Path("/hdfs-api"));
        for (FileStatus fileStatus : status) {
            System.out.println(fileStatus.toString());
        }
    }
    
    @Test
    public void getFileBlockLocations() throws IOException {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfs-api/test/a.txt"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
        for (BlockLocation block : blocks){
            System.out.println(block);
        }
    }

    @After
    public void destory(){
        fileSystem = null;
    }
}
