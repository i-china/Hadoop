package com.hdfs.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLOutput;

/**
 * @author Hale Lv
 * @created 2021-02-17 12:20
 * @project Github
 */
public class copyFromLocalBigFile {
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
            fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration , HDFS_USER);
        }catch (IOException e){
            e.printStackTrace();
        }catch (InterruptedException e){
            e.printStackTrace();
        }catch (URISyntaxException e){
            e.printStackTrace();
        }
    }

    /**
     *   @Description: copyFromLocalBigFile
     *   @param: [] 
     *   @return: void
     */
    @Test
    public void copyFromLocalBigFile() throws IOException {
        File file = new File("F:\\jenkins.mp4");
        final float fileSize = file.length();
        InputStream in = new BufferedInputStream(new FileInputStream(file));
        FSDataOutputStream out = fileSystem.create(new Path("/hdfs-api/hadoop/jenkins.mp4"), new Progressable() {
           long fileCount = 0;

           public void progress(){
               fileCount++;

               System.out.println(" upload progess " + (fileCount * 64 * 1024 / fileSize) * 100 + " %");
           }
        });
        IOUtils.copyBytes(in, out, 4096);
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
