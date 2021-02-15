package com.hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author HaleLv
 * @date 2021-02-15
 **/
// Hdfs 工具类
public class HdfsUtils {
    private static final String HDFS_PATH = "hdfs://39.106.208.58:8020";
    private static final String HDFS_USER = "hdfs";
    private static FileSystem fileSystem;

    static {
        try{
            Configuration configuration = new Configuration();
            configuration.set("dfs.replication","3");
            fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,HDFS_USER);
        }catch (IOException e){
            e.printStackTrace();
        } catch (InterruptedException e){
            e.printStackTrace();
        }catch (URISyntaxException e){
            e.printStackTrace();
        }
    }

    public static FileSystem getFileSystem(){
        return fileSystem;
    }

//     mkdir 创建目录
    public static boolean mkdir(String path) throws Exception {
        return fileSystem.mkdirs(new Path(path));
    }

//    查看文件内容
    public static String text(String path, String encode) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        return inputStreamToString(inputStream,encode);
    }

//    将输入流转换为指定字符
    private static String inputStreamToString(InputStream inputStream, String encode) {
        try{
            if(encode == null || ("".equals(encode))) {
                encode = "utf-8";
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, encode));
            StringBuffer builder = new StringBuffer();
            String str = "";

        }catch (IOException e){
            e.printStackTrace();
        }
    }

}
