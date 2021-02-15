package com.hdfs.utils;

import com.sun.org.glassfish.gmbal.Description;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
 * @param
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

    /**
     *   @Author: iFaithFreedom
     *   @Description: mkdir 创建文件夹，支持递归创建
     *   @param: [path : 本地地址]
     *   @return: boolean
     *   @Date: 2021-02-15 21:13
     */
    public static boolean mkdir(String path) throws Exception {
        return fileSystem.mkdirs(new Path(path));
    }

    /**
     *   @Author: iFaithFreedom
     *   @Description:
     *   @param: path 路径地址
     *   @return: 返回文件内容字符串
     *   @Date: 2021-02-15 20:55
     */
    public static String text(String path, String encode) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path(path));
        return inputStreamToString(inputStream,encode);
    }

    /**
     *   @Author: iFaithFreedom
     *   @Description: createAndWrite 创建文件并写入
     *   @param: [path 路径地址, context：文件内容]
     *   @return: void 
     *   @Date: 2021-02-15 21:04
     */
    public void createAndWrite(String path, String context) throws Exception{
        FSDataOutputStream out = fileSystem.create(new Path(path));
        out.write(context.getBytes());
        out.flush();
        out.close();
    }

    /**
     *   @Author: iFaithFreedom
     *   @Description: rename 文件重命名
     *   @param: [oldPath 旧文件地址, newPath 新文件地址]
     *   @return: boolean
     *   @Date: 2021-02-15 21:15
     */
    public boolean rename(String oldPath, String newPath) throws Exception {
        return fileSystem.rename(new Path(oldPath), new Path(newPath));
    }

  
    /**
     *   @Author: iFaithFreedom
     *   @Description: inputStreamToString
     *   @param: [inputStream, encode] 
     *   @return: java.lang.String
     *   @Date: 2021-02-15 21:08 
     */
    private static String inputStreamToString(InputStream inputStream, String encode) {
        try{
            if(encode == null || ("".equals(encode))) {
                encode = "utf-8";
            }
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, encode));
            StringBuffer builder = new StringBuffer();
            String str = "";
            while((str = reader.readLine()) != null){
                builder.append(str).append("\n");
            }
        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }

}
