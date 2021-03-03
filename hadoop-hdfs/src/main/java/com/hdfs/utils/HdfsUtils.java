package com.hdfs.utils;

import com.sun.org.glassfish.gmbal.Description;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

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
     *   @Description: rename 文件重命名
     *   @param: [oldPath 旧文件地址, newPath 新文件地址]
     *   @return: boolean
     */
    public boolean rename(String oldPath, String newPath) throws Exception {
        return fileSystem.rename(new Path(oldPath), new Path(newPath));
    }

    /**
     *   @Description: copyFromLocalFiles 上传文件到hdfs中
     *   @param: [localPath 本地文件路径, hdfsPath 存储到hdfs上的路径]
     *   @return: void
     */
    public void copyFromLocalFiles(String localPath, String hdfsPath) throws Exception {
         fileSystem.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
    }

    /**
     *   @Author: iFaithFreedom
     *   @Description: copyToLocalFile 从hdfs下载文件
     *   @param: [hdfsPath ：hdfs的路径, localPath ：本地路径]
     *   @return: void
     *   @Date: 2021-02-15 21:36
     */
    public void copyToLocalFile(String hdfsPath, String localPath) throws Exception {
        fileSystem.copyToLocalFile(new Path(hdfsPath), new Path(localPath));
    }

    /**目录
     *   @Description: listFiles 查询给定路径文件或目录的状态
     *   @param: [path :目录路径]
     *   @return: org.apache.hadoop.fs.FileStatus[] 文件信息的数组
     */
    public FileStatus[] listFiles(String path) throws Exception {
        return fileSystem.listStatus(new Path(path));
    }

    /**
     *   @Description: listFilesRecursive 查询给定路径中文件的状态和快位置
     *   @param: [path :目录或者文件路径, recursive：]
     *   @return: org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> :文件信息的数组
     */
    public RemoteIterator<LocatedFileStatus> listFilesRecursive(String path, boolean recursive) throws Exception {
        return fileSystem.listFiles(new Path(path), recursive);
    }

    /**
     *   @Description: getFileBlockLocation 查看文件块信息
     *   @param: [path 文件路径]
     *   @return: org.apache.hadoop.fs.BlockLocation[] 块信息数组
     */
    public BlockLocation[] getFileBlockLocation(String path) throws Exception {
        FileStatus fileStatus = fileSystem.getFileStatus(new Path(path));
        return fileSystem.getFileBlockLocations(fileStatus, 0 , fileStatus.getLen());
    }

    /**
     *   @Description: delete
     *   @param: [path ：文件路径]
     *   @return: boolean ：删除是否成功
     */
    public boolean delete(String path) throws Exception {
        return fileSystem.delete(new Path(path),true);
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
