package com.iscas.Interface;

/**
 * @Author: lxj
 * @Date: 2019/3/14 9:29
 * @Version 1.0
 */
import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
public class HDFSInterface {
    FileSystem fs = null;
    Configuration conf =null;
    public HDFSInterface() {
        try {
            conf = new Configuration();
            conf.set("fs.defaultFS", com.iscas.Configuration.Configuration.HDFSURL);
            conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 将本地文件上传到HDFS
     * @param localPath  要上传的文件所在的本地路径
     * @param HDFSPath   要上传到HDFS的目标路径
     * @throws Exception
     */
    public void AddFileToHdfs(String localPath,String HDFSPath) throws Exception {
        Path src = new Path(localPath);
        Path dst = new Path(HDFSPath);
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }
    /**
     * 查看文件夹的文件及文件夹信息
     * @param HDFSPath HDFS路径
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     * @para HDFSPath HDFS上的文件夹路径
     */
    public void testListAll(String HDFSPath) throws FileNotFoundException, IllegalArgumentException, IOException {
        FileStatus[] listStatus = fs.listStatus(new Path(HDFSPath));
        String flag = "";
        for (FileStatus fstatus : listStatus) {
            if (fstatus.isFile()) {
                flag = "f-- ";
            } else {
                flag = "d-- ";
            }
            System.out.println(flag + fstatus.getPath().getName());
            System.out.println(fstatus.getPermission());
        }
    }
    /**
     * 将文件流写入HDFS
     * @param in 输入流
     * @param HDFSPath HDFS路径
     * @throws IOException
     * @throws IllegalArgumentException
     * @throws FileNotFoundException
     * @para HDFSPath HDFS上的文件夹路径
     */
    public boolean writeToHDFS(InputStream in,String HDFSPath){
        //create()方法表示新创建一个文件，如果想在一个文件上追加，请用append()方法。
        try {
            FSDataOutputStream out = fs.create(new Path(HDFSPath));
            IOUtils.copyBytes(in, out, conf, false);
        } catch (IOException e) {
            return false;
        }
        return true;
    }
}
