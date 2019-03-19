package com.iscas.Interface;
import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import java.io.InputStream;
import java.util.Date;
/**
 * @Author: lxj
 * @Date: 2019/3/18 11:02
 * @Version 1.0
 */
public class FTPInterface {
    /**
     * 将FTP上的文件上传到HDFS
     * @param FTPPath FTP文件路径
     * @param HDFSPath HDFS文件路径 以/结尾
     * @Version 1.0
     */
    public boolean loadFromFtpToHdfs(String FTPPath,String HDFSPath) {
        FTPClient ftp = new FTPClient();
        InputStream inputStream = null;
        FSDataOutputStream outputStream = null;
        boolean flag = true;
        try {
            ftp.connect(com.iscas.Configuration.Configuration.FTPURL, com.iscas.Configuration.Configuration.FTPPort);
            boolean isConnect = ftp.login(com.iscas.Configuration.Configuration.FTPUserName, com.iscas.Configuration.Configuration.FTPPassword);
            if (isConnect) {
                ftp.setFileType(FTP.BINARY_FILE_TYPE);
                ftp.setControlEncoding("UTF-8");
                int reply = ftp.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    ftp.disconnect();
                }
                FTPFile[] files = ftp.listFiles(FTPPath);
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", com.iscas.Configuration.Configuration.HDFSURL);
                conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                FileSystem hdfs = FileSystem.get(conf);
                for (FTPFile file : files) {
                    if (!(file.getName().equals(".") || file.getName().equals(".."))) {
                        inputStream = ftp.retrieveFileStream(FTPPath + file.getName());
                        outputStream = hdfs.create(new Path("HDFSPath" + file.getName()));
                        IOUtils.copyBytes(inputStream, outputStream, conf, false);
                        if (inputStream != null) {
                            inputStream.close();
                            ftp.completePendingCommand();
                        }
                    }
                }
                ftp.disconnect();
            }
        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        }
        return flag;
    }
    public static void main(String[] args) {
        FTPInterface ftp = new FTPInterface();
        System.out.println(new Date());
        System.out.println(ftp.loadFromFtpToHdfs("/test","/APITest1/"));
        System.out.println(new Date());
    }
}
