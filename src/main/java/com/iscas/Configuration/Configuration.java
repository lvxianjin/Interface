package com.iscas.Configuration;

/**
 * @Author: lxj
 * @Date: 2019/3/19 9:46
 * @Version 1.0
 */
public class Configuration {
    public static final String RedisURL = "";
    public static final int RedisPort = 6379;
    public static final String HDFSURL = "hdfs://master:9000";
    public static final String Alluxio = "";
    public static final String FTPURL = "10.0.0.122";
    public static final int FTPPort = 21;
    public static final String FTPUserName = "AKITO";
    public static final String FTPPassword = "626629";
    public static final String HBaseQuorum = "master,slave1,slave2"; //HBase中zookeeper集群
    public static final String HBasePort = "2181";
    public static final String AlluxioURL = "alluxio://172.17.0.5:19998/";
}
