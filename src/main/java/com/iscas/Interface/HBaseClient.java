package com.iscas.Interface;

/**
 * @Author: lxj
 * @Date: 2019/3/14 15:49
 * @Version 1.0
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.CellUtil;
import java.io.IOException;

public class HBaseClient {
    private static Admin admin = null;
    private static Connection connection;
    private static Configuration configuration;
    public HBaseClient() {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", com.iscas.Configuration.Configuration.HBaseQuorum);
        configuration.set("hbase.zookeeper.property.clientPort", com.iscas.Configuration.Configuration.HBasePort);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    /**
     * 向HBase中插入数据
     * @param tableName 表名
     * @param rowkey 行键
     * @param colFamily 列簇
     * @param col 列名
     * @param val 值
     * @Version 1.0
     */
    public boolean writeToHBase(String tableName, String rowkey, String colFamily, String col, String val) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(col), Bytes.toBytes(val));
        table.put(put);
        table.close();
        return true;
    }
    /**
     * 向HBase读取数据
     * @param tableName 表名
     * @param rowkey 行键
     * @param colFamily 列簇
     * @param col 列名
     * */
    public String readFromHBase(String tableName, String rowkey, String colFamily, String col){
        String value = "";
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            get.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
            Result result = table.get(get);
            for  (Cell cell : result.rawCells()) {
                value = Bytes.toString(CellUtil.cloneValue(cell));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }
}
