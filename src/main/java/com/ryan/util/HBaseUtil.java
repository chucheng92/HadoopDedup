package com.ryan.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by Ryan Tao on 2017/2/5.
 * HBase Java Util
 *
 * @author Ryan Tao
 * @github lemonjing
 */
public class HBaseUtil {

    private static Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    static Configuration conf = null;

    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "Master.Hadoop,Slave1.Hadoop,Slave2.Hadoop");
    }

    /**
     * create table
     *
     * @param tableName
     * @param family
     * @throws Exception
     */
    public static void createTable(String tableName, String[] family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        for (int i = 0; i < family.length; i++) {
            descriptor.addFamily(new HColumnDescriptor(family[i]));
        }
        if (admin.tableExists(tableName)) {
            logger.info("===table exists===");
            return;
        } else {
            admin.createTable(descriptor);
            logger.info("===create table success===");
        }
    }

    /**
     * get result with specific rowkey
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static Result getResultByRowKey(String tableName, String rowKey) throws IOException {
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, tableName);

        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = hTable.get(get);

        if (null != result) {
            for (KeyValue kv : result.list()) {
                System.out.println("family: " + Bytes.toString(kv.getFamily()));
                System.out.println("qualifier: " + Bytes.toString(kv.getQualifier()));
                System.out.println("value: " + Bytes.toString(kv.getValue()));
                System.out.println("timestamp: " + kv.getTimestamp());
            }
        }
        return result;
    }

    /**
     * get result with specific rowKey, family
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @return
     * @throws IOException
     */
    public static Result getResultByFamily(String tableName, String rowKey, String family) throws IOException {
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, tableName);

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addFamily(Bytes.toBytes(family));

        Result result = hTable.get(get);
        if (null != result) {
            for (KeyValue kv : result.list()) {
                System.out.println("family: " + Bytes.toString(kv.getFamily()));
                System.out.println("qualifier: " + Bytes.toString(kv.getQualifier()));
                System.out.println("value: " + Bytes.toString(kv.getValue()));
                System.out.println("timestamp: " + kv.getTimestamp());
            }
        }
        return result;
    }

    /**
     * get result with specific rowKey, family, qualifier
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @return
     * @throws IOException
     */
    public static Result getResultByQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException {
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, tableName);

        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));

        Result result = hTable.get(get);

        if (null != result) {
            for (KeyValue kv : result.list()) {
                System.out.println("family: " + Bytes.toString(kv.getFamily()));
                System.out.println("qualifier: " + Bytes.toString(kv.getQualifier()));
                System.out.println("value: " + Bytes.toString(kv.getValue()));
                System.out.println("timestamp: " + kv.getTimestamp());
            }
        }
        return result;
    }

    /**
     * judge specific qualifier-value exists
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @param value
     * @return
     * @throws IOException
     */
    @Deprecated
    public static boolean isMatch(String tableName, String rowKey, String family, String qualifier, String value) throws IOException {
        Result result = getResultByQualifier(tableName, rowKey, family, qualifier);

        if (null != value) {
            for (KeyValue kv : result.list()) {
                if (value.equals(Bytes.toString(kv.getValue()))) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * get specific versions result with specific rowKey, family, qualifier
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @return
     * @throws IOException
     */
    public static Result getResultByVersion(String tableName, String rowKey, String family, String qualifier) throws IOException {
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        get.setMaxVersions(5);
        Result result = hTable.get(get);

        if (null != result) {
            for (KeyValue kv : result.list()) {
                System.out.println("family: " + Bytes.toString(kv.getFamily()));
                System.out.println("qualifier: " + Bytes.toString(kv.getQualifier()));
                System.out.println("value: " + Bytes.toString(kv.getValue()));
                System.out.println("timestamp: " + kv.getTimestamp());
            }
        }
        return result;
    }

    /**
     * put(also update) result with specific rowKey, family, qualifier
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @return
     * @throws IOException
     */
    public static void put(String tableName, String rowKey, String family, String qualifier, String value) throws IOException {
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, tableName);

        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
        hTable.put(put);

        logger.info("===put data success.===");
    }

    /**
     * put(also update) result with specific rowKey, family, qualifier
     * batch mode
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @param value
     * @throws IOException
     */
    public static void batchPut(String tableName, String rowKey, String family, String[] qualifier, String[] value) throws IOException {
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, tableName);

        Put put = new Put(Bytes.toBytes(rowKey));

        // 维护列族的信息
        HColumnDescriptor[] hColumnDescriptors = hTable.getTableDescriptor().getColumnFamilies();

        for (int i = 0; i < hColumnDescriptors.length; i++) {
            String familyName = hColumnDescriptors[i].getNameAsString();
            if (familyName.equals(family)) {
                for (int j = 0; j < qualifier.length; j++) {
                    put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier[j]), Bytes.toBytes(value[j]));
                }
            }
        }

        hTable.put(put);

        logger.info("===put data success(batch mode)===");
    }

    /**
     * delete specific qualifier
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void delete(String tableName, String rowKey, String family, String qualifier) throws IOException {
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, Bytes.toBytes(tableName));

        Delete delete = new Delete(Bytes.toBytes(rowKey));

        delete.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        hTable.delete(delete);

        logger.info(family + ":" + qualifier + "is deleted!");
    }

    /**
     * delete all qualifiers
     *
     * @param tableName
     * @param rowKey
     * @throws IOException
     */
    public static void deleteAll(String tableName, String rowKey) throws IOException {
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        hTable.delete(deleteAll);

        logger.info("all qualifiers are deleted.");
    }

    /**
     * delete table
     *
     * @param tableName
     * @throws IOException
     */
    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);

        logger.info(tableName + "is deleted!");
    }

    /**
     * scan table
     *
     * @param tableName
     * @throws IOException
     */
    public static void scan(String tableName) throws IOException {
        Scan scan = new Scan();
        ResultScanner rs = null;
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, tableName);

        try {
            rs = hTable.getScanner(scan);
            for (Result res : rs) {
                for (KeyValue kv : res.list()) {
                    System.out.println("family: " + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier: " + Bytes.toString(kv.getQualifier()));
                    System.out.println("value: " + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp: " + kv.getTimestamp());
                }
            }
        } finally {
            rs.close();
        }
    }

    /**
     * range scan table
     *
     * @param tableName
     * @param startRow
     * @param stopRow
     * @throws IOException
     */
    public static void rangeScan(String tableName, String startRow, String stopRow) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(stopRow));
        ResultScanner rs = null;
        // 客户端可以通过HTable对象与服务端进行CRUD操作
        HTable hTable = new HTable(conf, tableName);

        try {
            rs = hTable.getScanner(scan);
            for (Result res : rs) {
                for (KeyValue kv : res.list()) {
                    System.out.println("family: " + Bytes.toString(kv.getFamily()));
                    System.out.println("qualifier: " + Bytes.toString(kv.getQualifier()));
                    System.out.println("value: " + Bytes.toString(kv.getValue()));
                    System.out.println("timestamp: " + kv.getTimestamp());
                }
            }
        } finally {
            rs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        String tableName = "finger_print";
        String[] family = {"fileFamily", "extFamily"};

        // 1.createTable
        try {
            createTable(tableName, family);
        } catch (Exception e) {
            logger.info("===create table error.===");
            e.printStackTrace();
        }

        // 2.put
//        put(tableName, "rowkey2", family[0], "hashvalue", "FFEEFF1");
//        put(tableName, "rowkey3", family[0], "hashvalue", "FFEEFF1");

        //3.batchPut
//        String[] qualifier = { "id", "hashvalue", "fileNum", "chunkNum"};
//        String[] value = {"1", "FFEEFF2", "1", "1"};
//        batchPut(tableName, "rowkey1", family[0], qualifier, value);

//        getResultByRowKey(tableName, "rowkey0");
//        logger.info("===getResultByRowKey over.===");
//
//        getResultByFamily(tableName, "rowkey1", family[0]);
//        logger.info("===getResultByFamily over.===");
//
//        getResultByQualifier(tableName, "rowkey1", family[0], "hashvalue");
//        logger.info("===getResultByQualifier over.===");
//
//        getResultByVersion(tableName, "rowkey1", family[0], "hashvalue");
//        logger.info("===getResultByVersion over.===");
//
//        scan(tableName);
//        logger.info("===scan over.===");
//
//        rangeScan(tableName, "rowkey1", "rowkey4");
//        logger.info("===rangeScan over.===");
//
//        logger.info("===test over.===");
    }
}
