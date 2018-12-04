package com.yaoge.putao.study.hbase;

import com.google.common.collect.Lists;
import com.yaoge.putao.study.common.LoadProperties;
import com.yaoge.putao.study.module.HbaseRow;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class HbaseClient {

    private static final Logger logger = LoggerFactory.getLogger(HbaseClient.class);

    private static Configuration configuration;
    private static Connection connection;
    private static Table table;

    //init
    static {
        String zookeepers = LoadProperties.getPropertityValue("hbase.zookeeper.quorum");
        String hbaseMaster = LoadProperties.getPropertityValue("hbase.master");
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", zookeepers);
        configuration.set("hbase.master", hbaseMaster);
        try {
            connection = ConnectionFactory.createConnection(configuration);
            logger.info("init connection success");
        } catch (IOException e) {
            logger.error("init connection fail", e);
        }
    }

    // 获得连接
    public static Connection getCon() {
        if (connection == null || connection.isClosed()) {
            try {
                connection = ConnectionFactory.createConnection(configuration);
                logger.info("create connection success");
            } catch (IOException e) {
                logger.error("create connection fail", e);
            }
        }
        return connection;
    }

    // 关闭连接
    public static void close() {
        if (connection != null) {
            try {
                connection.close();
                logger.error("close hbase connection success");
            } catch (IOException e) {
                logger.error("close hbase connection fail", e);

            }
        }
    }


    //获得表
    private static Table getTable(String tableName) {
        try {
            Admin admin = getCon().getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                return null;
            }
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            logger.error("get table fail ", e);
        }
        return table;
    }

    //创建表
    public void createTable(String tableName, String... familyNames) {
        if (StringUtils.isBlank(tableName) || familyNames == null) {
            return;
        }
        try {
            Admin admin = getCon().getAdmin();
            if (!admin.tableExists(TableName.valueOf(tableName))) {
                return;
            }
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            admin.createTable(tableDescriptor);

            admin.close();
        } catch (IOException e) {
            logger.error("create table fail ", e);
        }
    }

    //判断一个对象的所有字段是否为空
    public static <T> boolean checkObjectAllFields(T t) {
        if (t == null) {
            return false;
        }
        String object = ToStringBuilder.reflectionToString(t, ToStringStyle.SIMPLE_STYLE);
        //long bankNums = Splitter.on(",").trimResults().splitToList(object).stream().filter(filed -> StringUtils.isBlank(filed)).count();
        //System.out.println(bankNums);
        // return bankNums > 0 ? false : true;
        return false;
    }


    public void put(HbaseRow hbaseRow) {
        if (checkObjectAllFields(hbaseRow)) {
            table = getTable(hbaseRow.getTableName());
            Put put = new Put(Bytes.toBytes(hbaseRow.getRowKey()));
            put.addColumn(Bytes.toBytes(hbaseRow.getFamilyName()), Bytes.toBytes(hbaseRow.getColumnName()), Bytes.toBytes(hbaseRow.getColumnValue()));
            try {
                table.put(put);
            } catch (IOException e) {
                logger.error("put hbaseRow fail ", e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }


//    public Map<String, String> getOneRow(String tableName, String rowkey) {
//        Map<String, String> map = new HashMap<String, String>();
//        try {
//            Table table = getTable(tableName);
//            Get get = new Get(Bytes.toBytes(rowkey));
//            Result result = table.get(get);
//            for (Cell cell : result.rawCells()) {
//                byte[] f = CellUtil.cloneFamily(cell);
//                byte[] q = CellUtil.cloneQualifier(cell);
//                byte[] v = CellUtil.cloneValue(cell);
//                String f_q = Bytes.toString(f) + ":" + Bytes.toString(q);
//                String value = Bytes.toString(v);
//                map.put(f_q, value);
//            }
//            table.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return map;
//    }

    public long rowCount(String tableName) {
        long count = 0;
        try {
            Table table = getTable(tableName);
            Scan scan = new Scan();
            scan.setFilter(new FirstKeyOnlyFilter());
            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                count += result.size();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return count;
    }
//
//    public void deleteOneRow(String tableName, String rowKey) {
//        try {
//            Table table = getTable(tableName);
//            Delete delete = new Delete(Bytes.toBytes(rowKey));
//            table.delete(delete);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        System.out.println("-------------------删除一行数据成功----------------");
//    }
//
//    public void dropTable(String tableName) {
//        try {
//            if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
//                admin.disableTable(TableName.valueOf(tableName));
//                admin.deleteTable(TableName.valueOf(tableName));
//                System.out.println("-------------------删除表成功----------------");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//    }
//
//    public List<String> scanTable(String tableName) {
//        try {
//            Table table = getTable(tableName);
//            Scan scan = new Scan();
//            ResultScanner resultScanner = table.getScanner(scan);
//            List<String> list = new ArrayList<String>();
//            for (Result rs : resultScanner) {
//                for (Cell cell : rs.rawCells()) {
//                    byte[] k = CellUtil.cloneRow(cell);
//                    byte[] f = CellUtil.cloneFamily(cell);
//                    byte[] q = CellUtil.cloneQualifier(cell);
//                    byte[] v = CellUtil.cloneValue(cell);
//                    long t = cell.getTimestamp();
//                    String oneRow= Bytes.toString(k)+"\t"+"\t"+"\t"+"cloumn="+Bytes.toString(f)+":"+ Bytes.toString(q)+", "+"timestamp="+t+", "+"value="+Bytes.toString(v);
//                    System.out.println(oneRow);
//                    list.add(oneRow);
//                }
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    public List<String> descTable(String tableName, String columnName) {
//        List<String> list = new ArrayList<String>();
//        try {
//            HTableDescriptor tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName));
//            HColumnDescriptor[] columnDesc = tableDesc.getColumnFamilies();
//            for (int i = 0; i < columnDesc.length; i++) {
//                list.add(columnDesc[i].toString());
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        return list;
//    }

//    public static void main(String[] args) {
//        HbaseClient hbaseClient = new HbaseClient();
////         HbaseRow row = new HbaseRow("testFor", "", "cc", "ddd");
////         hbaseClient.put(row);
//    }
    @Test
    //一个本地文件64M
    //hbase 文件270M

    public void putData() throws IOException {
        logger.info("start time = {}",System.currentTimeMillis());
        List<Put> list=makeTestData();
        logger.info("end time = {}",System.currentTimeMillis());
        table = getTable("pid_test");
        table.put(list);
        table.close();
        logger.info("putData over");
        logger.info("size ={}",rowCount("pid_test"));


    }

    public List<Put> makeTestData() throws IOException {
        List<Put> list= Lists.newArrayList();
//        FileWriter fw = new FileWriter("/Users/luyao/fileData", true);
//        BufferedWriter bw = new BufferedWriter(fw);
        for(int i=1;i<=70000;i++){
            String rowkey="hadoop001"+i;//主键+pid;
            String command="curl -d apk=1498507219960SqjKWrRZ.apk&devices=MI 4LTE&intents=intent_finish_task=crowdsource://www.meituan.com/finishedtask,intent_balance_activity = crowdsource://www.meituan.com/BalanceActivity,intent_setting = crowdsource://www.meituan.com/setting,intent_main = crowdsource://www.meituan.com/home,intent_equipmentmall_activity = crowdsource://www.meituan.com/EquipmentMallActivity&events=1000&jenkinsUrl=http://ci.sankuai.com/job/banma/job/banma_autotest/job/banma_app_ci/job/Banma_Android_CrowdSource_Monkey/269/&after=notifyCrashAndAnrforStability&notify=wangtongchuang@meituan.com&numbers=561e73d2&misId=wangtongchuang&conanKey=d28caa9d-ad75-4d62-8420-df2f658bfb76 https://conan.sankuai.com/ci/stability/advanced-monkey";
            String ppid= "8183"+9*i;
            String egid= "500"+9*i;
            String gid= "500"+9*i;
            String euid= "500"+9*i;
            String user= "sankuai"+9*i;
            String uid= "500"+9*i;
            String starttime= "2017-12-04T16:15:56+0800";
            String mtime= "2015-06-16T10:15:41+0800";
            String owner_gid= "0"+9*i;
            String owner_uid= "0"+9*i;
            String mode= "100755"+9*i;
            String path= "/usr/bin/curl";
            String sid= "-1"+9*i;
            String processname= "curl";
            String host_name="hadoop001";
//            String file_data=i+" "+command+" "+ppid+" "+egid+" "+gid+" "+euid+" "+user+" "+uid+" "+starttime+ " "+mtime+" "+owner_gid+" "+owner_uid+" "+
//                    mode+" "+path+" "+sid+" "+processname+" "+host_name+"\n";
//            bw.write(file_data);
            Put put=new Put(Bytes.toBytes(rowkey));
            String familyName="data";
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("command"),Bytes.toBytes(command));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("ppid"),Bytes.toBytes(ppid));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("egid"),Bytes.toBytes(egid));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("user"),Bytes.toBytes(user));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("uid"),Bytes.toBytes(uid));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("starttime"),Bytes.toBytes(starttime));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("mtime"),Bytes.toBytes(mtime));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("owner_gid"),Bytes.toBytes(owner_gid));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("owner_uid"),Bytes.toBytes(owner_uid));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("mode"),Bytes.toBytes(mode));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("path"),Bytes.toBytes(path));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("gid"),Bytes.toBytes(gid));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("euid"),Bytes.toBytes(euid));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("sid"),Bytes.toBytes(sid));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("processname"),Bytes.toBytes(processname));
            put.addColumn(Bytes.toBytes(familyName),Bytes.toBytes("host_name"),Bytes.toBytes(host_name));
            list.add(put);
        }
//        bw.close();
//        fw.close();
       return list;
    }
}
