import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
/**
 * Created by hadoop on 17-5-9.
 */
public class HBaseOp {

    static Configuration conf = HBaseConfiguration.create();
    static HTable table;

    static {
        try {
            table = new HTable(conf, "Wuxia");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //通过HBaseAdmin HTableDescriptor来创建一个新表
    public static void create(String tableName, String columnFamily) throws Exception{
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)){
            System.out.println("Table exist");
            System.exit(0);
        }
        else {
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDescriptor);
            System.out.println("Table create success");
        }
    }

    //添加一条数据，通过HTable Put为已存在的表添加数据
    public static void put(String tableName,String row,String columnFamily,String column,String data) throws IOException{

        Put put = new Put(Bytes.toBytes(row));
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(data));
        table.put(put);
        System.out.println("put success");
        //table.close();
    }

    //获取tableName表里列为row的结果集
    public static void get(String tableName,String row) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(row));
        Result result = table.get(get);
        System.out.println("get "+ result);
    }

    //通过HTable Scan来获取tableName表的所有数据信息
    public static void scan (String tableName) throws IOException{
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result s:resultScanner){
            System.out.println("Scan "+ resultScanner);
        }
    }

    public static boolean contains(String tableName, String key) throws IOException {
        HTable table = new HTable(conf, tableName);
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result s:resultScanner){
            if(key.equals(Bytes.toString(s.getRow())))
                return true;
        }
        return false;
    }

    public static boolean delete(String tableName) throws Exception{
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(admin.tableExists(tableName)){
            try {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    /*public static void main(String[] args) {
        String tableName = "hbase_test";
        String columnFamily = "c1";

        try {
            HBaseOp.create(tableName, columnFamily);
            HBaseOp.put(tableName, "row1", columnFamily, "column1", "data1");
            HBaseOp.get(tableName, "row1");
            HBaseOp.scan(tableName);
            if(HBaseOp.delete(tableName)==true){
                System.out.println("delete table "+ tableName+"success");
            }

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
    }*/
}