package hyy.nju.cn;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class Student {
    public static Configuration cfg;
    public static void addData(String tableName, String rowKey, String family, String qualifier, String value){
        try{
            Connection conn = ConnectionFactory.createConnection(cfg);
            Table table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
            table.put(put);
            //System.out.println("insert record success!");
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }

	public static void scanTable(String tableName) throws IOException{
        Connection conn = ConnectionFactory.createConnection(cfg);
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan s = new Scan();
        s.readAllVersions();
        ResultScanner ss = table.getScanner(s);
        for(Result r:ss){
            System.out.println(new String(r.getRow()));
            for(Cell cell:r.rawCells()){
                System.out.println("rowkey:" + Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family:" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        System.out.println("scan all table success!");
    }

    public static void addStudent(String tableName, String rowKey, String[] data) throws IOException{
        addData(tableName, rowKey, "Description","Name",data[0]);
        addData(tableName, rowKey, "Description","Height",data[1]);
        addData(tableName, rowKey, "Courses","Chinese",data[2]);
        addData(tableName, rowKey, "Courses","Math",data[3]);
        addData(tableName, rowKey, "Courses","Physics",data[4]);
        addData(tableName, rowKey, "Home","Province",data[5]);
        System.out.println("insert student record success!");
    }


	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception{
        Configuration HBASE_CONFIG = new Configuration();
        cfg = HBaseConfiguration.create(HBASE_CONFIG);
        Connection conn = ConnectionFactory.createConnection(cfg);
        Admin admin = conn.getAdmin();
        TableName name = TableName.valueOf("Students");
		HTableDescriptor t = new HTableDescriptor(name);
        //TableDescriptorBuilder t1 = TableDescriptorBuilder.newBuilder(name);
        t.modifyFamily(new HColumnDescriptor("ID"));
        t.modifyFamily(new HColumnDescriptor("Description"));
        t.modifyFamily(new HColumnDescriptor("Courses"));
        t.modifyFamily(new HColumnDescriptor("Home"));
        if (admin.tableExists(t.getTableName())) {
            admin.disableTable(t.getTableName());
            admin.deleteTable(t.getTableName());
        }
        admin.createTable(t);
        System.out.println("create table success!");
        String[] stuData1 = {"Li Lei", "176", "80", "90", "95", "Zhejiang"};
        String[] stuData2 = {"Han Meimei", "183", "88", "77", "66", "Beijing"};
        String[] stuData3 = {"Xiao Ming", "162", "90", "90", "90", "Shanghai"};
        addStudent("Students", "001", stuData1);
        addStudent("Students", "002", stuData2);
        addStudent("Students", "003", stuData3);
        scanTable("Students");
    }
}