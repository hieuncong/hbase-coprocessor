package hbase;

import hbase.coprocessor.regionobserver.RegionObserverExample;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TableOps {
    private static Configuration config;
    private static Connection connection;
    private static Admin admin;
    private static TableName tableName = TableName.valueOf("test");
    private static Table table;

    static {
        try {
            config = HBaseConfiguration.create();
            config.addResource(new Path("src/main/resources/hbase-site-vm.xml"));
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(tableName);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(table.getDescriptor());
        String path = "hdfs://master:9000/user/hadoop/hbase-coprocessor-1.0.jar";
        TableDescriptor tableDescriptor = TableDescriptorBuilder
                .newBuilder(tableName)
                .setValue("COPROCESSOR$1", path + "|"
                        + RegionObserverExample.class.getCanonicalName() + "|"
                        + Coprocessor.PRIORITY_USER)
                .setColumnFamily(ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes("f1"))
                        .build())
                .build();
        admin.modifyTable(tableDescriptor);
    }
}
