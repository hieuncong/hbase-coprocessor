package hbase;

import com.google.protobuf.ServiceException;
import hbase.coprocessor.endpoint.Sum;
import hbase.coprocessor.regionobserver.RegionObserverExample;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;

public class TableOps {
    private static Configuration config;
    private static Connection connection;
    private static Admin admin;
    private static TableName tableName1 = TableName.valueOf("test");
    private static Table table1;
    private static TableName tableName2 = TableName.valueOf("test2");
    private static Table table2;

    static {
        try {
            config = HBaseConfiguration.create();
            config.addResource(new Path("src/main/resources/hbase-site-vm.xml"));
            connection = ConnectionFactory.createConnection(config);
            table1 = connection.getTable(tableName1);
            table2 = connection.getTable(tableName2);
            admin = connection.getAdmin();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(config);
//        updateTableAtt();
//        Put p = new Put(Bytes.toBytes("r1"));
//        p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("a"), Bytes.toBytes("v1"));
//        p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("b"), Bytes.toBytes("v2"));
//
//        table1.put(p);
    }

    public static void sumEndpointExample() throws IOException {
        System.out.println(table1.getDescriptor());
        final Sum.SumRequest request = Sum.SumRequest.newBuilder().setFamily("f1").setColumn("a").build();
        try {
            Map<byte[], Long> results = table1.coprocessorService(
                    Sum.SumService.class,
                    null,  /* start key */
                    null,  /* end   key */
                    new Batch.Call<Sum.SumService, Long>() {
                        @Override
                        public Long call(Sum.SumService aggregate) throws IOException {
                            CoprocessorRpcUtils.BlockingRpcCallback<Sum.SumResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
                            aggregate.getSum(null, request, rpcCallback);
                            Sum.SumResponse response = rpcCallback.get();
                            System.out.println(response);
                            return response.hasSum() ? response.getSum() : 0L;
                        }
                    }
            );

            for (Long sum : results.values()) {
                System.out.println("Sum = " + sum);
            }
        } catch (ServiceException e) {
            e.printStackTrace();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public static void updateTableAtt() throws IOException {
        String regionObserver = "hdfs://master:9000/user/hadoop/hbase-coprocessor-regionobserver-1.0.jar";
        TableDescriptor tableDescriptor = TableDescriptorBuilder
                .newBuilder(tableName2)
                .setValue("COPROCESSOR$1", regionObserver + "|"
                        + RegionObserverExample.class.getCanonicalName() + "|"
                        + Coprocessor.PRIORITY_USER)
                .setColumnFamily(ColumnFamilyDescriptorBuilder
                        .newBuilder(Bytes.toBytes("f1"))
                        .build())
                .build();
        admin.modifyTable(tableDescriptor);
    }
}
