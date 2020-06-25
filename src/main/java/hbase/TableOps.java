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
        sumEndpointExample();
    }

    public static void sumEndpointExample() {
        final Sum.SumRequest request = Sum.SumRequest.newBuilder().setFamily("f1").setColumn("a").build();
        try {
            Map<byte[], Long> results = table.coprocessorService(
                    Sum.SumService.class,
                    null,  /* start key */
                    null,  /* end   key */
                    new Batch.Call<Sum.SumService, Long>() {
                        @Override
                        public Long call(Sum.SumService aggregate) throws IOException {
                            CoprocessorRpcUtils.BlockingRpcCallback<Sum.SumResponse> rpcCallback = new CoprocessorRpcUtils.BlockingRpcCallback<>();
                            aggregate.getSum(null, request, rpcCallback);
                            Sum.SumResponse response = rpcCallback.get();

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
