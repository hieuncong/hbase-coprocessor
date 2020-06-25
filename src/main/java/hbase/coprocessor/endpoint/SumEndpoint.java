package hbase.coprocessor.endpoint;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.shaded.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SumEndpoint extends Sum.SumService implements Coprocessor, CoprocessorService{
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

    private RegionCoprocessorEnvironment env;

    @Override
    public Service getService() {
        return (Service) this;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {

    }

    @Override
    public Iterable<Service> getServices() {
        return null;
    }

    @Override
    public void getSum(RpcController controller, Sum.SumRequest request, RpcCallback<Sum.SumResponse> done) {
        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes(request.getFamily()));
        scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(request.getColumn()));

        Sum.SumResponse response = null;
        InternalScanner scanner = null;

        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList<>();
            boolean hasMore = false;
            long sum = 0L;

            do {
                hasMore = scanner.next(results);
                for (Cell cell : results) {
                    sum = sum + Bytes.toLong(CellUtil.cloneValue(cell));
                }
                results.clear();
            } while (hasMore);

            response = Sum.SumResponse.newBuilder().setSum(sum).build();
        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {}
            }
        }

        done.run(response);
    }

}
