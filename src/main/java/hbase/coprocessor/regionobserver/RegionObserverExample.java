package hbase.coprocessor.regionobserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class RegionObserverExample implements RegionCoprocessor, RegionObserver {
    private static final byte[] ADMIN = Bytes.toBytes("admin");
    private static final byte[] EMP = Bytes.toBytes("emp");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("family");
    private static final byte[] COLUMN = Bytes.toBytes("qualifier");
    private static final byte[] VALUE = Bytes.toBytes("You can't see this row");

    private static Configuration config;
    private static Connection connection;
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void preGetOp(final ObserverContext<RegionCoprocessorEnvironment> e, final Get get, final List<Cell> results)
            throws IOException {

        if (Bytes.equals(get.getRow(), ADMIN)) {
            Cell c = CellBuilderFactory
                    .create(CellBuilderType.SHALLOW_COPY)
                    .setRow(get.getRow())
                    .setFamily(COLUMN_FAMILY)
                    .setQualifier(COLUMN)
                    .setValue(VALUE)
                    .setTimestamp(System.currentTimeMillis())
                    .setType(Cell.Type.Put)
                    .build();
            results.add(c);
            e.bypass();
        }

        else if (Bytes.equals(get.getRow(), EMP)) {
            Cell c = CellBuilderFactory
                    .create(CellBuilderType.SHALLOW_COPY)
                    .setRow(get.getRow())
                    .setFamily(COLUMN_FAMILY)
                    .setQualifier(COLUMN)
                    .setValue(VALUE)
                    .setTimestamp(System.currentTimeMillis())
                    .setType(Cell.Type.Put)
                    .build();
            results.add(c);
            e.bypass();
        }
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        table2.put(put);
        c.bypass();
    }
}
