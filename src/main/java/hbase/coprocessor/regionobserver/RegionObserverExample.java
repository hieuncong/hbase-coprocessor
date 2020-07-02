package hbase.coprocessor.regionobserver;

import org.apache.hadoop.conf.Configuration;
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

    private final static String SOURCE_TABLE = "test";
    private final static String INDEX_TABLE = "test2";

    private Connection connection;

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        Configuration config = env.getConfiguration();
        connection = ConnectionFactory.createConnection(config);
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        connection.close();
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability) throws IOException {
        byte[] tableName = c.getEnvironment().getRegionInfo().getTable().getName();

        // Not necessary though if you register the coprocessor for the specific table
        if (!Bytes.equals(tableName, Bytes.toBytes(SOURCE_TABLE))) {
            return;
        }

        byte[] row1 = put.getRow();
        byte[] value1 = CellUtil.cloneValue(put.get(Bytes.toBytes("f1"), Bytes.toBytes("q1")).get(0));

        Put put2 = new Put(value1);
        put2.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("row"), row1);

        TableName indexTblName = TableName.valueOf(INDEX_TABLE);
        Table indexTbl = connection.getTable(indexTblName);
        indexTbl.put(put2);
        indexTbl.close();
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
        } else if (Bytes.equals(get.getRow(), EMP)) {
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

}
