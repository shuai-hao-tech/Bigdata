package org.geekbang.time.week1.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * @author haoshuaistart
 * @create 2022-05-11 21:42
 */
public class HbaseDemo {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","192.168.0.191,192.168.0.192,192.168.0.197");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        //创建namespaceDescriptor
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("haoshuai").build();
        admin.createNamespace(namespaceDescriptor);


        //创建表
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("haoshuai:student"))
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info")).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("score")).build())
                .build();
        admin.createTable(tableDescriptor);
        System.out.println("创建表成功");

        //插入数据
        Table table = connection.getTable(TableName.valueOf("haoshuai:student"));
        ArrayList<Put> puts = new ArrayList<Put>();
        Put put = new Put(Bytes.toBytes("Tom"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("stedent_id"),Bytes.toBytes("20210000000001"));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("class"),Bytes.toBytes("1"));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("understanding"),Bytes.toBytes("75"));
        put.addColumn(Bytes.toBytes("score"),Bytes.toBytes("programming"),Bytes.toBytes("82"));
        puts.add(put);

        Put put1 = new Put(Bytes.toBytes("Jerry"));
        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("stedent_id"),Bytes.toBytes("20210000000002"));
        put1.addColumn(Bytes.toBytes("info"),Bytes.toBytes("class"),Bytes.toBytes("1"));
        put1.addColumn(Bytes.toBytes("score"),Bytes.toBytes("understanding"),Bytes.toBytes("85"));
        put1.addColumn(Bytes.toBytes("score"),Bytes.toBytes("programming"),Bytes.toBytes("67"));
        puts.add(put1);

        Put put2 = new Put(Bytes.toBytes("Jack"));
        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("stedent_id"),Bytes.toBytes("20210000000003"));
        put2.addColumn(Bytes.toBytes("info"),Bytes.toBytes("class"),Bytes.toBytes("2"));
        put2.addColumn(Bytes.toBytes("score"),Bytes.toBytes("understanding"),Bytes.toBytes("80"));
        put2.addColumn(Bytes.toBytes("score"),Bytes.toBytes("programming"),Bytes.toBytes("80"));
        puts.add(put2);

        Put put3 = new Put(Bytes.toBytes("Rose"));
        put3.addColumn(Bytes.toBytes("info"),Bytes.toBytes("stedent_id"),Bytes.toBytes("20210000000004"));
        put3.addColumn(Bytes.toBytes("info"),Bytes.toBytes("class"),Bytes.toBytes("2"));
        put3.addColumn(Bytes.toBytes("score"),Bytes.toBytes("understanding"),Bytes.toBytes("60"));
        put3.addColumn(Bytes.toBytes("score"),Bytes.toBytes("programming"),Bytes.toBytes("61"));
        puts.add(put3);

        Put put4 = new Put(Bytes.toBytes("Haoshuai"));
        put4.addColumn(Bytes.toBytes("info"),Bytes.toBytes("stedent_id"),Bytes.toBytes("20210000000005"));
        put4.addColumn(Bytes.toBytes("info"),Bytes.toBytes("class"),Bytes.toBytes("2"));
        put4.addColumn(Bytes.toBytes("score"),Bytes.toBytes("understanding"),Bytes.toBytes("70"));
        put4.addColumn(Bytes.toBytes("score"),Bytes.toBytes("programming"),Bytes.toBytes("81"));
        puts.add(put4);

        table.put(puts);
        System.out.println("数据插入成功");

        ArrayList<Delete> deletes = new ArrayList<Delete>();
        Delete delete = new Delete(Bytes.toBytes("Tom"));
        Delete delete1 = new Delete(Bytes.toBytes("Jerry"));
        deletes.add(delete);
        deletes.add(delete1);
        table.delete(deletes);
        System.out.println("数据删除成功");

        Get get = new Get(Bytes.toBytes("Rose"));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println( Bytes.toString(result.getRow())
                    + " ," +Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell))
                    + " ," +Bytes.toString(CellUtil.cloneValue(cell))
                    + " ," + cell.getTimestamp());
        }
        System.out.println("查询数据");

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()) {
            Result next = iterator.next();
            for (Cell cell : next.rawCells()) {
                System.out.println( Bytes.toString(next.getRow())
                        + " ," +Bytes.toString(CellUtil.cloneFamily(cell)) + ":" + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + " ," +Bytes.toString(CellUtil.cloneValue(cell))
                        + " ," + cell.getTimestamp());
            }
        }
        System.out.println("批量查询数据");

        admin.close();
        connection.close();

    }
}
