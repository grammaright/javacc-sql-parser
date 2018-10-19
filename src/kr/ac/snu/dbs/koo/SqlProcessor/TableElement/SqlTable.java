package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.HashSet;

public class SqlTable {
    public String tableName;
    public SqlColumn column;
    public ArrayList<SqlRecord> records;

    public static SqlTable constructTable(String tableName, HashSet<String> interestingOrder) {
        SqlTable table = new SqlTable();
        table.tableName = tableName;
        table.column = new SqlColumn();
        table.records = new ArrayList<>();

        String tablePath = "resources/" + tableName + ".txt";

        try {
            FileReader fr = new FileReader(tablePath);
            BufferedReader br = new BufferedReader(fr);

            String line = null;
            boolean isColProcess = true;
            while ((line = br.readLine()) != null) {
                String[] items = line.split(" ");

                if (isColProcess) {
                    // Column name 관련 처리
                    isColProcess = false;
                    table.column = SqlColumn.constructColumn(items, interestingOrder);
                } else {
                    SqlRecord record = SqlRecord.constructRecord(table.column, items);
                    table.records.add(record);
                }
            }

            br.close();
            fr.close();
        } catch (Exception e) {
            // TODO: Exception handling
            e.printStackTrace();
        }

        return table;
    }
}
