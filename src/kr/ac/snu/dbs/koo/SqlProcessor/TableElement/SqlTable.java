package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

import kr.ac.snu.dbs.koo.SqlGrammar.Types.Attributer;
import kr.ac.snu.dbs.koo.SqlProcessor.SqlUtils;

import java.io.*;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

public class SqlTable {
    public String tableName;
    public SqlColumn column;
    public ArrayList<SqlRecord> records;

    public String tablePath;

    private static SqlTable constructTable(String tablePath, HashSet<Attributer> interestingOrder, boolean isColProcess) {
        SqlTable table = new SqlTable();
        table.column = (isColProcess) ? new SqlColumn() : null;
        table.records = new ArrayList<>();

        table.tablePath = tablePath;
        // TODO: table name / table path 관련 정리 필요
        // or regex
        String[] tableNameComp = tablePath.split("/");
        table.tableName = tableNameComp[tableNameComp.length - 1].split("\\.")[0];

        HashSet<String> tableInterestingOrder = new HashSet<>();
        if (interestingOrder != null) {
            Iterator interestingOrderIter = interestingOrder.iterator();
            while (interestingOrderIter.hasNext()) {
                Attributer item = (Attributer) interestingOrderIter.next();
                boolean asterisk = (item.table == null) && (item.attribute.equals("*"));
                if (table.tableName.equals(item.table) || asterisk) {
                    tableInterestingOrder.add(item.attribute);
                }
            }
        }

        try {
            FileReader fr = new FileReader(tablePath);
            BufferedReader br = new BufferedReader(fr);

            String line = null;
            while ((line = br.readLine()) != null) {
                if (line.equals("")) continue;
                String[] items = line.split(" ");

                if (isColProcess) {
                    // Column name 관련 처리
                    isColProcess = false;
                    table.column = SqlColumn.constructColumn(items, tableInterestingOrder);
                } else {
                    if (table.column != null && items.length < table.column.values.size()) continue;

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

    public static SqlTable constructTable(String tablePath, HashSet<Attributer> interestingOrder) {
        return constructTable(tablePath, interestingOrder, true);
    }

    public static SqlTable constructTableFromMergeSorted(SqlColumn column, String tablePath) {
        SqlTable table = constructTable(tablePath, null, false);
        table.column = column;
        return table;
    }

    public void writeTableToTmp() {
        try {
            String tmpDir = "resources/tmp/" + System.currentTimeMillis() + "/";

            // TODO: if directory not exists
            (new File("resources/tmp/")).mkdir();
            (new File(tmpDir)).mkdir();

            String resultPath = tmpDir + tableName + "_filtered.txt";
            tablePath = resultPath;

            FileWriter fw = new FileWriter(resultPath);
            BufferedWriter bw = new BufferedWriter(fw);

            // write column
            for (int i = 0; i < column.values.size(); i++) {
                bw.write(column.values.get(i) + "(" + SqlUtils.sqlValueTypeToString(column.types.get(i))+ ") ");
            }
            bw.write("\n");

            // write to file
            for (int i = 0; i < records.size(); i++) {
                for (int j = 0; j < records.get(i).values.size(); j++) {
                    bw.write(records.get(i).values.get(j).toString() + " ");
                }
                bw.write("\n");
            }

            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
