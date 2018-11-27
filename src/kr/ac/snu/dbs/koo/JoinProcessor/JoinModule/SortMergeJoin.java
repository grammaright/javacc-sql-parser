package kr.ac.snu.dbs.koo.JoinProcessor.JoinModule;

import kr.ac.snu.dbs.koo.MergeSort.MergeSort;
import kr.ac.snu.dbs.koo.SqlGrammar.Types.Attributer;
import kr.ac.snu.dbs.koo.SqlGrammar.Types.Formula;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlColumn;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlRecord;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlTable;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlValue;

import java.io.*;
import java.util.ArrayList;

public class SortMergeJoin {

    private String joinDir = null;

    public SortMergeJoin() {
        joinDir = "resources/tmp/" + System.currentTimeMillis() + "/";

        // TODO: if directory not exists
        (new File("resources/tmp/")).mkdir();
        (new File(joinDir)).mkdir();
    }

    public SqlTable join2Table(SqlTable table1, SqlTable table2, Formula joinCondition) {
        Attributer table1OrderList, table2OrderList;
        if (table1.tableName.equals(joinCondition.rvalue.table)) {
            table1OrderList = joinCondition.rvalue;
            table2OrderList = joinCondition.lvalue;
        } else {
            table1OrderList = joinCondition.lvalue;
            table2OrderList = joinCondition.rvalue;
        }

        // orderList 는 key 에 따라 달라짐
        try {
            // Write
            String tablePath = joinDir + "/" + table1.tableName + "-" + table2.tableName + "-joined.txt";
            FileWriter fw = new FileWriter(tablePath);
            BufferedWriter bw = new BufferedWriter(fw);

            // sort it
            table1 = MergeSort.orderTable(table1, table1OrderList);
            table2 = MergeSort.orderTable(table2, table2OrderList);

            int target1ColumnIndex = matchTargetIndex(table1, joinCondition, table1.column);
            int target2ColumnIndex = matchTargetIndex(table2, joinCondition, table2.column);

            if (target1ColumnIndex == -1 || target2ColumnIndex == -1) {
                // TODO: Error Condition
                throw new Exception("Error Condition: No attribute in table");
            }

            FileReader trFr = new FileReader(table2.tablePath);
            FileReader gsFr = new FileReader(table1.tablePath);
            FileReader tsFr = null;

            BufferedReader trBr = new BufferedReader(trFr);
            BufferedReader gsBr = new BufferedReader(gsFr);
            BufferedReader tsBr = null;

            int tr = 0;     // ranges over R
            int ts = 0;     // ranges over S
            int gs = 0;     // start of current S-partition

            String trLine = null;
            String tsLine = null;
            String gsLine = null;
            while (((trLine = trBr.readLine()) != null) && ((gsLine = gsBr.readLine()) != null)) {
                SqlRecord tri = SqlRecord.constructRecord(null, trLine.split(" "));
                SqlRecord gsj = SqlRecord.constructRecord(null, gsLine.split(" "));
                while (SqlValue.compare(tri.values.get(target1ColumnIndex), gsj.values.get(target2ColumnIndex)) == 1) {
                    if ((trLine = trBr.readLine()) == null) break;
                    tri = SqlRecord.constructRecord(null, trLine.split(" "));
                    tr++;   // continue scan of R
                }

                while (SqlValue.compare(tri.values.get(target1ColumnIndex), gsj.values.get(target2ColumnIndex)) == -1) {
                    if ((gsLine = gsBr.readLine()) == null) break;
                    gsj = SqlRecord.constructRecord(null, gsLine.split(" "));
                    gs++;   // continue scan of S
                }

                ts = gs;                                            // Needed in case Tri != Gsj
                while (SqlValue.compare(tri.values.get(target1ColumnIndex), gsj.values.get(target2ColumnIndex)) == 0) {           // process current R partition
                    ts = gs;                                        // reset S partition scan
                    tsBr.close();
                    tsFr.close();
                    tsFr = new FileReader(table1.tablePath);
                    tsBr = new BufferedReader(tsFr);
                    for (int i = 0; i < ts; i++) tsLine = tsBr.readLine();
                    SqlRecord tsj = SqlRecord.constructRecord(null, tsLine.split(" "));

                    while (SqlValue.compare(tsj.values.get(target2ColumnIndex), tri.values.get(target1ColumnIndex)) == 0) {       // process current R tuple
                        // write to disk
                        for (int i = 0; i < tri.values.size(); i++) {
                            bw.write(tri.values.get(i).toString() + " ");
                        }
                        for (int i = 0; i < tsj.values.size(); i++) {
                            bw.write(tsj.values.get(i).toString() + " ");
                        }
                        bw.write("\n");

                        if ((tsLine = tsBr.readLine()) == null) break;
                        ts++;
                        tsj = SqlRecord.constructRecord(null, tsLine.split(" "));
                    }

                    if ((trLine = trBr.readLine()) == null) break;
                    tr++;
                    tri = SqlRecord.constructRecord(null, trLine.split(" "));
                }
                gs = ts;

                gsBr.close();
                gsFr.close();
                gsFr = new FileReader(table1.tablePath);
                gsBr = new BufferedReader(gsFr);
                for (int i = 0; i < gs; i++) gsBr.readLine();
            }

            bw.close();
            fw.close();

            SqlColumn totalColumn = SqlColumn.concat(table2.column, table1.column);
            return SqlTable.constructTableFromMergeSorted(totalColumn, tablePath);

        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private int matchTargetIndex(SqlTable table, Formula condition, SqlColumn column) {
        if (condition == null) return -1;
        for (int i = 0; i < column.values.size(); i++) {
            String columnItem = column.values.get(i);
            String refineTableName = table.tableName.split("_")[0];     // TODO: Merge Sort 결과로 table 명 변경됨.

            if (((refineTableName.equals(condition.lvalue.table) &&
                    columnItem.equals(condition.lvalue.attribute)) ||
                    (refineTableName.equals(condition.rvalue.table) &&
                            columnItem.equals(condition.rvalue.attribute)))) {
                // 해당 Table 에 해당 Column 이 존재할 때
                // outer target column index에 넣어놓음
                return i;
            }
        }
        return -1;
    }
}
