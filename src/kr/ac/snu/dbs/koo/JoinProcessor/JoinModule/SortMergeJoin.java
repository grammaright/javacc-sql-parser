package kr.ac.snu.dbs.koo.JoinProcessor.JoinModule;

import kr.ac.snu.dbs.koo.MergeSort.MergeSort;
import kr.ac.snu.dbs.koo.SqlGrammar.Types.Attributer;
import kr.ac.snu.dbs.koo.SqlGrammar.Types.Formula;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlColumn;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlRecord;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlTable;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlValue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
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

            // TODO: 이것도 Disk Based로..?
            int tr = 0;     // ranges over R
            int ts = 0;     // ranges over S
            int gs = 0;     // start of current S-partition
            while (tr != table2.records.size() && gs != table1.records.size()) {
                SqlValue tri = table2.records.get(tr).values.get(target2ColumnIndex);
                SqlValue gsj = table1.records.get(gs).values.get(target1ColumnIndex);
                while (SqlValue.compare(tri, gsj) == 1) {
                    if (++tr >= table2.records.size()) break;
                    tri = table2.records.get(tr).values.get(target2ColumnIndex);      // continue scan of R
                }

                while (SqlValue.compare(tri, gsj) == -1) {
                    if (++gs >= table1.records.size()) break;
                    gsj = table1.records.get(gs).values.get(target1ColumnIndex);      // continue scan of S
                }

                ts = gs;                                            // Needed in case Tri != Gsj
                while (SqlValue.compare(tri, gsj) == 0) {           // process current R partition
                    ts = gs;                                        // reset S partition scan
                    SqlValue tsj = table1.records.get(ts).values.get(target1ColumnIndex);
                    while (SqlValue.compare(tsj, tri) == 0) {       // process current R tuple
                        SqlRecord trRecord = table2.records.get(tr);
                        SqlRecord tsRecord = table1.records.get(ts);

                        // write to disk
                        for (int i = 0; i < trRecord.values.size(); i++) {
                            bw.write(trRecord.values.get(i).toString() + " ");
                        }
                        for (int i = 0; i < tsRecord.values.size(); i++) {
                            bw.write(tsRecord.values.get(i).toString() + " ");
                        }
                        bw.write("\n");

                        if (++ts >= table1.records.size()) break;
                        tsj = table1.records.get(ts).values.get(target1ColumnIndex);
                    }
                    if (++tr >= table2.records.size()) break;
                    tri = table2.records.get(tr).values.get(target2ColumnIndex);
                }
                gs = ts;
                if (gs >= table1.records.size()) break;
                gsj = table1.records.get(gs).values.get(target1ColumnIndex);
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
