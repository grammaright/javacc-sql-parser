package kr.ac.snu.dbs.koo.JoinProcessor.JoinModule;

import com.sun.tools.javac.util.ArrayUtils;
import kr.ac.snu.dbs.koo.SqlGrammar.Types.Formula;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlColumn;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlRecord;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlTable;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlValue;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin {

    public static int BUFFER_SIZE = 4;

    // buffer init
    // 0 ~ BUFFER_SIZE - 3: Outer Buffer
    // BUFFER_SIZE - 2: Scan for S
    // BUFFER_SIZE - 1: Output Buffer
    private SqlRecord[] buffer = new SqlRecord[BUFFER_SIZE];

    private String joinDir = null;

    public BlockNestedJoin() {
        joinDir = "resources/tmp/" + System.currentTimeMillis() + "/";

        // TODO: if directory not exists
        (new File("resources/tmp/")).mkdir();
        (new File(joinDir)).mkdir();
    }

    public SqlTable join2Table(SqlTable outerTable, SqlTable innerTable, Formula joinCondition) {
        try {
            // Write
            String tablePath = joinDir + "/" + outerTable.tableName + "-" + innerTable.tableName + "-joined.txt";
            FileWriter fw = new FileWriter(tablePath);
            BufferedWriter bw = new BufferedWriter(fw);

            // Outer 관련 변수 및 Column 처리
            FileReader frOuter = new FileReader(outerTable.tablePath);
            BufferedReader brOuter = new BufferedReader(frOuter);

            String outerLine = brOuter.readLine();
            String[] outerItems = outerLine.split(" ");
            SqlColumn outerColumn = SqlColumn.constructColumn(outerItems, null);
            int outerTargetColumnIndex = matchTargetIndex(outerTable, joinCondition, outerColumn);

            // Inner 관련 변수 및 Column 처리
            FileReader frInner = new FileReader(innerTable.tablePath);
            BufferedReader brInner = new BufferedReader(frInner);

            String innerLine = brInner.readLine();
            String[] innerItems = innerLine.split(" ");
            SqlColumn innerColumn = SqlColumn.constructColumn(innerItems, null);
            int innerTargetColumnIndex = matchTargetIndex(outerTable, joinCondition, innerColumn);

            if (outerTargetColumnIndex == -1 || innerTargetColumnIndex == -1) {
                // TODO: Error Condition
                throw new Exception("Error Condition: No attribute in table");
//                return null;
            }

            SqlColumn totalColumn = SqlColumn.concat(outerColumn, innerColumn);

            while (true) {
                // Buffer Block 채우기
                int outerBlockCount = 0;
                while ((outerLine = brOuter.readLine()) != null) {
                    outerItems = outerLine.split(" ");
                    buffer[outerBlockCount++] = SqlRecord.constructRecord(outerColumn, outerItems);
                    if (outerBlockCount == (BUFFER_SIZE - 2)) break;
                }

                if (outerBlockCount == 0) {
                    // Outer Block 읽을 것이 없을 경우 break
                    break;
                }

                // Outer loop
                for (int i = 0; i < outerBlockCount; i++) {
                    // Inner loop
                    while ((innerLine = brInner.readLine()) != null) {
                        innerItems = innerLine.split(" ");
                        buffer[BUFFER_SIZE - 2] = SqlRecord.constructRecord(innerColumn, innerItems);

                        int outerSize = buffer[i].values.size();
                        String[] totalItems = new String[outerSize + innerItems.length];
                        for (int j = 0; j < totalItems.length; j++) {
                            if (j < outerSize) {
                                totalItems[j] = buffer[i].values.get(j).toString();
                            } else {
                                totalItems[j] = innerItems[j - outerSize];
                            }
                        }

                        // TODO: cross product, invalid condition 둘 다 고려해야 함.
                        if ((joinCondition == null) ||
                                (SqlValue.compare(buffer[i].values.get(outerTargetColumnIndex), buffer[BUFFER_SIZE - 2].values.get(innerTargetColumnIndex)) == 0)) {
                            // cross-product / equlity check 만 있다고 가정
                            buffer[BUFFER_SIZE - 1] = SqlRecord.constructRecord(totalColumn, totalItems);

                            // write to disk
                            for (int k = 0; k < buffer[BUFFER_SIZE - 1].values.size(); k++) {
                                bw.write(buffer[BUFFER_SIZE - 1].values.get(k).toString() + " ");
                            }
                            bw.write("\n");
                        }
                    }

                    // inner buffered reader의 다음 iter 때 Column line 지우기 위해서
                    // TODO: 더럽..
                    brInner.close();
                    frInner.close();
                    frInner = new FileReader(innerTable.tablePath);
                    brInner = new BufferedReader(frInner);
                    brInner.readLine();
                }
            }

            // 끝나고 나머지들
            brOuter.close();
            frOuter.close();
            brInner.close();
            frInner.close();

            bw.close();
            fw.close();

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
            if (((table.tableName.equals(condition.lvalue.table) &&
                        columnItem.equals(condition.lvalue.attribute)) ||
                (table.tableName.equals(condition.rvalue.table) &&
                        columnItem.equals(condition.rvalue.attribute)))) {
                // 해당 Table 에 해당 Column 이 존재할 때
                // outer target column index에 넣어놓음
                return i;
            }
        }
        return -1;
    }
}
