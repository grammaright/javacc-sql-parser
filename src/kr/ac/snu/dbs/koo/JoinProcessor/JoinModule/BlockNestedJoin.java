package kr.ac.snu.dbs.koo.JoinProcessor.JoinModule;

import com.sun.tools.javac.util.ArrayUtils;
import kr.ac.snu.dbs.koo.SqlGrammar.Types.Formula;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlColumn;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlRecord;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlTable;

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

            int outerTargetColumnIndex = -1;
            String outerLine = brOuter.readLine();
            String[] outerItems = outerLine.split(" ");
            SqlColumn outerColumn = SqlColumn.constructColumn(outerItems, null);
            outerTargetColumnIndex = matchTargetIndex(outerTable, joinCondition, outerColumn);

            // Outer 관련 변수 및 Column 처리
            FileReader frInner = new FileReader(innerTable.tablePath);
            BufferedReader brInner = new BufferedReader(frInner);

            int innerTargetColumnIndex = -1;
            String innerLine = brOuter.readLine();
            String[] innerItems = innerLine.split(" ");
            SqlColumn innerColumn = SqlColumn.constructColumn(innerItems, null);
            innerTargetColumnIndex = matchTargetIndex(outerTable, joinCondition, innerColumn);

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

                        String[] totalItems = new String[outerItems.length + innerItems.length];
                        for (int j = 0; j < totalItems.length; j++) {
                            if (j < outerItems.length) {
                                totalItems[j] = outerItems[j];
                            } else {
                                totalItems[j] = innerItems[j - outerItems.length];
                            }
                        }

                        if ((outerTargetColumnIndex == -1 || innerTargetColumnIndex == -1) ||
                                (buffer[i].values.get(outerTargetColumnIndex) ==
                                        buffer[BUFFER_SIZE - 2].values.get(innerTargetColumnIndex))) {
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
        for (int i = 0; i < column.values.size(); i++) {
            String columnItem = column.values.get(i);
            if (condition != null &&
                    (table.tableName.equals(condition.lvalue.table) &&
                            columnItem.equals(condition.lvalue.attribute)) &&
                    (table.tableName.equals(condition.rvalue.table) &&
                            columnItem.equals(condition.rvalue.attribute))) {
                // 해당 Table 에 해당 Column 이 존재할 때
                // outer target column index에 넣어놓음
                return i;
            }
        }
        return -1;
    }
}
