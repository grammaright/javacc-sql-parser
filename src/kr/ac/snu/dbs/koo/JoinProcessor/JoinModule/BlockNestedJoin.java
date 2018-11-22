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

    public static int PAGE_SIZE = 2;
    public static int BUFFER_SIZE = 4;

    // buffer init
    // 0 ~ BUFFER_SIZE - 3: Outer Buffer
    // BUFFER_SIZE - 2: Scan for S
    // BUFFER_SIZE - 1: Output Buffer
    private SqlRecord[][] buffer = new SqlRecord[BUFFER_SIZE][PAGE_SIZE];
    private int[] bufferPointer = new int[BUFFER_SIZE];

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
                throw new Exception("Error Condition: No attribute in table");
            }

            SqlColumn totalColumn = SqlColumn.concat(outerColumn, innerColumn);

            while (true) {
                // Buffer Block 채우기
                int outerBlockCount = 0;
                while ((outerLine = brOuter.readLine()) != null) {
                    outerItems = outerLine.split(" ");
                    buffer[outerBlockCount][bufferPointer[outerBlockCount]++] = SqlRecord.constructRecord(outerColumn, outerItems);
                    if (bufferPointer[outerBlockCount] == PAGE_SIZE) {
                        outerBlockCount++;
                        if (outerBlockCount < BUFFER_SIZE - 2) bufferPointer[outerBlockCount] = 0;
                        else if (outerBlockCount == (BUFFER_SIZE - 2)) break;
                    }
                }

                if (outerBlockCount == 0 && bufferPointer[0] == 0) {
                    // Outer Block 읽을 것이 없을 경우 break
                    break;
                }

                // Outer loop
                for (int i = 0; i < (outerBlockCount + 1) * PAGE_SIZE; i++) {
                    int bufferIndex = i / PAGE_SIZE;
                    int pointerIndex = i % PAGE_SIZE;

                    // pointer overflow 되기 때문에
                    if (bufferPointer[bufferIndex] <= pointerIndex) continue;

                    // Inner loop
                    while (true) {
                        // fill block for inner loop
                        while ((innerLine = brInner.readLine()) != null) {
                            innerItems = innerLine.split(" ");
                            buffer[BUFFER_SIZE - 2][bufferPointer[BUFFER_SIZE - 2]++] = SqlRecord.constructRecord(innerColumn, innerItems);
                            if (bufferPointer[BUFFER_SIZE - 2] == PAGE_SIZE) break;
                        }

                        if (bufferPointer[BUFFER_SIZE - 2] == 0) break;

                        for (int j = 0; j < PAGE_SIZE; j++) {
                            SqlRecord outerRecord = buffer[bufferIndex][pointerIndex];
                            SqlRecord innerRecord = buffer[BUFFER_SIZE - 2][j];

                            if ((joinCondition == null) ||
                                    (SqlValue.compare(outerRecord.values.get(outerTargetColumnIndex), innerRecord.values.get(innerTargetColumnIndex)) == 0)) {
                                int outerSize = outerRecord.values.size();
                                int innerSize = innerRecord.values.size();
                                String[] totalItems = new String[outerSize + innerSize];
                                for (int k = 0; k < totalItems.length; k++) {
                                    // TODO: Type..?
                                    if (k < outerSize) {
                                        totalItems[k] = outerRecord.values.get(k).toString();
                                    } else {
                                        totalItems[k] = innerRecord.values.get(k - outerSize).toString();
                                    }
                                }

                                buffer[BUFFER_SIZE - 1][bufferPointer[BUFFER_SIZE - 1]++] = SqlRecord.constructRecord(totalColumn, totalItems);

                                if (bufferPointer[BUFFER_SIZE - 1] == PAGE_SIZE) {
                                    // Output Buffer 꽉 찼을 경우
                                    for (int l = 0; l < PAGE_SIZE; l++) {
                                        for (int k = 0; k < buffer[BUFFER_SIZE - 1][l].values.size(); k++) {
                                            bw.write(buffer[BUFFER_SIZE - 1][l].values.get(k).toString() + " ");
                                        }
                                        bw.write("\n");
                                    }

                                    bufferPointer[BUFFER_SIZE - 1] = 0;
                                }
                            }
                        }

                        bufferPointer[BUFFER_SIZE - 2] = 0;
                    }

                    // inner buffered reader의 다음 iter 때 Column line 지우기 위해서
                    // TODO: 더럽..
                    brInner.close();
                    frInner.close();
                    frInner = new FileReader(innerTable.tablePath);
                    brInner = new BufferedReader(frInner);
                    brInner.readLine();
                }

                for (int j = 0; j < BUFFER_SIZE - 2; j++) {
                    bufferPointer[j] = 0;
                }
            }

            // 남은 Output buffer 있을 경우
            for (int l = 0; l < bufferPointer[BUFFER_SIZE - 1]; l++) {
                for (int k = 0; k < buffer[BUFFER_SIZE - 1][l].values.size(); k++) {
                    bw.write(buffer[BUFFER_SIZE - 1][l].values.get(k).toString() + " ");
                }
                bw.write("\n");
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
