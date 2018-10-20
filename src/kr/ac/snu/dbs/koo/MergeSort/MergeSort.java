package kr.ac.snu.dbs.koo.MergeSort;

import kr.ac.snu.dbs.koo.SqlGrammar.Types.Attributer;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MergeSort {
    public static int PAGE_SIZE = 4096;
    public static int BUFFER_SIZE = 4;
    
    // buffer init
    // 0 ~ BUFFER_SIZE - 1: Input Buffer
    // BUFFER_SIZE - 1 ~ BUFFER_SIZE: Output Buffer
    private SqlRecord[][] buffer = new SqlRecord[BUFFER_SIZE][PAGE_SIZE];
    
    // buffer pointer init
    private int bufferPointer = 0;
    private int pagePointer[] = new int[BUFFER_SIZE];
    
    // column information
    private SqlColumn column = new SqlColumn();

    // sort by information
    private String targetFieldName = "age";
    private int targetColIndex = -1;
    private SqlValueType targetType = SqlValueType.NULL;

    private int currentPass = 0;
    private int currentRun = 0;
    private int beforeRun = 0;
    
    private String pathPrefix = "";
    private String lastPath = null;

    public static SqlTable orderTable(SqlTable original, Attributer orderList) throws Exception {
        if (orderList == null) return original;

        MergeSort ms = new MergeSort();
        String tablePath = "resources/" + original.tableName;
        ms.executeOnPath(tablePath + ".txt", tablePath, orderList.attribute);
        return SqlTable.constructTable(ms.lastPath, null);
    }

    public void executeOnPath(String path, String prefix, String field) throws Exception {
        pathPrefix = prefix;
        targetFieldName = field;
        
        firstPass(path);
        beforeRun = currentRun;
        currentRun = 0;

        flush();
        while (beforeRun > 1) {
            currentPass++;

            otherPass();
            beforeRun = currentRun;
            currentRun = 0;
            flush();
        }
    }
    
    private void flush() {
        // flush all data (buffer, pointer)
        buffer = new SqlRecord[BUFFER_SIZE][PAGE_SIZE];
        pagePointer = new int[BUFFER_SIZE];
    }
    
    private void firstPass(String path) throws Exception {
        try {
            FileReader fr = new FileReader(path);
            BufferedReader br = new BufferedReader(fr);

            String line = null;
            while ((line = br.readLine()) != null) {
                String[] items = line.split(" ");

                // Column name 관련 처리
                if (targetColIndex == -1) {
                    SqlColumn column = SqlColumn.constructColumn(items, null);
                    for (int i = 0; i < column.values.size(); i++) {
                        if (column.values.get(i).equals(targetFieldName)) {
                            targetColIndex = i;
                            targetType = column.types.get(i);
                            break;
                        }
                    }
                    
                    if (targetColIndex == -1) {
                        System.out.println("No field name founded.");
                        break;
                    }
                    
                    continue;
                }
                
                fillInputBufferForFirstPass(column, items);
            }
            
            // 끝나고 나머지들 
            writeOutputBufferForFirstPass();
            
            br.close();
            fr.close();
            
        } catch (IOException e) {
            e.printStackTrace();       
        }
    }
    
    private void fillInputBufferForFirstPass(SqlColumn column, String[] input) {
        buffer[bufferPointer][pagePointer[bufferPointer]++] = SqlRecord.constructRecord(column, input);
        if (pagePointer[bufferPointer] >= PAGE_SIZE) {
            bufferPointer++;
            if (bufferPointer >= BUFFER_SIZE) {
                writeOutputBufferForFirstPass();
                for (int i = 0; i < BUFFER_SIZE; i++) {
                    bufferPointer = 0;
                    pagePointer[i] = 0;
                }
            }
        }
    }
    
    private void writeOutputBufferForFirstPass() {
        try {
            for (int i = 0; i < BUFFER_SIZE; i++) {
                if (pagePointer[i] == 0) {
                    continue;
                }
                String currentPath = pathPrefix + "_" + Integer.toString(currentPass) + "_" + Integer.toString(currentRun) + ".txt";
                lastPath = currentPath;

                FileWriter fw = new FileWriter(currentPath);
                BufferedWriter bw = new BufferedWriter(fw);

                int count = 0;
                while (count < pagePointer[i]) {
                    // 비교 대상
                    int minIndex = -1;
                    SqlRecord minValue = null;
                    for (int j = 1; j < pagePointer[i]; j++) {
                        SqlRecord target = buffer[i][j];
                        if (target == null) {
                            continue;
                        }

                        if (minValue == null || SqlValue.compare(target.values.get(targetColIndex), minValue.values.get(targetColIndex)) == -1) {
                            minValue = target;
                            minIndex = i;
                        }
                    }

                    // write to file
                    for (int j = 0; j < buffer[i][minIndex].values.size(); j++) {
                        bw.write(buffer[i][minIndex].values.get(j) + " ");
                    }
                    bw.write("\n");

                    // clear buffer
                    buffer[i][minIndex] = null;
                    count += 1;
                }

                bw.close();
                fw.close();

                currentRun += 1;
                pagePointer[i] = 0;
            }
        } catch (IOException e) {
            e.printStackTrace();       
        }
    }
    
    private void otherPass() {
        // TODO: exception condition check
        try {
            int scannedRun = 0;
            while (true) {
                int result = fillInputBuffer(scannedRun);
                currentRun += 1;

                if (result < scannedRun + (BUFFER_SIZE - 1)) {
                    break;
                }
                scannedRun = result;
                flush();
            }  
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private int fillInputBuffer(int scannedRun) throws IOException {
        // scan input file
        FileReader[] fr = new FileReader[BUFFER_SIZE - 1];
        BufferedReader[] br = new BufferedReader[BUFFER_SIZE - 1];
        
        String writePath = pathPrefix + "_" + Integer.toString(currentPass) + "_" + Integer.toString(currentRun)  + ".txt";
        lastPath = writePath;
        
        FileWriter fw = new FileWriter(writePath, true);
        BufferedWriter bw = new BufferedWriter(fw);
        
        int localPagePointer[] = new int[BUFFER_SIZE];
        int gap = Math.min(scannedRun + BUFFER_SIZE - 1, beforeRun);
        for (int i = scannedRun; i < gap; i++) {
            String currentPath = pathPrefix+ "_" + Integer.toString(currentPass - 1) + "_" + Integer.toString(i)  + ".txt";
            
            fr[i % (BUFFER_SIZE - 1)] = new FileReader(currentPath);
            br[i % (BUFFER_SIZE - 1)] = new BufferedReader(fr[i % (BUFFER_SIZE - 1)]);
            
            // for init
            localPagePointer[i % (BUFFER_SIZE - 1)] = PAGE_SIZE;
        }
        
        int calMin = Math.min(BUFFER_SIZE - 1, beforeRun - scannedRun);
        while (true) {
            int eofCount = 0;
            for (int i = 0; i < calMin; i++) {
                if (localPagePointer[i] >= PAGE_SIZE) {
                    localPagePointer[i] = 0;
                    pagePointer[i] = 0;
                    for (int j = 0; j < PAGE_SIZE; j++) {
                        String line = null;
                        if ((line = br[i].readLine()) == null) {
                            if (j == 0) eofCount++;
                            break;
                        }

                        String[] items = line.split(" ");
                        if (items.length != column.values.size()) break;
                        buffer[i][pagePointer[i]++] = SqlRecord.constructRecord(column, items);
                    }
                }
            }
            
            if (eofCount >= calMin) {
                boolean allEnded = true;
                for (int i = 0; i < calMin; i++) {
                    int lowest = Math.min(PAGE_SIZE, pagePointer[i]);
                    if (localPagePointer[i] < lowest) {
                        allEnded = false;
                    }
                }
                if (allEnded) {
                    break;
                }
            }
            
             // pointer가 가리키는 값 중에서 가장 작은 값 찾기
            int minIndex = -1;
            SqlRecord minValue = null;
            for (int i = 0; i < BUFFER_SIZE - 1; i++) {
                int lowest = Math.min(PAGE_SIZE, pagePointer[i]);
                if (localPagePointer[i] >= lowest) {
                    continue;
                }
                
                SqlRecord target = buffer[i][localPagePointer[i]];
                if (target == null) {
                    continue;
                }

                if (minValue == null || SqlValue.compare(target.values.get(targetColIndex), minValue.values.get(targetColIndex)) == -1) {
                    minValue = target;
                    minIndex = i;
                }
            }
            
            // TODO: eofCount 와 실제 다 읽은 경우가 다를 수 있음.
            // 추후 수정
            // 홀수 개 들어가있거나 할때 그런 거같기도..
            if (minIndex == -1) {
                break;
            }
            
            // 해당 input buffer pointer 별경
            localPagePointer[minIndex]++;
            
            // Output buffer 쓰기 
            buffer[BUFFER_SIZE - 1][localPagePointer[BUFFER_SIZE - 1]++] = minValue;
            // pointer overflow 
            if (localPagePointer[BUFFER_SIZE - 1] >= PAGE_SIZE) {
                // TODO: write file
                for (int j = 0; j < PAGE_SIZE; j++) {
                    SqlRecord sv = buffer[BUFFER_SIZE - 1][j];

                    for (int k = 0; k < sv.values.size(); k++) {
                        bw.write(sv.values.get(k) + " ");
                    }

                    bw.write("\n");
                }
                bw.write("\n");
                localPagePointer[BUFFER_SIZE - 1] = 0;
            }
        }

        // last remained
        for (int j = 0; j < localPagePointer[BUFFER_SIZE - 1]; j++) {
            SqlRecord sv = buffer[BUFFER_SIZE - 1][j];

            for (int k = 0; k < sv.values.size(); k++) {
                bw.write(sv.values.get(k) + " ");
            }

            bw.write("\n");
        }
            
        bw.close();
        fw.close();

        for (int i = 0; i < calMin; i++) {
            fr[i].close();
            br[i].close();
        }

        return scannedRun + calMin;
    }

    public String getLastPath() {
        return lastPath;
    }

}
