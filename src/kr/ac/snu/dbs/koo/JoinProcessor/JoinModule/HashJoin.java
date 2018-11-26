package kr.ac.snu.dbs.koo.JoinProcessor.JoinModule;

import kr.ac.snu.dbs.koo.SqlGrammar.Types.Formula;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlColumn;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlRecord;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlTable;

import java.io.*;
import java.util.ArrayList;

public class HashJoin {

    public static int PAGE_SIZE = 2;
    public static int BUFFER_SIZE = 8;

    // buffer for partition phase & probing phase
    // in partition phase
    //      0 ~ BUFFER_SIZE - 3: Outer Buffer
    //      BUFFER_SIZE - 2: Scan for S
    //      BUFFER_SIZE - 1: Output Buffer
    // in probing phase
    //      0: Input Buffer
    //      1: Output Buffer
    private SqlRecord[][] buffer = null;
    private int[] bufferPointer = null;

    // hash table for probing phase
    private ArrayList<SqlRecord>[] hashTable = null;

    private int firstPartitionCount;

    private ArrayList<String> endPartitionPaths = new ArrayList<>();
    private String joinDir = null;


    public HashJoin() {
        joinDir = "resources/tmp/" + System.currentTimeMillis() + "/";

        // TODO: if directory not exists
        (new File("resources/tmp/")).mkdir();
        (new File(joinDir)).mkdir();
    }

    public void clear() {

    }

    public SqlTable join2Table(SqlTable table1, SqlTable table2, Formula joinCondition) {
        int table1Index, table2Index;
        if (table1.tableName.equals(joinCondition.lvalue.table)) {
            table1Index = table1.column.values.indexOf(joinCondition.lvalue.attribute);
            table2Index = table2.column.values.indexOf(joinCondition.rvalue.attribute);
        } else if (table1.tableName.equals(joinCondition.rvalue.table)){
            table1Index = table1.column.values.indexOf(joinCondition.rvalue.attribute);
            table2Index = table2.column.values.indexOf(joinCondition.lvalue.attribute);
        } else {
            return null;
        }
        if (table1Index == -1 || table2Index == -1) return null;

        try {
            partitioningPhase(table1.tablePath, true, table1Index);
            firstPartitionCount = endPartitionPaths.size();
            partitioningPhase(table2.tablePath, true, table2Index);

            for (String endPartitionPath : endPartitionPaths) {
                System.out.println(endPartitionPath);
            }

            probingPhase(table1Index, table2Index);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return SqlTable.constructTableFromMergeSorted(totalColumn, tablePath);;
    }

    private void partitioningPhase(String inputFilePath, boolean isTable, int interestingIndex) throws Exception {
        // init IO stuffs
        // buffered reader
        FileReader fr = new FileReader(inputFilePath);
        BufferedReader br = new BufferedReader(fr);
        if (isTable) {
            // Table 형태이기 때문에, column line 버림
            br.readLine();
        }

        // buffered writer
        FileWriter[] fw = new FileWriter[BUFFER_SIZE - 1];
        BufferedWriter[] bw = new BufferedWriter[BUFFER_SIZE - 1];
        String[] savedFileName = new String[BUFFER_SIZE - 1];
        int[] recordCount = new int[BUFFER_SIZE - 1];
        for (int i = 0; i < fw.length; i++) {
            String[] token = inputFilePath.split("/");
            savedFileName[i] = token[token.length - 1].split("\\.")[0];

            fw[i] = new FileWriter(joinDir + "/" + savedFileName[i] + "_" + Integer.toString(i) + ".txt");
            bw[i] = new BufferedWriter(fw[i]);
        }

        // init for buffer
        buffer = new SqlRecord[BUFFER_SIZE][PAGE_SIZE];
        bufferPointer = new int[BUFFER_SIZE];

        while (true) {
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] lineItmes = line.split(" ");
                buffer[0][bufferPointer[0]++] = SqlRecord.constructRecord(null, lineItmes);
                if (bufferPointer[0] == PAGE_SIZE) break;
            }

            if (bufferPointer[0] == 0) {
                // read 쪽에서 읽을 것이 없을 경우 break
                break;
            }

            for (int i = 0; i < PAGE_SIZE; i++) {
                int hashResult = hashFunction(buffer[0][i].values.get(interestingIndex).toString());
                if (hashResult + 1 >= BUFFER_SIZE) {
                    throw new Exception("Hash function must return 0...BUFFER_SIZE - 1 on partitioningPhase");
                }
                buffer[hashResult + 1][bufferPointer[hashResult + 1]++] = buffer[0][i];
                recordCount[hashResult]++;

                if (bufferPointer[hashResult + 1] == PAGE_SIZE) {
                    // Output Buffer 꽉 찼을 경우
                    for (int j = 0; j < PAGE_SIZE; j++) {
                        for (int k = 0; k < buffer[hashResult + 1][j].values.size(); k++) {
                            bw[hashResult].write(buffer[hashResult + 1][j].values.get(k).toString() + " ");
                        }
                        bw[hashResult].write("\n");
                    }

                    bufferPointer[hashResult + 1] = 0;
                }
            }

            bufferPointer[0] = 0;
        }

        for (int i = 1; i < BUFFER_SIZE; i++) {
            for (int j = 0; j < bufferPointer[i]; j++) {
                for (int k = 0; k < buffer[i][j].values.size(); k++) {
                    bw[i - 1].write(buffer[i][j].values.get(k).toString() + " ");
                }
                bw[i - 1].write("\n");
            }
        }

        // clear
        fr.close();
        br.close();

        for (int i = 0; i < fw.length; i++) {
            bw[i].close();
            fw[i].close();

            String path = joinDir + "/" + savedFileName[i] + "_" + Integer.toString(i) + ".txt";
            if (recordCount[i] > BUFFER_SIZE - 2) {
                // Partition 다 안끝났을 때
                partitioningPhase(path, false, interestingIndex);
            } else {
                // 다 끝나면, Probing Phase 를 위해 path 저장
                endPartitionPaths.add(path);
            }
        }
    }

    private void probingPhase(int table1ColumnIndex, int table2ColumnIndex) throws Exception {
        // init for buffer
        buffer = new SqlRecord[BUFFER_SIZE][PAGE_SIZE];
        bufferPointer = new int[BUFFER_SIZE];

        // init for hashtable
        hashTable = new ArrayList[BUFFER_SIZE - 2];
        for (int i = 0; i < BUFFER_SIZE - 2; i++) {
            hashTable[i] = new ArrayList<>();
        }

        int table1Index = 0;
        int table2Index = 0;
        //        for (int i = 0; i < firstPartitionCount; i++) {
        while (table1Index < firstPartitionCount) {
            // hash table 생성
            // 못쓰는 buffer 공간을 이용하여 arrayList (linked list) 로 이루어진 hash table 생성
            String path = endPartitionPaths.get(table1Index);
            FileReader fr = new FileReader(path);
            BufferedReader br = new BufferedReader(fr);
            bufferPointer[2] = 0;
            String line = null;
            while ((line = br.readLine()) != null) {
                String[] lineItmes = line.split(" ");
                buffer[2][bufferPointer[2]] = SqlRecord.constructRecord(null, lineItmes);
                int hashResult = hashFunction2(buffer[2][bufferPointer[2]].values.get(table1ColumnIndex).toString());
                hashTable[hashResult].add(buffer[2][bufferPointer[2]]);
            }



        }
    }

    /*
        partition file 의 name 을 비교하는 method

        return
        0: equal - 둘 다 같음
        1: diff - inner (table2) 를 다음으로
        2: short (table2가 짧음) - 동작 후 다음 file을 통해 hash table (table 1) 새로 construct
        3: short (table1이 짧음) - 동작 후 다음 file을 통해 안쪽(table 2) 다음으로
     */
    private int comparePartition(String input1, String input2) {
        // TODO: 하드코딩
        String[] token1 = input1.split("_");
        String[] token2 = input2.split("_");

        if (token1.length < 3 || token2.length < 3) {
            return -1;
        }

        int index = 2;
        while (true) {
            if (index >= token1.length && index >= token2.length && token1.equals(token2)) return 0;
            if (index >= token1.length) return 2;
            else if (index >= token2.length) return 3;
            if ()
        }

        return -1;
    }

    private int hashFunction(String value) {
        // TODO:
        return Integer.valueOf(value) % (BUFFER_SIZE - 1);
    }

    private int hashFunction2(String value) {
        // TODO:
        return Integer.valueOf(value) % (BUFFER_SIZE - 2);
    }

}
