package kr.ac.snu.dbs.koo.JoinProcessor.JoinModule;

import kr.ac.snu.dbs.koo.SqlGrammar.Types.Formula;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlColumn;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlRecord;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlTable;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlValue;

import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedList;

public class HashJoin {

    public static int PAGE_SIZE = 2;
    public static int BUFFER_SIZE = 4;

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
    private LinkedList<SqlRecord>[] hashTable = null;

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

        SqlColumn totalColumn = SqlColumn.concat(table1.column, table2.column);

        try {
            partitioningPhase(table1.tablePath, 0, table1Index);
            firstPartitionCount = endPartitionPaths.size();
            partitioningPhase(table2.tablePath, 0, table2Index);

//            for (String endPartitionPath : endPartitionPaths) {
//                System.out.println(endPartitionPath);
//            }

            String tablePath = probingPhase(table1Index, table2Index, totalColumn);

            totalColumn = SqlColumn.concatTemp(table1.column,
                    table2.column,
                    table1.tableName.split("_")[0],
                    table2.tableName.split("_")[0]);
            return SqlTable.constructTableFromMergeSorted(totalColumn, tablePath);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    private void partitioningPhase(String inputFilePath, int recursiveCount, int interestingIndex) throws Exception {
        // init IO stuffs
        // buffered reader
        FileReader fr = new FileReader(inputFilePath);
        BufferedReader br = new BufferedReader(fr);
        int inputRecordCount = 0;
        if (recursiveCount == 0) {
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
                inputRecordCount++;
                if (bufferPointer[0] == PAGE_SIZE) break;
            }

            if (bufferPointer[0] == 0) {
                // read 쪽에서 읽을 것이 없을 경우 break
                break;
            }

            for (int i = 0; i < bufferPointer[0]; i++) {
                int hashResult;
                // TODO: 여기서는 처음에만 hashFunction() 사용하고, 그 이후부터는 SHA-256 이용한 customHashFunction() 사용
                if (recursiveCount == 0) hashResult = hashFunction(buffer[0][i].values.get(interestingIndex).toString());
                else hashResult = customHashFunction(Integer.toString(recursiveCount), buffer[0][i].values.get(interestingIndex).toString());
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
            if (recordCount[i] > BUFFER_SIZE - 2 && inputRecordCount != recordCount[i]) {
                // same value for the join attributes OR hash function does note have randomness / uniformity 라면
                // OR partition 다 안끝났을 때

                partitioningPhase(path, recursiveCount + 1, interestingIndex);
            } else {
                // 다 끝나면, Probing Phase 를 위해 path 저장
                endPartitionPaths.add(path);
            }
        }
    }

    // buffer 0번째 index => input buffer, 1번째 index => output buffer (joined record)
    private String probingPhase(int table1ColumnIndex, int table2ColumnIndex, SqlColumn totalColumn) throws Exception {
        // init for buffer
        buffer = new SqlRecord[BUFFER_SIZE][PAGE_SIZE];
        bufferPointer = new int[BUFFER_SIZE];

        // init for hashtable
        // Hash Table을 만들기 위함이지만, Memory Size 고려해야 하기 때문
        int maxTableRecordSize = (BUFFER_SIZE - 2) * PAGE_SIZE;
        int tableRecordCount = 0;
        hashTable = new LinkedList[BUFFER_SIZE - 2];

        // Write
        String tablePath = joinDir + "/hash-joined.txt";
        FileWriter fw = new FileWriter(tablePath);
        BufferedWriter bw = new BufferedWriter(fw);

        int table1Index = 0;
        int table2Index = firstPartitionCount;
        while (table1Index < firstPartitionCount && table2Index < endPartitionPaths.size()) {
            // hash table 생성
            // 못쓰는 buffer 공간을 이용하여 arrayList (linked list) 로 이루어진 hash table 생성
            String table1Path = endPartitionPaths.get(table1Index);
            String table2Path = endPartitionPaths.get(table2Index);

            int compareResult = comparePartition(table1Path, table2Path);
            if (compareResult == 1) {
                table2Index++;
                continue;
            }
            else if (compareResult == 2) {
                table1Index++;
                continue;
            }

            FileReader fr1 = new FileReader(table1Path);
            BufferedReader br1 = new BufferedReader(fr1);

            while (true) {
                // init
                bufferPointer[0] = 0;
                tableRecordCount = 0;

                for (int i = 0; i < BUFFER_SIZE - 2; i++) {
                    hashTable[i] = new LinkedList<>();
                }

                // 최대한 hash table 만들기
                String line = null;
                while ((line = br1.readLine()) != null) {
                    String[] lineItmes = line.split(" ");
                    buffer[0][bufferPointer[0]] = SqlRecord.constructRecord(null, lineItmes);
                    int hashResult = hashFunction2(buffer[0][bufferPointer[0]].values.get(table1ColumnIndex).toString());
                    hashTable[hashResult].add(buffer[0][bufferPointer[0]]);
                    if (++tableRecordCount >= maxTableRecordSize) break;
                }

                if (tableRecordCount == 0) break;

                FileReader fr2 = new FileReader(table2Path);
                BufferedReader br2 = new BufferedReader(fr2);

                while (true) {
                    bufferPointer[0] = 0;

                    // 비교를 위한 input buffer 관련
                    while ((line = br2.readLine()) != null) {
                        String[] lineItmes = line.split(" ");
                        buffer[0][bufferPointer[0]++] = SqlRecord.constructRecord(null, lineItmes);
                        if (PAGE_SIZE == bufferPointer[0]) break;
                    }

                    if (bufferPointer[0] == 0) break;

                    for (int i = 0; i < bufferPointer[0]; i++) {
                        int hashResult = hashFunction2(buffer[0][i].values.get(table2ColumnIndex).toString());
                        for (int j = 0; j < hashTable[hashResult].size(); j++) {
                            if (SqlValue.compare(buffer[0][i].values.get(table2ColumnIndex), hashTable[hashResult].get(j).values.get(table1ColumnIndex)) != 0) {
                                continue;
                            }
                            int table1Size = hashTable[hashResult].get(j).values.size();
                            int table2Size = buffer[0][i].values.size();
                            String[] totalItems = new String[table1Size + table2Size];
                            for (int k = 0; k < totalItems.length; k++) {
                                // TODO: Type..?
                                if (k < table1Size) {
                                    totalItems[k] = hashTable[hashResult].get(j).values.get(k).toString();
                                } else {
                                    totalItems[k] = buffer[0][i].values.get(k - table1Size).toString();
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
                }


                // Output Buffer 나머지 부분
                for (int i = 0; i < bufferPointer[BUFFER_SIZE - 1]; i++) {
                    for (int j = 0; j < buffer[BUFFER_SIZE - 1][i].values.size(); j++) {
                        bw.write(buffer[BUFFER_SIZE - 1][i].values.get(j).toString() + " ");
                    }
                    bw.write("\n");
                }

                bufferPointer[BUFFER_SIZE - 1] = 0;

                br2.close();
                fr2.close();
            }

            if (compareResult == 0) table2Index++;
            else if (compareResult == 3) table1Index++;
            else if (compareResult == 4) table2Index++;

            br1.close();
            fr1.close();
        }

        bw.close();
        fw.close();

        return tablePath;
    }

    /*
        partition file 의 name 을 비교하는 method

        return
        equal - 둘 다 같음
            0: equal
        diff - inner (table2) 를 다음으로
            1: input1 이 큼
            2: input2 가 큼
        short
            3: input1이 김 - 동작 후 다음 file을 통해 hash table (table 1) 새로 construct
            4: input2가 김 - 동작 후 다음 file을 통해 안쪽(table 2) 다음으로
     */
    private int comparePartition(String input1, String input2) {
        // TODO: 하드코딩
        String[] token1 = input1.split("\\.")[0].split("_");
        String[] token2 = input2.split("\\.")[0].split("_");

        if (token1.length < 3 || token2.length < 3) {
            return -1;
        }

        int index = 2;
        while (index <= Math.max(token1.length, token2.length)) {
            int token1Value = -1;
            int token2Value = -1;
            if (token1.length > index) {
                token1Value = Integer.valueOf(token1[index]);
            }
            if (token2.length > index) {
                token2Value = Integer.valueOf(token2[index]);
            }

            if (token1Value != -1 && token2Value != -1) {
                if (token1Value > token2Value) return 1;
                else if (token1Value < token2Value) return 2;
                else return 0;
            } else if (token1Value != -1) {
                return 3;
            } else if (token2Value != -1) {
                return 4;
            }

            index++;
        }

        return -1;
    }

    // 아래 3개의 hash function에 대해서
    // TODO: Int String 말고, 정말 String 인 type 에 대해서 개선해야 함. (requirement: sid, rid 이기 때문에 생략)

    private int hashFunction(String value) {
        return Integer.valueOf(value) % (BUFFER_SIZE - 1);
    }

    private int customHashFunction(String seed, String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(seed.getBytes());
            return (md.digest()[md.digest().length - 1] * Integer.valueOf(value)) % (BUFFER_SIZE - 1);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return -1;
    }

    private int hashFunction2(String value) {
        return Integer.valueOf(value) % (BUFFER_SIZE - 2);
    }

}
