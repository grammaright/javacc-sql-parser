package kr.ac.snu.dbs.koo.MergeSort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class MergeSort {
    public static int PAGE_SIZE = 2;
    public static int BUFFER_SIZE = 4;
    
    // buffer init
    // 0 ~ BUFFER_SIZE - 1: Input Buffer
    // BUFFER_SIZE - 1 ~ BUFFER_SIZE: Output Buffer
    private String[][] buffer = new String[BUFFER_SIZE][PAGE_SIZE];
    
    // buffer pointer init 
    private int bufferPointer = 0;
    private int pagePointer[] = new int[BUFFER_SIZE];
    
    // column information
    private String fieldName = "rating";
    private int targetColIndex = -1;
    private boolean isString = false;
    
    private int currentPass = 0;
    private int currentRun = 0;
    private int beforeRun = 0;
    
    
    public static void main(String args []) {
        MergeSort a = new MergeSort();
        a.pipeLine();
        return;
    }
    
    private void pipeLine() {
        firstPass("resources/S2.txt");
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
        // TODO
    }
    
    private void flush() {
        // flush all data (buffer, pointer)
        buffer = new String[BUFFER_SIZE][PAGE_SIZE];
        bufferPointer = 0;
        pagePointer = new int[BUFFER_SIZE];
    }
    
    private void firstPass(String path) {
        try {
            FileReader fr = new FileReader(path);
            BufferedReader br = new BufferedReader(fr);

            String line = null;
            while ((line = br.readLine()) != null) {
                String[] items = line.split(" ");

                // Column name 관련 처리
                if (targetColIndex == -1) {
                    for (int i = 0; i < items.length; i++) {
//                        System.out.println(items[i]);
                        if (fieldName.equals(items[i].split("\\(")[0])) {
                            // col index 
                            targetColIndex = i;
                            
                            // type check
                            // TODO: regex를 쓰던지..
                            String type = items[i].split("\\(")[1].split("\\)")[0];
                            if (type.equals("string")) {
                                isString = true;
                            } else {
                                isString = false;
                            }
                            
                            break;
                        }
                    }
                    
                    if (targetColIndex == -1) {
                        System.out.println("No field name founded.");
                        break;
                    }
                    
                    continue;
                }
                
                fillInputBufferForFirstPass(items[targetColIndex]);
            }
            
            // 끝나고 나머지들 
            writeOutputBufferForFirstPass();
            
            br.close();
            fr.close();
            
        } catch (IOException e) {
            e.printStackTrace();       
        }
    }
    
    private void fillInputBufferForFirstPass(String input) {
        buffer[0][pagePointer[0]] = input;
        pagePointer[0]++;
        if (pagePointer[0] >= PAGE_SIZE) {
            // 
            writeOutputBufferForFirstPass();
            
            // flush
            pagePointer[0] = 0;
        }
    }
    
    private void writeOutputBufferForFirstPass() {
        if (pagePointer[0] == 0) {
            return;
        }
        // TODO: path name
        String currentPath = "resources/B_" + Integer.toString(currentPass) + "_" + Integer.toString(currentRun)  + ".txt";
        
        try {
            FileWriter fw = new FileWriter(currentPath);
            BufferedWriter bw = new BufferedWriter(fw);
            int count = 0;
        
            while (count < pagePointer[0]) {
                // 비교 대상 
                int minIndex = -1;
                int minIntegerValue = Integer.MAX_VALUE;
                for (int i = 0; i < pagePointer[0]; i++) {
                    Object target = buffer[0][i];
                    if (isString == true) {
                        // TODO: pass
                    } else {
                        int castedValue = Integer.parseInt((String) target);
                        if (castedValue < minIntegerValue) {
                            minIntegerValue = castedValue;
                            minIndex = i;
                        }
                    }
                }
                
                // write to file
                bw.write(buffer[0][minIndex] + " ");
                buffer[0][minIndex] = Integer.toString(Integer.MAX_VALUE);
                count += 1;
            }
            
            bw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();       
        }
        
        currentRun += 1;
    }
    
    private void otherPass() {
        // TODO: exception condiiton check
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
        
        String writePath = "resources/B_" + Integer.toString(currentPass) + "_" + Integer.toString(currentRun)  + ".txt";
        FileWriter fw = new FileWriter(writePath, true);
        BufferedWriter bw = new BufferedWriter(fw);
        
        int localPagePointer[] = new int[BUFFER_SIZE];
        int gap = Math.min(scannedRun + BUFFER_SIZE - 1, beforeRun);
        for (int i = scannedRun; i < gap; i++) {
            String currentPath = "resources/B_" + Integer.toString(currentPass - 1) + "_" + Integer.toString(i)  + ".txt";
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
                    String line = null;
                    if (!((line = br[i].readLine()) != null)) {
                        eofCount++;
                        continue;
                    }
                    
                    localPagePointer[i] = 0;
                    pagePointer[i] = 0;
                    String[] items = line.split(" ");
                    for (int j = 0; j < items.length; j++) {
                        buffer[i][pagePointer[i]++] = items[j];
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
            int minIntegerValue = Integer.MAX_VALUE;
            for (int i = 0; i < BUFFER_SIZE - 1; i++) {
                int lowest = Math.min(PAGE_SIZE, pagePointer[i]);
                if (localPagePointer[i] >= lowest) {
                    continue;
                }
                
                Object target = buffer[i][localPagePointer[i]];
                if (isString == true) {
                    // TODO: pass
                } else {
                    int castedValue = Integer.parseInt((String) target);
                    if (castedValue < minIntegerValue) {
                        minIntegerValue = castedValue;
                        minIndex = i;
                    }
                }
            }
            
            // TODO: eofCount 와 실제 다 읽은 경우가 다를 수 있음.
            // TODO: 추후 수정 
            // TODO: 홀수 개 들어가있거나 할때 그런 거같기도..
            if (minIndex == -1) {
                break;
            }
            
            // 해당 input buffer pointer 별경
            localPagePointer[minIndex]++;
            
            // Output buffer 쓰기 
            buffer[BUFFER_SIZE - 1][localPagePointer[BUFFER_SIZE - 1]++] = Integer.toString(minIntegerValue);
            // pointer overflow 
            if (localPagePointer[BUFFER_SIZE - 1] >= PAGE_SIZE) {
                // TODO: write file
                for (int j = 0; j < PAGE_SIZE; j++) {
                    bw.write(buffer[BUFFER_SIZE - 1][j] + " ");
                    buffer[BUFFER_SIZE - 1][j] = Integer.toString(Integer.MAX_VALUE);
                }
                bw.write("\n");
                localPagePointer[BUFFER_SIZE - 1] = 0;
            }
        }

        // last remained
        for (int j = 0; j < localPagePointer[BUFFER_SIZE - 1]; j++) {
            bw.write(buffer[BUFFER_SIZE - 1][j] + " ");
        }
            
        bw.close();
        fw.close();

        for (int i = 0; i < calMin; i++) {
//            String currentPath = "resources/B_" + Integer.toString(currentPass - 1) + "_" + Integer.toString(i)  + ".txt";
            fr[i].close();
            br[i].close();
        }
        
        
        return scannedRun + calMin;
    }

    
}
