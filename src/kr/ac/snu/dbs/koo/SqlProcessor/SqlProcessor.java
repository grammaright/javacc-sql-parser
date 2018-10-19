package kr.ac.snu.dbs.koo.SqlProcessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import kr.ac.snu.dbs.koo.SqlGrammar.Types.Attributer;
import kr.ac.snu.dbs.koo.SqlGrammar.Types.Formula;
import kr.ac.snu.dbs.koo.SqlGrammar.ParseException;
import kr.ac.snu.dbs.koo.MergeSort.MergeSort;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlTable;

public class SqlProcessor {

    public static boolean DEBUGGING = true;

    private ArrayList<Attributer> projection = null;
    private ArrayList<String> tables = null;
    private ArrayList<Formula> whereList = null;
    private Attributer orderList = null;
    
    private ArrayList<String> cols = new ArrayList<>();
    private ArrayList<ArrayList<String>> values = new ArrayList<>();

    public static void run(ArrayList<Attributer> projection, ArrayList<String> tables, ArrayList<Formula> whereList,
            Attributer orderList) throws ParseException {

//        1. 무조건 오름차순으로 출력
//        2. 한 column에 대해서만 sorting한다고 가정
//        3. sorting이 필요한 경우 from문을 통해 한 테이블만 가져온다고 가정 (즉, join을 고려하지 않음)
//        4. 값이 같은 경우 원래 저장된 테이블 순서대로 출력 
//        5. WHERE, ORDER BY가 같이 있는 경우 ORDER BY를 처리하고 WHERE을 처리할 것

        SqlProcessor sp = new SqlProcessor(projection, tables, whereList, orderList);
        sp.runQuery();
    }

    SqlProcessor(ArrayList<Attributer> projection, ArrayList<String> tables, ArrayList<Formula> whereList,
            Attributer orderList) {
        this.projection = projection;
        this.tables = tables;
        this.whereList = whereList;
        this.orderList = orderList;
    }

    public void raiseExceptions() throws ParseException {
        // 3. join X
        if (whereList != null && whereList.size() > 1) {
            throw new ParseException();
        }

        // 레거시
        // temporal exception for NoTableFounded
        for (int i = 0; i < projection.size(); i++) {
            Attributer item = projection.get(i);
            if (item.table == null) {
                continue;
            }

            boolean isMatching = false;
            for (int j = 0; j < tables.size(); j++) {
                if (item.table.equals(tables.get(j))) {
                    isMatching = true;
                    break;
                }
            }

            if (isMatching == false) {
                throw new ParseException();
            }
        }

        // where
        if (whereList != null) {
            for (int i = 0; i < whereList.size(); i++) {
                Formula item = whereList.get(i);
                boolean isMatching = false;
                if (item.lvalue.table == null) {
                    // e.g. 20 < s.age is not valid
                    throw new ParseException();
                } else {
                    // lvalue
                    for (int j = 0; j < tables.size(); j++) {
                        if (item.lvalue.table.equals(tables.get(j))) {
                            isMatching = true;
                            break;
                        }
                    }
                    if (isMatching == false) {
                        throw new ParseException();
                    }
                }

                isMatching = false;
                if (item.rvalue.table != null) {
                    // rvalue
                    for (int j = 0; j < tables.size(); j++) {
                        if (item.rvalue.table.equals(tables.get(j))) {
                            isMatching = true;
                            break;
                        }
                    }
                    if (isMatching == false) {
                        throw new ParseException();
                    }
                }
            }
        }
    }

    public void runQuery() throws ParseException {
        if (DEBUGGING) debug();
        
        raiseExceptions();

        // TODO: Interesting orders: from 절의 table 도 고려해야 함.
        HashSet<String> interestingOrder = constructInterestingOrder();

        // TODO: 현재는 Table 1개만 고려
        SqlTable table = SqlTable.constructTable(tables.get(0), interestingOrder);

        // order by
        if (orderList != null) {
            table = MergeSort.orderTable(table, orderList);
        }
        
        // where
        if (whereList != null) {
            for (int i = 0; i < whereList.size(); i++) {
                Formula item = whereList.get(i);
                if (item.operend.equals("=") || item.operend.equals("==")) {
                    int index = cols.indexOf(item.lvalue.attribute);
                    for (int j = 0; j < values.size(); j++) {
                        String target = values.get(j).get(index);
                        if (!target.equals(item.rvalue.attribute)) {
                            values.remove(j);
                            j--;
                            continue;
                        }
                    }
                } else if (item.operend.equals("<")) {
                    int index = cols.indexOf(item.lvalue.attribute);
                    for (int j = 0; j < values.size(); j++) {
                        int target = Integer.parseInt(values.get(j).get(index));
                        int rvalue = Integer.parseInt(item.rvalue.attribute);
                        if (!(target < rvalue)) {
                            values.remove(j);
                            j--;
                            continue;
                        }
                    }
                } else if (item.operend.equals(">")) {
                    int index = cols.indexOf(item.lvalue.attribute);
                    for (int j = 0; j < values.size(); j++) {
                        int target = Integer.parseInt(values.get(j).get(index));
                        int rvalue = Integer.parseInt(item.rvalue.attribute);
                        if (!(target > rvalue)) {
                            values.remove(j);
                            j--;
                            continue;
                        }
                    }
                }
            }
        }
        
        printTables();
    }

    private HashSet<String> constructInterestingOrder() {
        HashSet<String> result = new HashSet<>();
        for (int i = 0; i < projection.size(); i++) {
            result.add(projection.get(i).attribute);
        }

        for (int i = 0; i < whereList.size(); i++) {
            result.add(whereList.get(i).lvalue.attribute);
        }

        result.add(orderList.attribute);

        return result;
    }
    
    // projection 도 함 
    private void printTables() {
        ArrayList<Integer> printIndexes = new ArrayList<>();
        if (projection.size() == 1 && projection.get(0).attribute.equals("*")) {
            for (int i = 0; i < cols.size(); i++) {
                printIndexes.add(i);
                System.out.print(cols.get(i) + "\t");
            }
        } else {
            for (int i = 0; i < projection.size(); i++) {
                int index = cols.indexOf(projection.get(i).attribute);
                if (index != -1) {
                    printIndexes.add(index);
                    System.out.print(projection.get(i).attribute + "\t");
                }
            }
        }
        System.out.println();
        
        for (int i = 0; i < values.size(); i++) {
            ArrayList<String> item = values.get(i);
            for (int j = 0; j < printIndexes.size(); j++) {
                System.out.print(item.get(printIndexes.get(j)) + "\t");
            }
            System.out.println();
        }
    }
    
    private ArrayList<String> getOrderResult(String path) {
        ArrayList<String> result = new ArrayList<>();
        
        try {
            FileReader fr = new FileReader(path);
            BufferedReader br = new BufferedReader(fr);

            String line = null;
            while ((line = br.readLine()) != null) {
                String[] items = line.split(" ");
                for (int i = 0; i < items.length; i++) {
                    result.add(items[i]);
                }
            }
            br.close();
            fr.close();
            
        } catch (IOException e) {
            e.printStackTrace();       
        }
        return result;
    }
    
    // For Debug
    public void debug() throws ParseException {
        System.out.println("\n\n[INFO] select caluse");
        for (int i = 0; i < projection.size(); i++) {
            System.out.println("[INFO] " + projection.get(i));
        }
        System.out.println("\n[INFO] from clause");
        for (int i = 0; i < tables.size(); i++) {
            System.out.println("[INFO] " + tables.get(i));
        }
        if (whereList != null) {
            System.out.println("\n[INFO] where clause");
            for (int i = 0; i < whereList.size(); i++) {
                System.out.println("[INFO] " + whereList.get(i));
            }
        }
        if (orderList != null) {
            System.out.println("\n[INFO] order clause");
            System.out.println("[INFO] " + orderList);
//            for (int i = 0; i < order_list.size(); i++) {
//              System.out.println("[INFO] " + order_list.get(i));
//            }
        }
    }
}
