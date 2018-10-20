package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

import java.util.ArrayList;
import java.util.HashSet;

public class SqlColumn {
    public ArrayList<String> values;
    public ArrayList<Integer> columnIndices;
    public ArrayList<SqlValueType> types;

    public static SqlColumn constructColumn(String[] items, HashSet<String> interestingOrder) throws Exception {
        SqlColumn column = new SqlColumn();
        column.values = new ArrayList<>();
        column.columnIndices = new ArrayList<>();
        column.types = new ArrayList<>();

        // asterisk
        boolean existAsterisk = false;
        if (interestingOrder != null && interestingOrder.contains("*")) {
            existAsterisk = true;
            interestingOrder.remove("*");
        }

        for (int i = 0; i < items.length; i++) {
            String item = items[i];
            String[] temp = item.split("\\(");

            // consider interesting order
            if (!existAsterisk) {
                if (interestingOrder != null && !interestingOrder.contains(temp[0])) {
                    interestingOrder.remove(temp[0]);
                    continue;
                }
            }

            if (interestingOrder != null) interestingOrder.remove(temp[0]);
            column.values.add(temp[0]);
            column.columnIndices.add(i);
            String type = temp[1].split("\\)")[0];
            if (type.equals("integer")) {
                column.types.add(SqlValueType.INT);
            } else if (type.equals("string")) {
                column.types.add(SqlValueType.STRING);
            } else {
                throw new Exception("UnExcepted type");
            }
        }

        if (interestingOrder != null && !interestingOrder.isEmpty()) {
           throw new Exception("no column in table");
        }

        return column;
    }
}
