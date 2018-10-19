package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

import java.util.ArrayList;
import java.util.HashSet;

public class SqlColumn {
    public ArrayList<String> values;
    public ArrayList<SqlValueType> types;

    public static SqlColumn constructColumn(String[] items, HashSet<String> interestingOrder) throws Exception {
        for (String item : items) {
            // consider interesting order

            if (interestingOrder != null && !interestingOrder.contains(item)) {
                continue;
            }

            String[] temp = item.split("\\(");
            table.column.values.add(temp[0]);
            String type = temp[1].split("\\)")[0];
            if (type.equals("integer")) {
                table.column.types.add(SqlValueType.INT);
            } else if (type.equals("string")) {
                table.column.types.add(SqlValueType.STRING);
            } else {
                throw new Exception("UnExcepted type");
            }
        }
    }
}
