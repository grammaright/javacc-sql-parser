package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

import java.util.ArrayList;

public class SqlRecord {
    public ArrayList<SqlValue> values;

    public static SqlRecord constructRecord(SqlColumn column, String[] items) {
        SqlRecord record = new SqlRecord();
        record.values = new ArrayList<>();

        for (int i = 0; i < column.values.size(); i++) {
            SqlValueType type = column.types.get(i);
            int index = column.columnIndices.get(i);

            if (type == SqlValueType.INT) {
                SqlValueInteger value = new SqlValueInteger(items[index]);
                record.values.add(value);
            } else if (type == SqlValueType.STRING) {
                SqlValueString value = new SqlValueString(items[index]);
                record.values.add(value);
            }
        }

        return record;
    }

    // TODO:
    public static SqlRecord constructRecord(String[] items) {
        SqlRecord record = new SqlRecord();
        record.values = new ArrayList<>();

        for (String item : items) {
            SqlValue value = SqlValue.constructValue(item);
            record.values.add(value);
        }

        return record;
    }
}
