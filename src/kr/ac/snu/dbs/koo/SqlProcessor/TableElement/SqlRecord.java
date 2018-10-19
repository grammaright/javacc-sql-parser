package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

import java.util.ArrayList;

public class SqlRecord {
    public ArrayList<SqlValue> values;

    public static SqlRecord constructRecord(SqlColumn column, String[] items) {
        SqlRecord record = new SqlRecord();

        for (int i = 0; i < items.length; i++) {
            SqlValueType type = column.types.get(i);
            if (type == SqlValueType.INT) {
                SqlValueInteger value = new SqlValueInteger();
                value.value = (int) (Double.parseDouble(items[i]));
                value.type = type;
                record.values.add(value);
            } else if (type == SqlValueType.STRING) {
                SqlValueString value = new SqlValueString();
                items[i].getChars(0, SqlValueString.MAX_CHAR_SIZE, value.value, 0);
                value.type = type;
                record.values.add(value);
            }
        }

        return record;
    }
}
