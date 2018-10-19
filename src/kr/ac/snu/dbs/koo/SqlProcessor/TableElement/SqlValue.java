package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

public class SqlValue {

    public SqlValueType type = SqlValueType.NULL;

    public static int compare(SqlValue first, SqlValue second) {
        if (first.type != second.type) {
            // TODO: exception
            return 0;
        }

        if (first.type == SqlValueType.INT) {
            SqlValueInteger f = (SqlValueInteger) first;
            SqlValueInteger s = (SqlValueInteger) second;

            if (f.value > s.value) return -1;
            else if (f.value < s.value) return 1;
            else return 0;
        } else if (first.type == SqlValueType.STRING) {
            SqlValueString f = (SqlValueString) first;
            SqlValueString s = (SqlValueString) second;

            if (f.value.equals(s.value)) return 0;
            for (int i = 0; i < SqlValueString.MAX_CHAR_SIZE; i++) {
                if (f.value.length < (i + 1)) return -1;
                else if (s.value.length < (i + 1)) return 1;

                if (f.value[i] < s.value[i]) return -1;
                else return 1;
            }
        }
        return 0;
    }
}
