package kr.ac.snu.dbs.koo.SqlProcessor;

import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlValueType;

public class SqlUtils {
    public static String sqlValueTypeToString(SqlValueType type) {
        if (type == SqlValueType.INT) return "integer";
        else if (type == SqlValueType.STRING) return "string";

        return "null";
    }
}
