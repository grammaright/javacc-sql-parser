package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

import java.util.Arrays;

public class SqlValueString extends SqlValue {
    public static int MAX_CHAR_SIZE = 20;

    public char[] value = new char[MAX_CHAR_SIZE];

    public SqlValueString(String input) {
        input.getChars(0, Math.min(SqlValueString.MAX_CHAR_SIZE, input.length()), value, 0);
        this.type = SqlValueType.STRING;
    }

    public String toString() {
        // char -> string 할 때 null 문자 그대로 출력되어서, 일일히 iter하면서 return 하도록 수정
        int realLength = 0;
        while (realLength < MAX_CHAR_SIZE) {
            if (value[realLength++] == 0) break;
        }
        return String.valueOf(value, 0, realLength - 1);
    }
}
