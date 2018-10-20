package kr.ac.snu.dbs.koo.SqlProcessor.TableElement;

public class SqlValueInteger extends SqlValue {
    public int value;

    public SqlValueInteger(String input) {
        value = (int) (Double.parseDouble(input));
        this.type = SqlValueType.INT;
    }

    public String toString() {
        return String.valueOf(value);
    }
}
