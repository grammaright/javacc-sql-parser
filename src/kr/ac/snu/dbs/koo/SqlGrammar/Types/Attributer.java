package kr.ac.snu.dbs.koo.SqlGrammar.Types;


public class Attributer {
  public String table = null;
  public String attribute = null;

  public String toString() {
    return table + "." + attribute;
  }
}
