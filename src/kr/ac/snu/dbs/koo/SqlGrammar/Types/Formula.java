package kr.ac.snu.dbs.koo.SqlGrammar.Types;


public class Formula {
  public Attributer lvalue = null;
  public String operend = null;
  public Attributer rvalue = null;

  public String toString() {
    return lvalue.toString() + " " + operend + " " + rvalue.toString();
  }
}