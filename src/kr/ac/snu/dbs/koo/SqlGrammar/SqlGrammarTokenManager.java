/* Generated By:JavaCC: Do not edit this line. SqlGrammarTokenManager.java */
package kr.ac.snu.dbs.koo.SqlGrammar;
import java.util.ArrayList;

/** Token Manager. */
public class SqlGrammarTokenManager implements SqlGrammarConstants
{

  /** Debug output. */
  public static  java.io.PrintStream debugStream = System.out;
  /** Set debug output. */
  public static  void setDebugStream(java.io.PrintStream ds) { debugStream = ds; }
static private int jjStopAtPos(int pos, int kind)
{
   jjmatchedKind = kind;
   jjmatchedPos = pos;
   return pos + 1;
}
static private int jjMoveStringLiteralDfa0_0()
{
   switch(curChar)
   {
      case 9:
         jjmatchedKind = 3;
         return jjMoveNfa_0(1, 0);
      case 10:
         jjmatchedKind = 4;
         return jjMoveNfa_0(1, 0);
      case 13:
         jjmatchedKind = 2;
         return jjMoveNfa_0(1, 0);
      case 32:
         jjmatchedKind = 1;
         return jjMoveNfa_0(1, 0);
      case 42:
         jjmatchedKind = 9;
         return jjMoveNfa_0(1, 0);
      case 44:
         jjmatchedKind = 19;
         return jjMoveNfa_0(1, 0);
      case 46:
         jjmatchedKind = 20;
         return jjMoveNfa_0(1, 0);
      case 59:
         jjmatchedKind = 18;
         return jjMoveNfa_0(1, 0);
      case 60:
         jjmatchedKind = 12;
         return jjMoveNfa_0(1, 0);
      case 62:
         jjmatchedKind = 11;
         return jjMoveNfa_0(1, 0);
      case 65:
         return jjMoveStringLiteralDfa1_0(0x100L);
      case 70:
         return jjMoveStringLiteralDfa1_0(0x40L);
      case 83:
         return jjMoveStringLiteralDfa1_0(0x20L);
      case 87:
         return jjMoveStringLiteralDfa1_0(0x80L);
      case 97:
         return jjMoveStringLiteralDfa1_0(0x100L);
      case 102:
         return jjMoveStringLiteralDfa1_0(0x40L);
      case 115:
         return jjMoveStringLiteralDfa1_0(0x20L);
      case 119:
         return jjMoveStringLiteralDfa1_0(0x80L);
      default :
         return jjMoveNfa_0(1, 0);
   }
}
static private int jjMoveStringLiteralDfa1_0(long active0)
{
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
   return jjMoveNfa_0(1, 0);
   }
   switch(curChar)
   {
      case 69:
         return jjMoveStringLiteralDfa2_0(active0, 0x20L);
      case 72:
         return jjMoveStringLiteralDfa2_0(active0, 0x80L);
      case 78:
         return jjMoveStringLiteralDfa2_0(active0, 0x100L);
      case 82:
         return jjMoveStringLiteralDfa2_0(active0, 0x40L);
      case 101:
         return jjMoveStringLiteralDfa2_0(active0, 0x20L);
      case 104:
         return jjMoveStringLiteralDfa2_0(active0, 0x80L);
      case 110:
         return jjMoveStringLiteralDfa2_0(active0, 0x100L);
      case 114:
         return jjMoveStringLiteralDfa2_0(active0, 0x40L);
      default :
         break;
   }
   return jjMoveNfa_0(1, 1);
}
static private int jjMoveStringLiteralDfa2_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjMoveNfa_0(1, 1);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
   return jjMoveNfa_0(1, 1);
   }
   switch(curChar)
   {
      case 68:
         if ((active0 & 0x100L) != 0L)
         {
            jjmatchedKind = 8;
            jjmatchedPos = 2;
         }
         break;
      case 69:
         return jjMoveStringLiteralDfa3_0(active0, 0x80L);
      case 76:
         return jjMoveStringLiteralDfa3_0(active0, 0x20L);
      case 79:
         return jjMoveStringLiteralDfa3_0(active0, 0x40L);
      case 100:
         if ((active0 & 0x100L) != 0L)
         {
            jjmatchedKind = 8;
            jjmatchedPos = 2;
         }
         break;
      case 101:
         return jjMoveStringLiteralDfa3_0(active0, 0x80L);
      case 108:
         return jjMoveStringLiteralDfa3_0(active0, 0x20L);
      case 111:
         return jjMoveStringLiteralDfa3_0(active0, 0x40L);
      default :
         break;
   }
   return jjMoveNfa_0(1, 2);
}
static private int jjMoveStringLiteralDfa3_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjMoveNfa_0(1, 2);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
   return jjMoveNfa_0(1, 2);
   }
   switch(curChar)
   {
      case 69:
         return jjMoveStringLiteralDfa4_0(active0, 0x20L);
      case 77:
         if ((active0 & 0x40L) != 0L)
         {
            jjmatchedKind = 6;
            jjmatchedPos = 3;
         }
         break;
      case 82:
         return jjMoveStringLiteralDfa4_0(active0, 0x80L);
      case 101:
         return jjMoveStringLiteralDfa4_0(active0, 0x20L);
      case 109:
         if ((active0 & 0x40L) != 0L)
         {
            jjmatchedKind = 6;
            jjmatchedPos = 3;
         }
         break;
      case 114:
         return jjMoveStringLiteralDfa4_0(active0, 0x80L);
      default :
         break;
   }
   return jjMoveNfa_0(1, 3);
}
static private int jjMoveStringLiteralDfa4_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjMoveNfa_0(1, 3);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
   return jjMoveNfa_0(1, 3);
   }
   switch(curChar)
   {
      case 67:
         return jjMoveStringLiteralDfa5_0(active0, 0x20L);
      case 69:
         if ((active0 & 0x80L) != 0L)
         {
            jjmatchedKind = 7;
            jjmatchedPos = 4;
         }
         break;
      case 99:
         return jjMoveStringLiteralDfa5_0(active0, 0x20L);
      case 101:
         if ((active0 & 0x80L) != 0L)
         {
            jjmatchedKind = 7;
            jjmatchedPos = 4;
         }
         break;
      default :
         break;
   }
   return jjMoveNfa_0(1, 4);
}
static private int jjMoveStringLiteralDfa5_0(long old0, long active0)
{
   if (((active0 &= old0)) == 0L)
      return jjMoveNfa_0(1, 4);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) {
   return jjMoveNfa_0(1, 4);
   }
   switch(curChar)
   {
      case 84:
         if ((active0 & 0x20L) != 0L)
         {
            jjmatchedKind = 5;
            jjmatchedPos = 5;
         }
         break;
      case 116:
         if ((active0 & 0x20L) != 0L)
         {
            jjmatchedKind = 5;
            jjmatchedPos = 5;
         }
         break;
      default :
         break;
   }
   return jjMoveNfa_0(1, 5);
}
static private int jjMoveNfa_0(int startState, int curPos)
{
   int strKind = jjmatchedKind;
   int strPos = jjmatchedPos;
   int seenUpto;
   input_stream.backup(seenUpto = curPos + 1);
   try { curChar = input_stream.readChar(); }
   catch(java.io.IOException e) { throw new Error("Internal Error"); }
   curPos = 0;
   int startsAt = 0;
   jjnewStateCnt = 11;
   int i = 1;
   jjstateSet[0] = startState;
   int kind = 0x7fffffff;
   for (;;)
   {
      if (++jjround == 0x7fffffff)
         ReInitRounds();
      if (curChar < 64)
      {
         long l = 1L << curChar;
         do
         {
            switch(jjstateSet[--i])
            {
               case 1:
                  if ((0x3ff000000000000L & l) != 0L)
                  {
                     if (kind > 16)
                        kind = 16;
                     jjCheckNAdd(10);
                  }
                  else if (curChar == 34)
                     jjstateSet[jjnewStateCnt++] = 7;
                  else if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 4;
                  else if (curChar == 61)
                  {
                     if (kind > 10)
                        kind = 10;
                  }
                  if (curChar == 61)
                     jjstateSet[jjnewStateCnt++] = 0;
                  break;
               case 0:
                  if (curChar == 61 && kind > 10)
                     kind = 10;
                  break;
               case 2:
                  if (curChar == 61 && kind > 10)
                     kind = 10;
                  break;
               case 3:
                  if (curChar == 39)
                     jjstateSet[jjnewStateCnt++] = 4;
                  break;
               case 5:
                  if (curChar == 39 && kind > 13)
                     kind = 13;
                  break;
               case 6:
                  if (curChar == 34)
                     jjstateSet[jjnewStateCnt++] = 7;
                  break;
               case 8:
                  if (curChar == 34 && kind > 13)
                     kind = 13;
                  break;
               case 10:
                  if ((0x3ff000000000000L & l) == 0L)
                     break;
                  kind = 16;
                  jjCheckNAdd(10);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else if (curChar < 128)
      {
         long l = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               case 1:
               case 9:
                  if ((0x7fffffe07fffffeL & l) == 0L)
                     break;
                  if (kind > 14)
                     kind = 14;
                  jjCheckNAdd(9);
                  break;
               case 4:
                  if ((0x7fffffe07fffffeL & l) != 0L)
                     jjAddStates(0, 1);
                  break;
               case 7:
                  if ((0x7fffffe07fffffeL & l) != 0L)
                     jjAddStates(2, 3);
                  break;
               default : break;
            }
         } while(i != startsAt);
      }
      else
      {
         int i2 = (curChar & 0xff) >> 6;
         long l2 = 1L << (curChar & 077);
         do
         {
            switch(jjstateSet[--i])
            {
               default : break;
            }
         } while(i != startsAt);
      }
      if (kind != 0x7fffffff)
      {
         jjmatchedKind = kind;
         jjmatchedPos = curPos;
         kind = 0x7fffffff;
      }
      ++curPos;
      if ((i = jjnewStateCnt) == (startsAt = 11 - (jjnewStateCnt = startsAt)))
         break;
      try { curChar = input_stream.readChar(); }
      catch(java.io.IOException e) { break; }
   }
   if (jjmatchedPos > strPos)
      return curPos;

   int toRet = Math.max(curPos, seenUpto);

   if (curPos < toRet)
      for (i = toRet - Math.min(curPos, seenUpto); i-- > 0; )
         try { curChar = input_stream.readChar(); }
         catch(java.io.IOException e) { throw new Error("Internal Error : Please send a bug report."); }

   if (jjmatchedPos < strPos)
   {
      jjmatchedKind = strKind;
      jjmatchedPos = strPos;
   }
   else if (jjmatchedPos == strPos && jjmatchedKind > strKind)
      jjmatchedKind = strKind;

   return toRet;
}
static final int[] jjnextStates = {
   4, 5, 7, 8, 
};

/** Token literal values. */
public static final String[] jjstrLiteralImages = {
"", null, null, null, null, null, null, null, null, "\52", null, "\76", "\74", 
null, null, null, null, null, "\73", "\54", "\56", };

/** Lexer state names. */
public static final String[] lexStateNames = {
   "DEFAULT",
};
static final long[] jjtoToken = {
   0x1fffe1L, 
};
static final long[] jjtoSkip = {
   0x1eL, 
};
static protected SimpleCharStream input_stream;
static private final int[] jjrounds = new int[11];
static private final int[] jjstateSet = new int[22];
static protected char curChar;
/** Constructor. */
public SqlGrammarTokenManager(SimpleCharStream stream){
   if (input_stream != null)
      throw new TokenMgrError("ERROR: Second call to constructor of static lexer. You must use ReInit() to initialize the static variables.", TokenMgrError.STATIC_LEXER_ERROR);
   input_stream = stream;
}

/** Constructor. */
public SqlGrammarTokenManager(SimpleCharStream stream, int lexState){
   this(stream);
   SwitchTo(lexState);
}

/** Reinitialise parser. */
static public void ReInit(SimpleCharStream stream)
{
   jjmatchedPos = jjnewStateCnt = 0;
   curLexState = defaultLexState;
   input_stream = stream;
   ReInitRounds();
}
static private void ReInitRounds()
{
   int i;
   jjround = 0x80000001;
   for (i = 11; i-- > 0;)
      jjrounds[i] = 0x80000000;
}

/** Reinitialise parser. */
static public void ReInit(SimpleCharStream stream, int lexState)
{
   ReInit(stream);
   SwitchTo(lexState);
}

/** Switch to specified lex state. */
static public void SwitchTo(int lexState)
{
   if (lexState >= 1 || lexState < 0)
      throw new TokenMgrError("Error: Ignoring invalid lexical state : " + lexState + ". State unchanged.", TokenMgrError.INVALID_LEXICAL_STATE);
   else
      curLexState = lexState;
}

static protected Token jjFillToken()
{
   final Token t;
   final String curTokenImage;
   final int beginLine;
   final int endLine;
   final int beginColumn;
   final int endColumn;
   String im = jjstrLiteralImages[jjmatchedKind];
   curTokenImage = (im == null) ? input_stream.GetImage() : im;
   beginLine = input_stream.getBeginLine();
   beginColumn = input_stream.getBeginColumn();
   endLine = input_stream.getEndLine();
   endColumn = input_stream.getEndColumn();
   t = Token.newToken(jjmatchedKind, curTokenImage);

   t.beginLine = beginLine;
   t.endLine = endLine;
   t.beginColumn = beginColumn;
   t.endColumn = endColumn;

   return t;
}

static int curLexState = 0;
static int defaultLexState = 0;
static int jjnewStateCnt;
static int jjround;
static int jjmatchedPos;
static int jjmatchedKind;

/** Get the next Token. */
public static Token getNextToken() 
{
  Token matchedToken;
  int curPos = 0;

  EOFLoop :
  for (;;)
  {
   try
   {
      curChar = input_stream.BeginToken();
   }
   catch(java.io.IOException e)
   {
      jjmatchedKind = 0;
      matchedToken = jjFillToken();
      return matchedToken;
   }

   jjmatchedKind = 0x7fffffff;
   jjmatchedPos = 0;
   curPos = jjMoveStringLiteralDfa0_0();
   if (jjmatchedKind != 0x7fffffff)
   {
      if (jjmatchedPos + 1 < curPos)
         input_stream.backup(curPos - jjmatchedPos - 1);
      if ((jjtoToken[jjmatchedKind >> 6] & (1L << (jjmatchedKind & 077))) != 0L)
      {
         matchedToken = jjFillToken();
         return matchedToken;
      }
      else
      {
         continue EOFLoop;
      }
   }
   int error_line = input_stream.getEndLine();
   int error_column = input_stream.getEndColumn();
   String error_after = null;
   boolean EOFSeen = false;
   try { input_stream.readChar(); input_stream.backup(1); }
   catch (java.io.IOException e1) {
      EOFSeen = true;
      error_after = curPos <= 1 ? "" : input_stream.GetImage();
      if (curChar == '\n' || curChar == '\r') {
         error_line++;
         error_column = 0;
      }
      else
         error_column++;
   }
   if (!EOFSeen) {
      input_stream.backup(1);
      error_after = curPos <= 1 ? "" : input_stream.GetImage();
   }
   throw new TokenMgrError(EOFSeen, curLexState, error_line, error_column, error_after, curChar, TokenMgrError.LEXICAL_ERROR);
  }
}

static private void jjCheckNAdd(int state)
{
   if (jjrounds[state] != jjround)
   {
      jjstateSet[jjnewStateCnt++] = state;
      jjrounds[state] = jjround;
   }
}
static private void jjAddStates(int start, int end)
{
   do {
      jjstateSet[jjnewStateCnt++] = jjnextStates[start];
   } while (start++ != end);
}
static private void jjCheckNAddTwoStates(int state1, int state2)
{
   jjCheckNAdd(state1);
   jjCheckNAdd(state2);
}

}
