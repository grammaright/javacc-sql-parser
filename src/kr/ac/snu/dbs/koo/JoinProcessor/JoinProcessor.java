package kr.ac.snu.dbs.koo.JoinProcessor;

import kr.ac.snu.dbs.koo.JoinProcessor.JoinModule.BlockNestedJoin;
import kr.ac.snu.dbs.koo.JoinProcessor.JoinModule.HashJoin;
import kr.ac.snu.dbs.koo.JoinProcessor.JoinModule.SortMergeJoin;
import kr.ac.snu.dbs.koo.SqlGrammar.Types.Formula;
import kr.ac.snu.dbs.koo.SqlProcessor.TableElement.SqlTable;

import java.util.ArrayList;

public class JoinProcessor {
    public enum JoinType {
        BLOCK_NESTED_JOIN,
        SORT_MERGE_JOIN,
        HASH_JOIN
    }

    public static SqlTable join2Table(SqlTable table1, SqlTable table2, ArrayList<Formula> whereList, JoinType type) {
        // whereList 에서 join condition 뽑아냄
        // TODO: 다른 join condition 도 있는지?
        // TODO: 현재 join condition 은 equality check 만 한다고 가정 (왜? 책이 그래서 ㅋㅋ)
        Formula joinCondition = null;
        for (int i = 0; i < whereList.size(); i++) {
            Formula item = whereList.get(i);
            if (item.lvalue.table == null || item.rvalue.table == null) {
                continue;
            }

            if (!item.lvalue.table.equals(item.rvalue.table)) {
                joinCondition = item;
                break;
            }
        }

        // 실행
        if (type.equals(JoinType.BLOCK_NESTED_JOIN)) {
            BlockNestedJoin join = new BlockNestedJoin();
            return join.join2Table(table1, table2, joinCondition);
        } else if (type.equals(JoinType.SORT_MERGE_JOIN)) {
            SortMergeJoin join = new SortMergeJoin();
            return join.join2Table(table1, table2, joinCondition);
        } else if (type.equals(JoinType.HASH_JOIN)) {
            HashJoin join = new HashJoin();
            SqlTable result = join.join2Table(table1, table2, joinCondition);
            join.clear();
            return result;
        }

        //
        return null;
    }
}
