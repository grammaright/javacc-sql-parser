# javacc-sql-parser

지식 및 데이터베이스 시스템 Project 입니다.


## 구성

```
.
└── kr.ac.snu.dbs.koo
    ├── JoinProcessor
    │   ├── JoinModule
    │   │   ├── BlockNestedJoin.java    # Block-Nested Join 구현체
    │   │   ├── HashJoin.java           # Hash Join 구현체
    │   │   └── SortMergeJoin.java      # Sort-Merge Join 구현체
    │   └── JoinProcessor.java          # Join 총괄 
    ├── MergeSort                       
    │   └── MergeSort.java              # Merge Sort 구현체
    ├── SqlGrammar                      # JavaCC 를 통한 SQL parser
    │   ├── ParseException.java
    │   ├── SimpleCharStream.java
    │   ├── SqlGrammar.java
    │   ├── SqlGrammar.jj
    │   ├── SqlGrammarConstants.java
    │   ├── SqlGrammarTokenManager.java
    │   ├── Token.java
    │   ├── TokenMgrError.java
    │   └── Types                       # SQL 문의 구문 (where, from 절 등)을 위한 구현체
    │       ├── Attributer.java
    │       └── Formula.java
    └── SqlProcessor                    
        ├── SqlProcessor.java           # SQL process 구현체
        ├── SqlUtils.java               # Utils
        └── TableElement                # Table 표현을 위한 Class
            ├── SqlColumn.java
            ├── SqlRecord.java
            ├── SqlTable.java
            ├── SqlValue.java
            ├── SqlValueInteger.java
            ├── SqlValueString.java
            └── SqlValueType.java
```  
    
## 프로그램 사용법

Eclipse 혹은 Intellij에서 import 한 뒤 main()가 존재하는 `SqlGrammar.java` target으로 Run 시키면 됩니다. 동작 후 SQL문을 넣으면 실행 결과가 출력됩니다.

## 시간 측정하는법

기본적으로 query 완료 후, SQL parsing(javacc 부분)을 제외한 모든 부분 (print문 포함)을 수행하는데 걸린 시간을 출력합니다.

예시)
``` sql
SELECT E.emp_no, E.salary from E order by E.salary;
...
Done 2844047 rows in 147673 ms
```

이외의 방법으로는, jar 파일로 export 한 뒤 terminal 의 time 명령어를 통해 실행시간을 측정합니다.


## External Merge Sort 에 관하여

### B 사이즈 변경하는 법

`kr.ac.snu.dbs.koo.MergeSort` 내의 `MergeSort` class 의 `PAGE_SIZE`, `BUFFER_SIZE`를 변경합니다.

### 중간 결과를 확인하기 위해 (pass, run)을 표시하여 파일로 쓸 것

SQL 중 중간 table 결과들이 `resources/tmp/` 에 저장됩니다.
timestamp 명의 directory가 생성되고, 그 내부에 External Merge Sort의 결과가 `B_1_2.txt` (e.g. 1번째 pass 2번째 run)과 같은 형태로 저장됩니다.


## Join 에 관하여

### B 사이즈 변경하는 법

- Block-Nested Join의 경우 `kr.ac.snu.dbs.koo.JoinProcessor.JoinModule` 내의 `BlockNestedJoin` class 의 `PAGE_SIZE`, `BUFFER_SIZE`를 변경합니다.
- Hash Join의 경우 `kr.ac.snu.dbs.koo.JoinProcessor.JoinModule` 내의 `HashJoin` class 의 `PAGE_SIZE`, `BUFFER_SIZE`를 변경합니다.
- Sort-Merge Join의 경우 Merge-Sort 를 사용하기 때문에, `kr.ac.snu.dbs.koo.MergeSort` 내의 `MergeSort` class 의 `PAGE_SIZE`, `BUFFER_SIZE`를 변경합니다.


### Join 방식 수정하는 법

`SqlProcessor` class 내에서, `runQuery()` method 내의 join 관련 `JoinProcessor.join2Table(table1, table2, whereList, JoinProcessor.JoinType.HASH_JOIN);` method 존재합니다.
4번째 파라미터를 동작하고자 하는 Type 으로 변경하시면 됩니다. 


## 진행 단계

Project 3 단계로 아래와 같이 수행됩니다. 

```
1. Sorting 한다면 무조건 오름차순으로 출력
2. 한 column에 대해서만 sorting한다고 가정
3. Sorting을 하는 경우 from문을 통해 한 테이블만 가져온다고 가정 (즉, join과 sorting을 동시에 하지 않음)
4. 값이 같은 경우 원래 저장된 테이블 순서대로 출력 
5. WHERE, ORDER BY가 같이 있는 경우 ORDER BY를 처리하고 WHERE을 처리할 것
```

## Experiments

실험 환경은 아래와 같습니다.

```
CPU: Intel(R) Core(TM) i7-6700HQ CPU @ 2.60GHz
Memory: 16GB
Storage: APPLE SSD SM0256L (250GB)
Filesystem: APFS
```

[test_db](https://github.com/datacharmer/test_db) 의 Employee Dataset을 이용하여 `Employee.emp_no = Salary.emp_no` 를 join한 아래와 같은 table을 사용하였습니다. 총 2844047 row입니다.
```
emp_no(integer) first_name(string) last_name(string) salary(integer) from_date(string) to_date(string)
10001 Georgi Facello 60117 1986-06-26 1987-06-26
10001 Georgi Facello 62102 1987-06-26 1988-06-25
...
499999 Sachin Tsukuda 74327 2000-11-29 2001-11-29
499999 Sachin Tsukuda 77303 2001-11-29 9999-01-01
```

실험에 사용된 SQL문은 아래와 같습니다.
```sql
SELECT E.emp_no, E.salary from E order by E.salary;
...
Done 2844047 rows in 147673 ms
```

### Results

#### Sort

실험 결과는 단일 측정으로, 정확하지 않을 수 있습니다.

- `PAGE_SIZE=2`, `BLOCK_SIZE=4`로 실험하였을 때, 약 1377388 ms 소요됩니다.
- `PAGE_SIZE=4096`, `BLOCK_SIZE=4`로 실험하였을 때, 약 117220 ms 소요됩니다.
- `PAGE_SIZE=4096`, `BLOCK_SIZE=32`로 실험하였을 때, 약 114264 ms 소요됩니다.
- `PAGE_SIZE=4096`, `BLOCK_SIZE=64`로 실험하였을 때, 약 146022 ms 소요됩니다.
- `PAGE_SIZE=4096`, `BLOCK_SIZE=256`로 실험하였을 때, 약 158320 ms 소요됩니다.


## Requirements 

- java 1.8


## TODO
- [ ] self-join 고려
- [ ] Table명 대소문자 관련 issue (APFS 의 경우 대소문자 구분 안해서 생기는 문제일 수 있음)
- [ ] `order by` 있을 경우, `constructTable() -> orderTable()` 말고, `orderTable()` 으로 한번에 처리하도록 수정
- [ ] SqlTable 에 SqlRecord를 전부 담제 말고, all disk-base 로? (현재는 주요 알고리즘의 경우에만 Disk base 로 동작함.)
- [ ] `Exception` 일괄 수정

