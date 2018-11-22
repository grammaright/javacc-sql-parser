# javacc-sql-parser

지식 및 데이터베이스 시스템 Project 입니다.


# 넣어야 할 이야기들

- 주요 알고리즘의 경우에만 Disk base 로 동작함. (????)
- JoinProcessor 내의 통해서 동작됨 (어떤거로 할 지 선택함)
- Block Nested Join 의 경우 Buffer page size 만 고려
- Sort-Merge Join 의 경우, Merge-Sort phase 의 경우 Buffer 와 Page size 를 고려. 이후 join phase 시에는 메모리 상에서 동작
- Hash Join 의 경우, Buffer page size 만 고려.


## 구성

- `kr.ac.snu.dbs.koo.SqlGrammar` 는 javacc를 이용한 jj 파일 변환 결과입니다.
- `kr.ac.snu.dbs.koo.SqlProcessor` 는 Database System 구현체입니다.
- `kr.ac.snu.dbs.koo.MergeSort` 는 External Merge Sort 구현체입니다.
- `kr.ac.snu.dbs.koo.JoinProcessor` 는 Join 구현체입니다.

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

## 진행 단계

Project 2 단계로 아래와 같이 수행됩니다. 

```
1. 무조건 오름차순으로 출력
2. 한 column에 대해서만 sorting한다고 가정
3. sorting이 필요한 경우 from문을 통해 한 테이블만 가져온다고 가정 (즉, join을 고려하지 않음)
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

실험 결과는 단일 측정으로, 정확하지 않을 수 있습니다.

- `PAGE_SIZE=2`, `BLOCK_SIZE=4`로 실험하였을 때, 약 1377388 ms 소요됩니다.
- `PAGE_SIZE=4096`, `BLOCK_SIZE=4`로 실험하였을 때, 약 117220 ms 소요됩니다.
- `PAGE_SIZE=4096`, `BLOCK_SIZE=32`로 실험하였을 때, 약 114264 ms 소요됩니다.
- `PAGE_SIZE=4096`, `BLOCK_SIZE=64`로 실험하였을 때, 약 146022 ms 소요됩니다.
- `PAGE_SIZE=4096`, `BLOCK_SIZE=256`로 실험하였을 때, 약 158320 ms 소요됩니다.


## Requirements 

- java 1.8


## TODO
- [ ] Table명 대소문자 관련 issue (APFS 의 경우 대소문자 구분 안해서 생기는 문제일 수 있음)
- [ ] `order by` 있을 경우, `constructTable() -> orderTable()` 말고, `orderTable()` 으로 한번에 처리하도록 수정
- [ ] `Exception` 일괄 수정
