## Works

``` sql
SELECT * FROM S;
SELECT * FROM S; SELECT * FROM R;
SELECT S.sid FROM S;
SELECT S.sid, S.sname FROM S;
SELECT S.sid, S.sname FROM S WHERE S.sid = 22;
SELECT S.sid, S.sname FROM S WHERE S.age > 18;
SELECT S.sid, S.sname FROM S WHERE S.name = 'Bob';
SELECT S.sid, S.sname FROM S, R WHERE S.sid = R.sid;
SELECT S.sid, S.sname FROM S, R WHERE S.sid = R.sid AND S.age > 18;
```

## Works with Requirements

copy from project 1 requirements.

- General

All statement is not case sensitive (e.g., “Select”, “select”, “SELECT” are correct).

``` sql
SELECT * FROM S;
SELECT * from S;
Select * from S;
select * from S;
```

Each query is separated by semicolon “;”.

``` sql
SELECT * FROM S; SELECT * FROM R;
SELECT * FROM S; SELECT * FROM R; SELECT * FROM S;
```

- Select statement

This statement can retrieve records with all (i.e., asterisk *) or some attributes from a given table.

``` sql
SELECT * FROM S;
SELECT S.sid FROM S;
```

This statement does NOT need to implement the “DISTINT” grammar.

This statement does NOT need to implement all aggregations such as “MIN” or “AVG”.

- From statement

This statement can have at most two tables (i.e., one table or two tables).

``` sql
SELECT * FROM S;
SELECT * FROM S, R;
```

This statement does NOT need to implement the “AS” grammar.

- Where statement

Where statement may not be given.

``` sql
SELECT * FROM S;
SELECT * FROM S WHERE S.sid = 1;
```

This statement can handle three operations which are “equal”, “greater than” and “less than”. (i.e., ‘=’, ‘>’, ‘<’)

``` sql
SELECT * FROM S WHERE S.sid = 1;
SELECT * FROM S WHERE S.sid < 1;
SELECT * FROM S WHERE S.sid > 1;
```

This statement has at most two conditions with using only “AND” (e.g., S.sid = 22 AND S.age > 18)

``` sql
SELECT * FROM S WHERE S.sid = 22 AND S.age > 18;
```

We assume that the attribute is left-hand-side and the value is right-hand-side (e.g., s.age > 20 is vaild, but 20 < s.age is not).

``` sql
SELECT * FROM S WHERE S.age > 20;
```

## Doesn't Work

``` sql
SELECT;
SELECT *;
SELECT FROM;
SELECT FROM S;
SELECT R.id FROM S;
SELECT R.id FROM S, R, C;
SELECT * FROM;
SELECT * FROM S WHERE 20 > S.age;
SELECT S.sid, S.sname FROM S, R, C WHERE S.sid = 22;
SELECT S.sid, C.wefjio FROM S, R , C WHERE S.sid = 22, D. = 2;
SELECT S.sid, S.sname FROM S, R WHERE S.sid = R.sid AND 18 > S.age;
SELECT S.sid, S.sname FROM S, R WHERE S.sid = R.sid AND S.age > 18 AND S.sid = 5;
SELECT S.sid, S.sname FROM S, R WHERE S.sid = R.sid AND S.age > 18 AND S.sid == 5;
SELECT S.sid, S.sname FROM S, R WHERE S.sid = R.sid AND 18 > S.age AND S.sid == 5;
```
