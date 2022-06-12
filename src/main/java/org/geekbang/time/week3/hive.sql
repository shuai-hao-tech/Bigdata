--建表语句
drop table t_user_hao;
create table t_user_hao(
    userid string,
    sex string,
    age int,
    occupation string,
    zipcode string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
Stored as textfile
;

load  data local inpath '/data/hive/users.dat' OVERWRITE into table t_user_hao;

drop table t_movie_hao;
create table t_movie_hao(
    MovieID string,
    MovieName string,
    MovieType string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
Stored as textfile
;
load  data local inpath '/data/hive/movies.dat' OVERWRITE into table t_movie_hao;


drop table t_rating_hao;
create table t_rating_hao(
    UserID string,
    MovieID string,
    Rate int,
    Times string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
WITH SERDEPROPERTIES ("field.delim"="::")
Stored as textfile
;
load  data local inpath '/data/hive/ratings.dat' OVERWRITE into table t_rating_hao;

--sql
--题目一（简单）
select
    b.age,
    avg(a.rate)
from t_rating_hao a
inner join t_user_hao b
on a.userid = b.userid
where a.movieid='2116'
group by b.age;

--题目二（中等）
select
    c.MovieName,
    avg(a.rate) avgrate,
    count(1) total
from t_rating_hao a
inner join t_user_hao b
on a.userid = b.userid
inner join t_movie_hao c
on a.MovieID = c.MovieID
where b.sex='M'
group by c.MovieName
having count(1) > 50
limit 10;


--题目三（选做）
select
    s.MovieName,s.avgrate,s.total
from (
    select
        c.MovieName MovieName,
        avg(a.rate) over(partition by c.MovieName ) avgrate,
        count(1) over(partition by c.MovieName ) total
    from t_rating_hao a
    inner join t_user_hao b
    on a.userid = b.userid
    inner join t_movie_hao c
    on a.MovieID = c.MovieID
    where b.sex='F'
    ) s
group by s.MovieName,s.avgrate,s.total
order by total desc
limit 10;