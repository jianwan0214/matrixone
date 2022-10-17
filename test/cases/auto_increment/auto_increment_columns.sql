
-- test auto_increment as primary key
drop table if exists t1;

create table t1(
a bigint primary key not null auto_increment,
b tinyint default null,
c smallint,
d int,
e bigint,
f tinyint unsigned,
g smallint unsigned,
h int unsigned,
i bigint unsigned,
j decimal(15, 2),
k decimal(21, 3),
l float,
m double,
n varchar(1000),
o char(100),
p date,
q datetime,
r timestamp,
s json
);
insert into t1 values();

select * from t1;