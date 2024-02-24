drop table if exists t;

create table t (
    i integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    c integer not null,
    v integer not null,
    p char(1) not null,
    d varchar(10) not null,
    r timestamp default current_timestamp not null,
    s integer not null
);

CREATE INDEX t_c ON t(c desc);

CREATE INDEX t_r ON t(r desc);

commit;