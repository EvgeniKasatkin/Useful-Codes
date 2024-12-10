
DROP TABLE IF EXISTS dwh.tests;
CREATE TABLE IF NOT EXISTS dwh.tests (
    customer_id int not null,
    order_id int not null);

insert into dwh.tests (customer_id, order_id)
values (1, 22), (1, 23), (1, 22), (2, 27), (2, 22), (2, 27), (3, 23), (3, 24), (3, 23), (3, 23);

--Query for sample was aggregated with window rank function.

select *
from (

    select *,
           rank() over(partition by customer_id order by total desc, order_id desc) as rn
    from (
        select customer_id, order_id, count(*) as total from dwh.tests group by 1, 2
         ) as t
)as d
where rn = 1;

--Equvivalent query was aggregated with distinct on clause.

select distinct on (customer_id)
    customer_id,
    order_id
from (
    select customer_id, order_id, count(*) as total from dwh.tests group by 1, 2
    ) as t
order by 1, total desc;