
DECLARE index_ INT64 DEFAULT 1;
DECLARE total_days INT64 DEFAULT 1;

Declare rollingdate_initial date  DEFAULT (DATE_SUB( (SELECT max(date(timestamp_micros(date_table))) FROM `name_value` ), INTERVAL 0 DAY));
Declare rollingdate date  DEFAULT (DATE_SUB( (SELECT max(date(timestamp_micros(date_table))) FROM `name_value` ), INTERVAL 0 DAY));

set total_days = DATE_DIFF(
 DATE (DATE_SUB(current_date(), INTERVAL 1 DAY)),
 DATE  (DATE_SUB( (select rollingdate_initial), INTERVAL 0 DAY)), DAY);


while index_ <= total_days do
    set rollingdate = DATE_SUB(rollingdate_initial, INTERVAL -index_ DAY);

    drop table if exists `clicks_x_events_table`;
    create table `clicks_x_events_table` as
    select distinct
      event_timestamp,
      event_name,
      user_id,
      user_pseudo_id,
      fingerprint
    FROM `events_*` , unnest(event_params) as param
        WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+') BETWEEN FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(rollingdate)) AND FORMAT_TIMESTAMP('%Y%m%d', TIMESTAMP(rollingdate));


    drop table if exists `id_with_collisions`;
    create table `id_with_collisions` as
    with app as (
          select  fgp,
          user_id,
          client_id,
          date_table
          from (
          select  fgp,
             case when user_id = 'undefined' then '' else user_id end as user_id,
             client_id,
             date_table
             from (
            SELECT
             case when t1.fingerprint = '' then null else t1.fingerprint end as fgp,
             t1.user_id as user_id,
             t1.user_pseudo_id as client_id,
             t1.event_timestamp as date_table
             FROM `clicks_x_events_table` t1
             )
                    )
            where (client_id != 'undefined')
        ),
        app_id_from_user as (
            select
            case when fgp = 'undefined' then '' else fgp end fgp,
            user_id,
            client_id, date_table ,
            rank() over(order by user_id) as potentional_applicant_id_for_user
            from app
            where user_id != 'undefined' and user_id != '' and user_id is not null
                            ),
        for_rank_id as (
            select *, rank() over(order by fgp) as rank_fgp
            from app
            where (user_id = 'undefined' or user_id = '' or user_id is null ) and client_id != 'undefined'
            ),
        app_id_no_user as (
            select
            *,
            max(rank_fgp) over(partition by client_id) as max_rank_fgp
            from for_rank_id
                        ),
        name_value_user_exists as (
            select
            -2 as id,
            potentional_applicant_id_for_user,
            'user_id' as name,
            user_id as value,
            1 as user_id_flg,
            date_table
            from app_id_from_user
            group by 1,2,3,4,5,6

        union all

            select
            -2 as id,
            potentional_applicant_id_for_user,
            'client_id' as name,
            client_id as value,
            1 as user_id_flg,
            date_table
            from app_id_from_user
            group by 1,2,3,4,5,6

        union all

            select
            -2 as id,
            potentional_applicant_id_for_user,
            'fingerprint' as name,
            fgp as value,
            1 as user_id_flg,
            date_table
            from app_id_from_user
            group by 1,2,3,4,5,6
                                ),
        name_value_user_no_exists as (
            select
                test_ as id,
                -2 as potentional_applicant_id_for_user,
                'client_id' as name,
                client_id_t2 as value,
                0 as user_id_flg,
                date_table
            from app_id_no_user
            where client_id_t2 is not null and (fgp_t1 != '')
            group by 1,2,3,4,5,6

            union all
            select
                max_rank_fgp as id,
                -2 as potentional_applicant_id_for_user,
                'fingerprint' as name,
                fgp as value,
                0 as user_id_flg,
                date_table
            from app_id_no_user
            where (fgp != '')
            group by 1,2,3,4,5,6

            union all
            select
                -1 as id ,
                -2 as potentional_applicant_id_for_user,
                'client_id' as name,
                client_id as value,
                0 as user_id_flg,
                date_table
                from app_id_no_user
                where (fgp = '')
                group by 1,2,3,4,5,6
                                ),

            all_sample as (
                select * from name_value_user_no_exists
                union all
                select * from name_value_user_exists
                        ),

            super_id_1 as (
              select
                id, potentional_applicant_id_for_user, name, value, user_id_flg,
                rank() over(order by id, potentional_applicant_id_for_user) as super_id, date_table
              from all_sample
              where (id + potentional_applicant_id_for_user > -3) and value != ''
              order by 3 desc
                        ),

            super_id_2 as (
              select
                id, potentional_applicant_id_for_user, name, value, user_id_flg, row_number() over() + max_exists_super_id as super_id, date_table
              from all_sample, (select max(super_id) as max_exists_super_id from super_id_1)
              where id + potentional_applicant_id_for_user = -3
                    ),

            day_with_collisions as (
              select super_id, user_id_flg, name, value, CURRENT_DATETIME() as date_insert,  date_table from super_id_1
              where value is not null
              union all
              select super_id, user_id_flg, name, value, CURRENT_DATETIME() as date_insert,  date_table from super_id_2
              where value is not null
                            )

        select * from day_with_collisions;



        drop table if exists `last_day`;
        create table `last_day` as
        with user_collision as (
            select super_id, real_super_id from (
                    select super_id, case when min(super_id) over(partition by rank_value) < super_id then min(super_id) over(partition by rank_value) else super_id end as real_super_id
            from (
                    select super_id, rank() over(order by value) as rank_value from `id_with_collisions`
                        )
                  )
        group by 1,2
        )
        --dedup
        select distinct min(t2.real_super_id) over(partition by value) as super_id, name, value, date_insert, min(date_table) over(partition by name, value) as date_table
        from `id_with_collisions` t1
        left join user_collision t2 on t2.super_id = t1.super_id;

        insert into `name_value` (
              with max_applicant_id as (
                select coalesce(max(super_id), 1) as max_super_id
                from  `name_value`
                                    ),
                 new_value as (
                    select t1.super_id as super_id_new, t1.name as name_new, t1.value as value_new, t1.date_insert as date_insert_new, t1.date_table as date_table_new, max_super_id,
                           t2.name as name_old, t2.value as value_old, t2.super_id as super_id_old,
                           case when t2.name is not null then 1 else 0 end as name_exists_flg,
                           count(t1.super_id) over(partition by t1.super_id) as total,
                           sum(case when t2.value is not null then 1 else 0 end) over(partition by t1.super_id) as sum_exists_flg
                    from `last_day` t1
                    left join `name_value` t2 on t2.value = t1.value, max_applicant_id t3
                    group by 1,2,3,4,5,6,7,8,9,10)

              select min(super_id_old) over(partition by super_id_new) as super_id, name_new as name, value_new as value, CURRENT_DATETIME() as date_insert, date_table_new as date_table
              from new_value
              where total > sum_exists_flg and sum_exists_flg > 0

              union all

              select max_super_id + rank() over(order by super_id_new) as super_id , name_new as name, value_new as value, CURRENT_DATETIME() as date_insert, date_table_new as date_table
              from new_value
              where sum_exists_flg  = 0
);

    set index_ = index_ + 1;
END WHILE;




drop table if exists `name_value_x_rank`;
create table `name_value_x_rank` as
select super_id,
       name,
       value,
       date_insert,
       date_table,
       min(rank_of_super_id) over(partition by user_id_rank) as rank_of_super_id,
      user_id_rank
from (

      select distinct super_id, name, value, date_insert, date_table,
      case when
        coalesce(last_value(rank_ IGNORE NULLS) over(partition by super_id order by date_table asc, value asc ), 1) = 0 then 1
      else
        coalesce(last_value(rank_ IGNORE NULLS) over(partition by super_id order by date_table asc, value asc ), 1) end as rank_of_super_id,
      user_id_rank
      from (

        select case when super_id_min != super_id then  super_id_min else super_id end as super_id, name, value, date_insert, date_table ,
               case when name = 'user_id' and user_id_exists_flg = 1 then dense_rank() over(partition by case when super_id_min != super_id then super_id_min else super_id end,
               case when name = 'user_id' and user_id_exists_flg = 1 then 1 else 0 end order by date_table asc) else null end as rank_, user_id_rank
        from (
          select
            t1.super_id,
            t1.name,
            t1.value,
            min(t1.date_insert) over(partition by t1.value) as date_insert ,
            min(t1.date_table) over(partition by t1.value) as date_table,
            t1.name_user_id_ex,  count(distinct t1.super_id) over(partition by t1.value) as total, min(t1.super_id) over(partition by t1.value) as super_id_min

 from  `name_value` t1
            )
order by super_id asc, date_table asc
)

;