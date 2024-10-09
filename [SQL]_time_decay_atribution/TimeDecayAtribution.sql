select *,
-- time decay distribution
 0.5*coalesce(value, 0)*channel_coef_click + 0.5*coalesce(value, 0)*time_coef_click as value_click 
 from (
     select *,
          sum(weight_of_click) over(partition by prospect_id) as summ_weight_of_clicks,
          weight_of_click/(sum(weight_of_click) over(partition by prospect_id)) as time_coef_click,
          sum(weight_of_channel) over(partition by prospect_id) as summ_weight_of_channel_clicks,
          weight_of_channel/(sum(weight_of_channel) over(partition by prospect_id)) as channel_coef_click

     from (
          select *,
          case when sum(date_difference_click_reg) over(partition by prospect_id) - date_difference_click_reg  = 0 then 1
               else sum(date_difference_click_reg) over(partition by prospect_id) - date_difference_click_reg end as weight_of_click

          from (
               select a.*,
               --weight of channel
               -- 0; 1; 2 --> 1;2;3 -> summa = 6 -> 3/6 2/6 1/6
               -- 2 -> 3/6 ; 1 -> 2/6; 0 -> 1/6
               case when channel_coeff = 0 then 1/6
                    when channel_coeff = 1 then 2/6
                    when channel_coeff = 2 then 3/6 end as weight_of_channel,

               --time difference
               case when date_diff( date(registration_date), date(timestamp_seconds(event_timestamp)), day) = 0 then 1
               else date_diff( date(registration_date), date(timestamp_seconds(event_timestamp)), day) end as date_difference_click_reg

               from `clicks_for_atribution` a
               )
          )
     )
;
