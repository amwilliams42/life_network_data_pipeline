{{ config(materialized='view') }}

with
    run_metrics as (
        -- Run metrics are already filtered by valid cost centers via int_run_crew_assignments
        select * from {{ ref('time_on_task_run_metrics') }}
    ),

    sched_hours as (
        -- Use pre-filtered sched_hours which already has correct cost centers
        select * from {{ ref('sched_hours') }}
    ),

    -- Daily run aggregations (with region splitting for TN)
    daily_runs as (
        select
            service_date,
            source_database,
            region,

            -- Run counts
            count(distinct leg_id) as total_runs,
            count(distinct case when is_training then null else leg_id end) as total_runs_excl_training,

            -- Time on Task totals (in hours)
            sum(time_on_task_hours) as total_time_on_task_hours,
            sum(time_on_task_excl_transport_hours) as total_time_on_task_excl_transport_hours,

            -- Average times per run (in minutes)
            avg(time_on_task_minutes) as avg_time_on_task_minutes,
            avg(time_on_task_excl_transport_minutes) as avg_time_on_task_excl_transport_minutes,
            avg(scene_time_minutes) as avg_scene_time_minutes,
            avg(destination_time_minutes) as avg_destination_time_minutes,
            avg(transport_time_minutes) as avg_transport_time_minutes,
            avg(response_time_minutes) as avg_response_time_minutes,

            -- Breakdown by time components
            sum(scene_time_hours) as total_scene_time_hours,
            sum(transport_time_hours) as total_transport_time_hours,
            sum(destination_time_hours) as total_destination_time_hours

        from run_metrics
        group by service_date, source_database, region
    ),

    -- Daily worked hours (aggregate from sched_hours, excluding training and special events)
    -- Split TN into Memphis/Mississippi and Nashville regions
    daily_scheduled as (
        select
            date_line as service_date,
            source_database,

            -- Determine region: il, mi, tn_memphis (includes Miss), tn_nashville
            case
                when source_database = 'il' then 'il'
                when source_database = 'mi' then 'mi'
                when source_database = 'tn' and (cost_center_name like 'Memp%' or cost_center_name like 'Miss%') then 'tn_memphis'
                when source_database = 'tn' and cost_center_name like 'Nash%' then 'tn_nashville'
                else source_database
            end as region,

            -- Total worked hours for the day (exclude training and special events)
            sum(case
                when special_event = false and (is_training = false or is_training is null)
                then worked_hours
                else 0
            end) as total_scheduled_hours,

            sum(case
                when special_event = false and (is_training = false or is_training is null)
                then worked_hours
                else 0
            end) as total_scheduled_hours_excl_training,

            -- Total shifts (count of cost centers with hours, excluding training/events)
            count(distinct case
                when special_event = false and (is_training = false or is_training is null) and worked_hours > 0
                then cost_center_id
            end) as total_shifts,

            count(distinct case
                when special_event = false and (is_training = false or is_training is null) and worked_hours > 0
                then cost_center_id
            end) as total_shifts_excl_training,

            -- Crew member count - not directly available in sched_hours, set to 0 for now
            0 as total_crew_members

        from sched_hours
        group by date_line, source_database, region
    ),

    -- Combine run and schedule data
    daily_combined as (
        select
            coalesce(r.service_date, s.service_date) as service_date,
            coalesce(r.source_database, s.source_database) as source_database,
            coalesce(r.region, s.region) as region,

            -- Run metrics
            coalesce(r.total_runs, 0) as total_runs,
            coalesce(r.total_runs_excl_training, 0) as total_runs_excl_training,
            coalesce(r.total_time_on_task_hours, 0) as total_time_on_task_hours,
            coalesce(r.total_time_on_task_excl_transport_hours, 0) as total_time_on_task_excl_transport_hours,

            -- Average times
            r.avg_time_on_task_minutes,
            r.avg_time_on_task_excl_transport_minutes,
            r.avg_scene_time_minutes,
            r.avg_destination_time_minutes,
            r.avg_transport_time_minutes,
            r.avg_response_time_minutes,

            -- Component totals
            coalesce(r.total_scene_time_hours, 0) as total_scene_time_hours,
            coalesce(r.total_transport_time_hours, 0) as total_transport_time_hours,
            coalesce(r.total_destination_time_hours, 0) as total_destination_time_hours,

            -- Schedule metrics
            coalesce(s.total_scheduled_hours, 0) as total_scheduled_hours,
            coalesce(s.total_scheduled_hours_excl_training, 0) as total_scheduled_hours_excl_training,
            coalesce(s.total_shifts, 0) as total_shifts,
            coalesce(s.total_shifts_excl_training, 0) as total_shifts_excl_training,
            coalesce(s.total_crew_members, 0) as total_crew_members

        from daily_runs as r
        full outer join daily_scheduled as s
            on r.service_date = s.service_date
            and r.source_database = s.source_database
            and r.region = s.region
    ),

    -- Calculate percentages and moving averages
    daily_metrics as (
        select
            *,

            -- Time on Task percentages
            case
                when total_scheduled_hours > 0
                then (total_time_on_task_hours / total_scheduled_hours)
                else 0
            end as time_on_task_pct,

            case
                when total_scheduled_hours > 0
                then (total_time_on_task_excl_transport_hours / total_scheduled_hours)
                else 0
            end as time_on_task_excl_transport_pct,

            -- Unit Hour Utilization (calls per scheduled hour)
            case
                when total_scheduled_hours > 0
                then total_runs::numeric / total_scheduled_hours
                else 0
            end as unit_hour_utilization,

            -- 5-week (35-day) moving averages for ToT percentage (partition by region)
            avg(case
                when total_scheduled_hours > 0
                then (total_time_on_task_hours / total_scheduled_hours)
                else 0
            end) over (
                partition by region
                order by service_date
                rows between 34 preceding and current row
            ) as time_on_task_pct_35day_ma,

            avg(case
                when total_scheduled_hours > 0
                then (total_time_on_task_excl_transport_hours / total_scheduled_hours)
                else 0
            end) over (
                partition by region
                order by service_date
                rows between 34 preceding and current row
            ) as time_on_task_excl_transport_pct_35day_ma,

            -- 5-week moving averages for average times
            avg(avg_scene_time_minutes) over (
                partition by region
                order by service_date
                rows between 34 preceding and current row
            ) as avg_scene_time_minutes_35day_ma,

            avg(avg_destination_time_minutes) over (
                partition by region
                order by service_date
                rows between 34 preceding and current row
            ) as avg_destination_time_minutes_35day_ma,

            avg(avg_transport_time_minutes) over (
                partition by region
                order by service_date
                rows between 34 preceding and current row
            ) as avg_transport_time_minutes_35day_ma,

            -- 5-week moving average for UHU
            avg(case
                when total_scheduled_hours > 0
                then total_runs::numeric / total_scheduled_hours
                else 0
            end) over (
                partition by region
                order by service_date
                rows between 34 preceding and current row
            ) as unit_hour_utilization_35day_ma,

            -- Date analytics
            extract(dow from service_date) as day_of_week,
            to_char(service_date, 'Day') as day_name,
            date_trunc('week', service_date)::date as week_start,
            date_trunc('month', service_date)::date as month_start

        from daily_combined
    )

select * from daily_metrics
order by service_date desc, region
