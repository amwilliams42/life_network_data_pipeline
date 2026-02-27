{{ config(materialized='view') }}

with
    -- Define specific cost centers to include (same as upcoming_week_shift_report)
    cost_center_list as (
        select cost_center_id, source_database
        from (values
            (29, 'il'), -- Orientation
            (6, 'il'),  -- EMT Carol Stream
            (10, 'il'), -- EMT Chicago
            (22, 'mi'), -- EMT Oakland County
            (3, 'il'),  -- EMT Skokie
            (20, 'mi'), -- EMT Wayne County
            (11, 'mi'), -- Lincoln Park Rescue
            (70, 'tn'), -- Memp - Critical Care
            (47, 'tn'), -- Memp - EMT BLS
            (63, 'tn'), -- Memp - LDT
            (52, 'tn'), -- Memp - Paramedic ALS
            (14, 'tn'), -- Memp - Special Events
            (48, 'tn'), -- Miss - EMT BLS
            (53, 'tn'), -- Miss - Paramedic ALS
            (49, 'tn'), -- Nash - EMT BLS
            (64, 'tn'), -- Nash - LDT
            (54, 'tn'), -- Nash - Paramedic ALS
            (56, 'tn'), -- Nash - Special Events
            (8, 'il'),  -- Paramedic Chicago
            (23, 'mi'), -- Paramedic Oakland County
            (4, 'il'),  -- Paramedic Skokie
            (21, 'mi'), -- Paramedic Wayne County
            (27, 'il')  -- Special Events
        ) as t(cost_center_id, source_database)
    ),

    -- Get the current pay period for each source database
    current_pay_periods as (
        select
            source_database,
            pay_period_id,
            pay_period_number,
            start_date as pay_period_start,
            end_date as pay_period_end
        from {{ ref('stg_pay_periods') }}
        where current_date between start_date and end_date
    ),

    schedule_raw as (
        select
            s.*,
            cpp.pay_period_id,
            cpp.pay_period_number,
            cpp.pay_period_start,
            cpp.pay_period_end
        from {{ ref('stg_schedule') }} s
        inner join cost_center_list cc
            on s.cost_center_id = cc.cost_center_id
            and s.source_database = cc.source_database
        inner join current_pay_periods cpp
            on s.source_database = cpp.source_database
        where s.date_line >= cpp.pay_period_start
            and s.date_line <= cpp.pay_period_end
            -- Include both training and non-training shifts
            -- BUT exclude shifts that are both open AND training
            and not (s.assignment_status = 'OPEN' and s.is_training = true)
            -- Exclude shifts with no unit
            and s.unit_name is not null
    ),

    -- Deduplicate schedule data based on assignment_id
    schedule as (
        select distinct on (assignment_id) *
        from schedule_raw
        order by assignment_id
    ),

    -- Calculate week number within pay period (1 or 2)
    -- Pay periods start on Sunday, so we can use date arithmetic
    schedule_with_week as (
        select
            *,
            case
                when date_line < pay_period_start + 7 then 1
                else 2
            end as pay_period_week
        from schedule
    ),

    -- Calculate cumulative hours per employee through each week
    employee_cumulative as (
        select
            source_database,
            user_id,
            date_line,
            shift_start,
            assignment_id,
            pay_period_week,
            sum(scheduled_hours) over (
                partition by source_database, user_id, pay_period_week
                order by date_line, shift_start, assignment_id
                rows between unbounded preceding and current row
            ) as cumulative_hours_week
        from schedule_with_week
        where assignment_status = 'ASSIGNED'
            and user_id is not null
    ),

    -- Calculate total scheduled hours for each employee per week (for overtime)
    employee_weekly_totals as (
        select
            source_database,
            user_id,
            pay_period_week,
            sum(scheduled_hours) as employee_week_total_hours,
            count(*) as employee_week_total_shifts
        from schedule_with_week
        where assignment_status = 'ASSIGNED'
            and user_id is not null
        group by source_database, user_id, pay_period_week
    ),

    -- Check if employee has overtime in each week (>40 hours)
    employee_weekly_overtime as (
        select
            source_database,
            user_id,
            pay_period_week,
            employee_week_total_hours,
            case
                when employee_week_total_hours > 40 then true
                else false
            end as has_overtime,
            case
                when employee_week_total_hours > 40
                then employee_week_total_hours - 40
                else 0
            end as overtime_hours
        from employee_weekly_totals
    ),

    -- Calculate pay period totals per employee
    employee_pay_period_totals as (
        select
            source_database,
            user_id,
            sum(employee_week_total_hours) as employee_pay_period_total_hours,
            sum(case when employee_week_total_hours > 40 then employee_week_total_hours - 40 else 0 end) as employee_pay_period_overtime_hours
        from employee_weekly_totals
        group by source_database, user_id
    ),

    -- Get shift partners (other crew members on the same unit/shift)
    shift_partners as (
        select
            s1.assignment_id,
            string_agg(
                distinct s2.assigned_name,
                ', '
                order by s2.assigned_name
            ) filter (where s2.assignment_id != s1.assignment_id) as partner_crew_members,
            count(distinct s2.assignment_id) filter (where s2.assignment_id != s1.assignment_id) as partner_count
        from schedule s1
        left join schedule s2
            on s1.unit_id = s2.unit_id
            and s1.date_line = s2.date_line
            and s1.shift_start = s2.shift_start
            and s1.source_database = s2.source_database
        group by s1.assignment_id
    ),

    -- Check shift staffing (has open positions)
    shift_staffing as (
        select
            source_database,
            unit_id,
            date_line,
            shift_start,
            count(*) as total_positions,
            count(*) filter (where assignment_status = 'ASSIGNED') as filled_positions,
            count(*) filter (where assignment_status = 'OPEN') as open_positions
        from schedule
        group by source_database, unit_id, date_line, shift_start
    ),

    -- Summary of open shifts and hours by day and market
    daily_open_summary as (
        select
            case
                when source_database = 'tn' and (cost_center_name like 'Memp%' or cost_center_name like 'Miss%') then 'Memphis'
                when source_database = 'tn' and cost_center_name like 'Nash%' then 'Nashville'
                when source_database = 'mi' then 'Michigan'
                when source_database = 'il' then 'Illinois'
            end as region,
            date_line as shift_date,
            day_name,
            count(*) filter (where assignment_status = 'OPEN') as open_shift_count,
            sum(scheduled_hours) filter (where assignment_status = 'OPEN') as open_hours
        from schedule
        group by
            case
                when source_database = 'tn' and (cost_center_name like 'Memp%' or cost_center_name like 'Miss%') then 'Memphis'
                when source_database = 'tn' and cost_center_name like 'Nash%' then 'Nashville'
                when source_database = 'mi' then 'Michigan'
                when source_database = 'il' then 'Illinois'
            end,
            date_line,
            day_name
    )

-- Main report output
select
    -- Pay period information
    sw.pay_period_id,
    sw.pay_period_number,
    sw.pay_period_start,
    sw.pay_period_end,
    sw.pay_period_week,

    -- Date and market organization
    case
        when sw.source_database = 'tn' and (sw.cost_center_name like 'Memp%' or sw.cost_center_name like 'Miss%') then 'Memphis'
        when sw.source_database = 'tn' and sw.cost_center_name like 'Nash%' then 'Nashville'
        when sw.source_database = 'mi' then 'Michigan'
        when sw.source_database = 'il' then 'Illinois'
    end as region,
    sw.date_line as shift_date,
    sw.day_name,

    -- Employee information (NULL for open shifts)
    sw.assigned_name,
    sw.employee_num,
    sw.job_title,

    -- Shift timing
    sw.shift_start,
    sw.shift_end,
    -- For open shifts, calculate hours from start/end times if scheduled_hours is 0
    case
        when sw.scheduled_hours > 0 then sw.scheduled_hours
        when sw.shift_start is not null and sw.shift_end is not null then
            extract(epoch from (sw.shift_end - sw.shift_start)) / 3600.0
        else 0
    end as scheduled_hours,

    -- Employee hours tracking (weekly)
    coalesce(ec.cumulative_hours_week, 0) as cumulative_hours_week,
    coalesce(ewo.employee_week_total_hours, 0) as employee_week_total_hours,
    coalesce(ewo.has_overtime, false) as employee_has_overtime,
    coalesce(ewo.overtime_hours, 0) as employee_overtime_hours,

    -- Employee pay period totals
    coalesce(eppt.employee_pay_period_total_hours, 0) as employee_pay_period_total_hours,
    coalesce(eppt.employee_pay_period_overtime_hours, 0) as employee_pay_period_overtime_hours,

    -- Unit and assignment details
    sw.unit_name,
    sw.position_slot,
    sw.assignment_status,
    case
        when sw.assignment_status = 'OPEN' then true
        else false
    end as is_open_shift,

    -- Shift staffing context
    case
        when ss.filled_positions = 0 then 'COMPLETELY_OPEN'
        when ss.open_positions > 0 and ss.filled_positions > 0 then 'UNDERSTAFFED'
        when ss.open_positions = 0 then 'FULLY_STAFFED'
    end as shift_staffing_status,
    ss.total_positions as shift_total_positions,
    ss.filled_positions as shift_filled_positions,
    ss.open_positions as shift_open_positions,
    sp.partner_crew_members,
    sp.partner_count,

    -- Urgency indicators
    case when sw.date_line = current_date then true else false end as is_today,
    case when sw.date_line = current_date + 1 then true else false end as is_tomorrow,
    case when sw.date_line <= current_date + 3 then true else false end as is_within_3_days,
    (sw.date_line - current_date) as days_until_shift,

    -- Cost center info
    sw.cost_center_name,

    -- Training indicator
    sw.is_training,

    -- Daily open shift summary (repeated for each row on that day/market)
    dos.open_shift_count as day_market_open_shifts,
    dos.open_hours as day_market_open_hours

from schedule_with_week sw
left join employee_cumulative ec
    on sw.assignment_id = ec.assignment_id
left join employee_weekly_overtime ewo
    on sw.source_database = ewo.source_database
    and sw.user_id = ewo.user_id
    and sw.pay_period_week = ewo.pay_period_week
left join employee_pay_period_totals eppt
    on sw.source_database = eppt.source_database
    and sw.user_id = eppt.user_id
left join shift_partners sp
    on sw.assignment_id = sp.assignment_id
left join shift_staffing ss
    on sw.source_database = ss.source_database
    and sw.unit_id = ss.unit_id
    and sw.date_line = ss.date_line
    and sw.shift_start = ss.shift_start
left join daily_open_summary dos
    on case
        when sw.source_database = 'tn' and (sw.cost_center_name like 'Memp%' or sw.cost_center_name like 'Miss%') then 'Memphis'
        when sw.source_database = 'tn' and sw.cost_center_name like 'Nash%' then 'Nashville'
        when sw.source_database = 'mi' then 'Michigan'
        when sw.source_database = 'il' then 'Illinois'
    end = dos.region
    and sw.date_line = dos.shift_date

order by region, sw.pay_period_week, sw.date_line, sw.shift_start, sw.unit_name, sw.assigned_name