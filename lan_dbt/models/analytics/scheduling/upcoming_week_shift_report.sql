{{ config(materialized='view') }}

with
    -- Define specific cost centers to include (same as sched_hours)
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

    -- Get the next week (Sunday through Saturday)
    next_week_range as (
        select
            -- Find next Sunday (or today if today is Sunday)
            case
                when extract(dow from current_date) = 0 then current_date  -- Today is Sunday
                else current_date + (7 - extract(dow from current_date)::integer)  -- Days until next Sunday
            end as week_start,
            -- Saturday is 6 days after Sunday
            case
                when extract(dow from current_date) = 0 then current_date + 6
                else current_date + (7 - extract(dow from current_date)::integer) + 6
            end as week_end
    ),

    schedule_raw as (
        select s.*
        from {{ ref('stg_schedule') }} s
        inner join cost_center_list cc
            on s.cost_center_id = cc.cost_center_id
            and s.source_database = cc.source_database
        cross join next_week_range nwr
        where s.date_line >= nwr.week_start
            and s.date_line <= nwr.week_end
            -- Include both training and non-training shifts
            -- BUT exclude shifts that are both open AND training
            and not (s.assignment_status = 'OPEN' and s.is_training = true)
    ),

    -- Deduplicate schedule data based on assignment_id
    schedule as (
        select distinct on (assignment_id) *
        from schedule_raw
        order by assignment_id
    ),

    -- Calculate cumulative hours per employee through the week
    employee_cumulative as (
        select
            source_database,
            user_id,
            date_line,
            shift_start,
            assignment_id,
            sum(scheduled_hours) over (
                partition by source_database, user_id
                order by date_line, shift_start, assignment_id
                rows between unbounded preceding and current row
            ) as cumulative_hours_week
        from schedule
        where assignment_status = 'ASSIGNED'
            and user_id is not null
    ),

    -- Calculate total scheduled hours for each employee for the week
    employee_week_totals as (
        select
            source_database,
            user_id,
            sum(scheduled_hours) as employee_total_scheduled_hours,
            count(*) as employee_total_shifts
        from schedule
        where assignment_status = 'ASSIGNED'
            and user_id is not null
        group by source_database, user_id
    ),

    -- Check if employee has overtime (>40 hours)
    employee_overtime as (
        select
            source_database,
            user_id,
            employee_total_scheduled_hours,
            case
                when employee_total_scheduled_hours > 40 then true
                else false
            end as has_overtime,
            case
                when employee_total_scheduled_hours > 40
                then employee_total_scheduled_hours - 40
                else 0
            end as overtime_hours
        from employee_week_totals
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
    -- Date and market organization
    case
        when s.source_database = 'tn' and (s.cost_center_name like 'Memp%' or s.cost_center_name like 'Miss%') then 'Memphis'
        when s.source_database = 'tn' and s.cost_center_name like 'Nash%' then 'Nashville'
        when s.source_database = 'mi' then 'Michigan'
        when s.source_database = 'il' then 'Illinois'
    end as region,
    s.date_line as shift_date,
    s.day_name,

    -- Employee information (NULL for open shifts)
    s.assigned_name,
    s.employee_num,
    s.job_title,

    -- Shift timing
    s.shift_start,
    s.shift_end,
    -- For open shifts, calculate hours from start/end times if scheduled_hours is 0
    case
        when s.scheduled_hours > 0 then s.scheduled_hours
        when s.shift_start is not null and s.shift_end is not null then
            extract(epoch from (s.shift_end - s.shift_start)) / 3600.0
        else 0
    end as scheduled_hours,

    -- Employee hours tracking
    coalesce(ec.cumulative_hours_week, 0) as cumulative_hours_week,
    coalesce(eo.employee_total_scheduled_hours, 0) as employee_week_total_hours,
    coalesce(eo.has_overtime, false) as employee_has_overtime,
    coalesce(eo.overtime_hours, 0) as employee_overtime_hours,

    -- Unit and assignment details
    s.unit_name,
    s.position_slot,
    s.assignment_status,
    case
        when s.assignment_status = 'OPEN' then true
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
    case when s.date_line = current_date then true else false end as is_today,
    case when s.date_line = current_date + 1 then true else false end as is_tomorrow,
    case when s.date_line <= current_date + 3 then true else false end as is_within_3_days,
    (s.date_line - current_date) as days_until_shift,

    -- Cost center info
    s.cost_center_name,

    -- Training indicator
    s.is_training,

    -- Daily open shift summary (repeated for each row on that day/market)
    dos.open_shift_count as day_market_open_shifts,
    dos.open_hours as day_market_open_hours

from schedule s
left join employee_cumulative ec
    on s.assignment_id = ec.assignment_id
left join employee_overtime eo
    on s.source_database = eo.source_database
    and s.user_id = eo.user_id
left join shift_partners sp
    on s.assignment_id = sp.assignment_id
left join shift_staffing ss
    on s.source_database = ss.source_database
    and s.unit_id = ss.unit_id
    and s.date_line = ss.date_line
    and s.shift_start = ss.shift_start
left join daily_open_summary dos
    on case
        when s.source_database = 'tn' and (s.cost_center_name like 'Memp%' or s.cost_center_name like 'Miss%') then 'Memphis'
        when s.source_database = 'tn' and s.cost_center_name like 'Nash%' then 'Nashville'
        when s.source_database = 'mi' then 'Michigan'
        when s.source_database = 'il' then 'Illinois'
    end = dos.region
    and s.date_line = dos.shift_date

order by region, s.date_line, s.shift_start, s.unit_name, s.assigned_name