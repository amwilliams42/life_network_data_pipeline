{{ config(materialized='view') }}

with
    schedule as (
        select * from {{ ref('stg_schedule') }}
    ),

    -- Get next pay period's date range
    next_pay_period as (
        select
            start_date as pay_period_start,
            end_date as pay_period_end,
            pay_period_year,
            pay_period_number
        from {{ ref('pay_periods') }}
        where start_date > current_date
        order by start_date asc
        limit 1
    ),

    -- Filter schedule to next pay period only
    next_pp_schedule as (
        select
            s.*,
            pp.pay_period_start as report_pay_period_start,
            pp.pay_period_end as report_pay_period_end,
            pp.pay_period_year as report_pay_period_year,
            pp.pay_period_number as report_pay_period_number
        from schedule s
        cross join next_pay_period pp
        where s.date_line >= pp.pay_period_start
            and s.date_line <= pp.pay_period_end
            and s.is_training = false  -- Exclude training shifts
    ),

    -- Calculate each employee's hours by week (for proper overtime calculation)
    employee_weekly_hours as (
        select
            source_database,
            user_id,
            (date_trunc('week', date_line + 1)::date - 1) as week_start,  -- Sunday start
            sum(scheduled_hours) as weekly_hours
        from next_pp_schedule
        where assignment_status = 'ASSIGNED'
            and user_id is not null
        group by source_database, user_id, (date_trunc('week', date_line + 1)::date - 1)
    ),

    -- Calculate overtime per employee (sum of weekly overtime hours)
    employee_overtime as (
        select
            source_database,
            user_id,
            sum(case when weekly_hours > 40 then weekly_hours - 40 else 0 end) as total_overtime_hours,
            bool_or(weekly_hours > 40) as has_overtime
        from employee_weekly_hours
        group by source_database, user_id
    ),

    -- Calculate total scheduled hours and shifts for the pay period
    employee_pp_totals as (
        select
            source_database,
            user_id,
            sum(scheduled_hours) as employee_total_scheduled_hours,
            count(*) as employee_total_shifts
        from next_pp_schedule
        where assignment_status = 'ASSIGNED'
            and user_id is not null
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
        from next_pp_schedule s1
        left join next_pp_schedule s2
            on s1.unit_id = s2.unit_id
            and s1.date_line = s2.date_line
            and s1.shift_start = s2.shift_start
            and s1.source_database = s2.source_database
        group by s1.assignment_id
    ),

    -- Check if shift is understaffed (has open positions)
    shift_staffing as (
        select
            source_database,
            unit_id,
            date_line,
            shift_start,
            count(*) as total_positions,
            count(*) filter (where assignment_status = 'ASSIGNED') as filled_positions,
            count(*) filter (where assignment_status = 'OPEN') as open_positions
        from next_pp_schedule
        group by source_database, unit_id, date_line, shift_start
    )

select
    -- Pay period information
    nps.report_pay_period_start as pay_period_start,
    nps.report_pay_period_end as pay_period_end,
    nps.report_pay_period_year as pay_period_year,
    nps.report_pay_period_number as pay_period_number,

    -- Region
    case
        when nps.source_database = 'tn' then 'Tennessee'
        when nps.source_database = 'mi' then 'Michigan'
        when nps.source_database = 'il' then 'Illinois'
    end as region,
    nps.source_database,

    -- Employee identifiers
    nps.user_id,
    nps.employee_num,
    nps.first_name,
    nps.last_name,
    nps.assigned_name,
    nps.job_title,

    -- Shift identifiers
    nps.assignment_id,
    nps.shift_id,

    -- Shift details
    nps.date_line as shift_date,
    nps.day_name,
    nps.shift_start,
    nps.shift_end,
    nps.scheduled_hours,
    (nps.date_line - current_date) as days_until_shift,

    -- Unit information
    nps.unit_id,
    nps.unit_name,
    nps.position_slot,
    nps.required_qualification,

    -- Cost center
    nps.cost_center_id,
    nps.cost_center_name,

    -- Assignment status
    nps.assignment_status,
    nps.published,

    -- Shift status indicators
    case
        when nps.assignment_status = 'OPEN' then true
        else false
    end as is_open_shift,

    case
        when nps.date_line = current_date then true
        else false
    end as is_today,

    case
        when nps.date_line = current_date + 1 then true
        else false
    end as is_tomorrow,

    case
        when nps.date_line <= current_date + 3 then true
        else false
    end as is_within_3_days,

    -- Shift staffing context
    ss.total_positions as shift_total_positions,
    ss.filled_positions as shift_filled_positions,
    ss.open_positions as shift_open_positions,
    case
        when ss.open_positions > 0 and ss.filled_positions > 0 then true
        when ss.filled_positions = 0 then true
        else false
    end as shift_is_understaffed,

    case
        when ss.filled_positions = 0 then 'COMPLETELY_OPEN'
        when ss.open_positions > 0 and ss.filled_positions > 0 then 'UNDERSTAFFED'
        when ss.open_positions = 0 then 'FULLY_STAFFED'
    end as shift_staffing_status,

    -- Partner information
    sp.partner_crew_members,
    sp.partner_count,

    -- Employee pay period totals and overtime flag
    ept.employee_total_scheduled_hours,
    ept.employee_total_shifts,
    coalesce(eot.has_overtime, false) as employee_has_overtime,
    coalesce(eot.total_overtime_hours, 0) as employee_overtime_hours,

    -- Pay/earning information
    nps.earning_code_id,
    nps.description as earning_code_description

from next_pp_schedule nps
left join shift_partners sp on nps.assignment_id = sp.assignment_id
left join shift_staffing ss
    on nps.source_database = ss.source_database
    and nps.unit_id = ss.unit_id
    and nps.date_line = ss.date_line
    and nps.shift_start = ss.shift_start
left join employee_pp_totals ept
    on nps.source_database = ept.source_database
    and nps.user_id = ept.user_id
left join employee_overtime eot
    on nps.source_database = eot.source_database
    and nps.user_id = eot.user_id
order by nps.source_database, nps.date_line, nps.unit_name, nps.assigned_name