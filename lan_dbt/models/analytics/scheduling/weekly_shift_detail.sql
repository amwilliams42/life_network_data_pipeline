{{ config(materialized='view') }}

with
    schedule as (
        select * from {{ ref('stg_schedule') }}
    ),

    -- Get previous week's date range (Sunday to Saturday)
    previous_week as (
        select
            (date_trunc('week', current_date)::date - interval '7 days')::date as week_start,
            (date_trunc('week', current_date)::date - interval '1 day')::date as week_end
    ),

    -- Filter schedule to previous week only
    prev_week_schedule as (
        select
            s.*,
            pw.week_start as report_week_start,
            pw.week_end as report_week_end
        from schedule s
        cross join previous_week pw
        where s.date_line >= pw.week_start
            and s.date_line <= pw.week_end
            and s.is_training = false  -- Exclude training shifts
    ),

    -- Calculate each employee's total scheduled and worked hours for the week
    employee_weekly_totals as (
        select
            source_database,
            user_id,
            sum(scheduled_hours) as employee_total_scheduled_hours,
            sum(hours_difference) as employee_total_worked_hours,
            count(*) as employee_total_shifts
        from prev_week_schedule
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
        from prev_week_schedule s1
        left join prev_week_schedule s2
            on s1.unit_id = s2.unit_id
            and s1.date_line = s2.date_line
            and s1.shift_start = s2.shift_start
            and s1.source_database = s2.source_database
        group by s1.assignment_id
    ),

    -- Check if shift was understaffed (has open positions)
    shift_staffing as (
        select
            source_database,
            unit_id,
            date_line,
            shift_start,
            count(*) as total_positions,
            count(*) filter (where assignment_status = 'ASSIGNED') as filled_positions,
            count(*) filter (where assignment_status = 'OPEN') as open_positions
        from prev_week_schedule
        group by source_database, unit_id, date_line, shift_start
    )

select
    -- Week information
    pws.report_week_start as week_start,
    pws.report_week_end as week_end,

    -- Region
    case
        when pws.source_database = 'tn' then 'Tennessee'
        when pws.source_database = 'mi' then 'Michigan'
        when pws.source_database = 'il' then 'Illinois'
    end as region,
    pws.source_database,

    -- Employee identifiers
    pws.user_id,
    pws.employee_num,
    pws.first_name,
    pws.last_name,
    pws.assigned_name,
    pws.job_title,

    -- Shift identifiers
    pws.assignment_id,
    pws.shift_id,

    -- Shift details
    pws.date_line as shift_date,
    pws.day_name,
    pws.shift_start,
    pws.shift_end,
    pws.scheduled_hours,

    -- Unit information
    pws.unit_id,
    pws.unit_name,
    pws.position_slot,
    pws.required_qualification,

    -- Cost center
    pws.cost_center_id,
    pws.cost_center_name,

    -- Assignment status
    pws.assignment_status,

    -- Actual time worked
    pws.clock_in_time,
    pws.clock_out_time,
    pws.hours_difference as actual_hours_worked,

    -- Shift status indicators
    case
        when pws.assignment_status = 'OPEN' then true
        else false
    end as is_open_shift,

    case
        when pws.assignment_status = 'ASSIGNED' and pws.clock_in_time is null then true
        else false
    end as is_no_show,

    case
        when pws.assignment_status = 'ASSIGNED'
            and pws.hours_difference is not null
            and pws.hours_difference < pws.scheduled_hours * 0.9  -- Worked less than 90% of scheduled
        then true
        else false
    end as is_callout_partial,

    -- Shift staffing context
    ss.total_positions as shift_total_positions,
    ss.filled_positions as shift_filled_positions,
    ss.open_positions as shift_open_positions,
    case
        when ss.open_positions > 0 and ss.filled_positions > 0 then true
        when ss.filled_positions = 0 then true
        else false
    end as shift_is_understaffed,

    -- Partner information
    sp.partner_crew_members,
    sp.partner_count,

    -- Employee weekly totals and overtime flag
    ewt.employee_total_scheduled_hours,
    ewt.employee_total_worked_hours,
    ewt.employee_total_shifts,
    case
        when ewt.employee_total_scheduled_hours > 40 then true
        else false
    end as employee_has_overtime,
    case
        when ewt.employee_total_scheduled_hours > 40
        then ewt.employee_total_scheduled_hours - 40
        else 0
    end as employee_overtime_hours,

    -- Pay information
    pws.earning_code_id,
    pws.description as earning_code_description,
    pws.pay_period_year,
    pws.pay_period_number,
    pws.pay_period_start,
    pws.pay_period_end

from prev_week_schedule pws
left join shift_partners sp on pws.assignment_id = sp.assignment_id
left join shift_staffing ss
    on pws.source_database = ss.source_database
    and pws.unit_id = ss.unit_id
    and pws.date_line = ss.date_line
    and pws.shift_start = ss.shift_start
left join employee_weekly_totals ewt
    on pws.source_database = ewt.source_database
    and pws.user_id = ewt.user_id
order by pws.source_database, pws.date_line, pws.assigned_name