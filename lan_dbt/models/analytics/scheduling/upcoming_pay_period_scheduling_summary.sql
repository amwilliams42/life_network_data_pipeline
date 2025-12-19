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
            sum(case when weekly_hours > 40 then weekly_hours - 40 else 0 end) as total_overtime_hours
        from employee_weekly_hours
        group by source_database, user_id
    ),

    overtime_summary as (
        select
            source_database,
            count(*) filter (where total_overtime_hours > 0) as employees_with_overtime,
            sum(total_overtime_hours) as total_overtime_hours_scheduled
        from employee_overtime
        group by source_database
    ),

    -- Identify understaffed shifts (units with some positions open on the same shift)
    shift_staffing as (
        select
            source_database,
            unit_id,
            unit_name,
            date_line,
            shift_start,
            count(*) as total_positions,
            count(*) filter (where assignment_status = 'ASSIGNED') as filled_positions,
            count(*) filter (where assignment_status = 'OPEN') as open_positions
        from next_pp_schedule
        group by source_database, unit_id, unit_name, date_line, shift_start
    ),

    understaffed_shifts as (
        select
            source_database,
            count(*) as total_understaffed_shifts,
            sum(open_positions) as total_understaffed_positions
        from shift_staffing
        where open_positions > 0  -- At least one position is open
            and filled_positions > 0  -- But some positions are filled (understaffed vs completely open)
        group by source_database
    ),

    completely_open_shifts as (
        select
            source_database,
            count(*) as total_completely_open_shifts,
            sum(total_positions) as total_completely_open_positions
        from shift_staffing
        where filled_positions = 0  -- No positions filled at all
        group by source_database
    ),

    -- Regional summary metrics
    regional_summary as (
        select
            source_database,

            -- Shift counts
            count(distinct concat(unit_id, '_', date_line, '_', shift_start)) as total_shifts_scheduled,
            count(*) as total_shift_positions_scheduled,
            count(*) filter (where assignment_status = 'ASSIGNED') as positions_filled,
            count(*) filter (where assignment_status = 'OPEN') as positions_open,

            -- Employee counts
            count(distinct user_id) filter (where assignment_status = 'ASSIGNED') as unique_employees_scheduled,

            -- Hours totals
            round(sum(scheduled_hours)::numeric, 1) as total_scheduled_hours,
            round(sum(scheduled_hours) filter (where assignment_status = 'ASSIGNED')::numeric, 1) as scheduled_hours_filled,
            round(sum(open_hours)::numeric, 1) as scheduled_hours_open,

            -- Shift counts by day of week
            count(distinct assignment_id) filter (where day_of_week = 0) as sunday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 1) as monday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 2) as tuesday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 3) as wednesday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 4) as thursday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 5) as friday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 6) as saturday_shifts,

            -- Get pay period dates for reference
            min(report_pay_period_start) as pay_period_start,
            max(report_pay_period_end) as pay_period_end,
            min(report_pay_period_year) as pay_period_year,
            min(report_pay_period_number) as pay_period_number

        from next_pp_schedule
        group by source_database
    )

select
    -- Region identifier
    case
        when rs.source_database = 'tn' then 'Tennessee'
        when rs.source_database = 'mi' then 'Michigan'
        when rs.source_database = 'il' then 'Illinois'
    end as region,
    rs.source_database,

    -- Pay period information
    rs.pay_period_start,
    rs.pay_period_end,
    rs.pay_period_year,
    rs.pay_period_number,
    (rs.pay_period_end - rs.pay_period_start + 1) as pay_period_days,

    -- Shift metrics
    rs.total_shifts_scheduled,
    rs.total_shift_positions_scheduled,
    rs.positions_filled,
    rs.positions_open,

    -- Open and understaffed shift details
    coalesce(cos.total_completely_open_shifts, 0) as completely_open_shifts,
    coalesce(cos.total_completely_open_positions, 0) as completely_open_positions,
    coalesce(us.total_understaffed_shifts, 0) as understaffed_shifts,
    coalesce(us.total_understaffed_positions, 0) as understaffed_positions,

    -- Combined open/understaffed
    coalesce(cos.total_completely_open_shifts, 0) + coalesce(us.total_understaffed_shifts, 0) as total_problem_shifts,
    coalesce(cos.total_completely_open_positions, 0) + coalesce(us.total_understaffed_positions, 0) as total_problem_positions,

    -- Employee counts
    rs.unique_employees_scheduled,

    -- Hours metrics
    rs.total_scheduled_hours,
    rs.scheduled_hours_filled,
    rs.scheduled_hours_open,

    -- Overtime metrics
    coalesce(ot.employees_with_overtime, 0) as employees_with_scheduled_overtime,
    coalesce(ot.total_overtime_hours_scheduled, 0) as total_overtime_hours_scheduled,

    -- Coverage percentages
    round((rs.positions_filled::numeric / nullif(rs.total_shift_positions_scheduled, 0) * 100)::numeric, 1) as coverage_pct,
    round((rs.scheduled_hours_filled::numeric / nullif(rs.total_scheduled_hours, 0) * 100)::numeric, 1) as scheduled_hours_coverage_pct,

    -- Staffing needs (inverse of coverage)
    round(((rs.positions_open::numeric / nullif(rs.total_shift_positions_scheduled, 0)) * 100)::numeric, 1) as positions_open_pct,

    -- Daily shift breakdown
    rs.sunday_shifts,
    rs.monday_shifts,
    rs.tuesday_shifts,
    rs.wednesday_shifts,
    rs.thursday_shifts,
    rs.friday_shifts,
    rs.saturday_shifts

from regional_summary rs
left join overtime_summary ot on rs.source_database = ot.source_database
left join understaffed_shifts us on rs.source_database = us.source_database
left join completely_open_shifts cos on rs.source_database = cos.source_database
order by rs.source_database