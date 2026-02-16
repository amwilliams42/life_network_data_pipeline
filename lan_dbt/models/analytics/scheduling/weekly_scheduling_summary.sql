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

    -- Calculate overtime: employees with >40 scheduled hours in the week
    employee_weekly_hours as (
        select
            source_database,
            user_id,
            assigned_name,
            sum(scheduled_hours) as total_scheduled_hours,
            sum(hours_worked) as total_worked_hours
        from prev_week_schedule
        where assignment_status = 'ASSIGNED'
            and user_id is not null
        group by source_database, user_id, assigned_name
    ),

    overtime_summary as (
        select
            source_database,
            count(*) filter (where total_scheduled_hours > 40) as employees_with_overtime,
            sum(total_scheduled_hours - 40) filter (where total_scheduled_hours > 40) as total_overtime_hours_scheduled,
            sum(total_worked_hours - 40) filter (where total_worked_hours > 40) as total_overtime_hours_worked
        from employee_weekly_hours
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
        from prev_week_schedule
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
            count(distinct user_id) filter (where assignment_status = 'ASSIGNED') as unique_employees_worked,

            -- Hours totals
            round(sum(scheduled_hours)::numeric, 1) as total_scheduled_hours,
            round(sum(scheduled_hours) filter (where assignment_status = 'ASSIGNED')::numeric, 1) as scheduled_hours_filled,
            round(sum(open_hours)::numeric, 1) as scheduled_hours_open,
            round(sum(hours_worked) filter (where assignment_status = 'ASSIGNED')::numeric, 1) as actual_hours_worked,

            -- Shift counts by day of week
            count(distinct assignment_id) filter (where day_of_week = 0) as sunday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 1) as monday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 2) as tuesday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 3) as wednesday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 4) as thursday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 5) as friday_shifts,
            count(distinct assignment_id) filter (where day_of_week = 6) as saturday_shifts,

            -- Get week dates for reference
            min(report_week_start) as week_start,
            max(report_week_end) as week_end

        from prev_week_schedule
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

    -- Week information
    rs.week_start,
    rs.week_end,

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

    -- Combined open/understaffed for "the works"
    coalesce(cos.total_completely_open_shifts, 0) + coalesce(us.total_understaffed_shifts, 0) as total_problem_shifts,
    coalesce(cos.total_completely_open_positions, 0) + coalesce(us.total_understaffed_positions, 0) as total_problem_positions,

    -- Employee counts
    rs.unique_employees_worked,

    -- Hours metrics
    rs.total_scheduled_hours,
    rs.scheduled_hours_filled,
    rs.scheduled_hours_open,
    rs.actual_hours_worked,

    -- Overtime metrics
    coalesce(ot.employees_with_overtime, 0) as employees_with_scheduled_overtime,
    coalesce(ot.total_overtime_hours_scheduled, 0) as total_overtime_hours_scheduled,
    coalesce(ot.total_overtime_hours_worked, 0) as total_overtime_hours_worked,

    -- Coverage and efficiency percentages
    round((rs.positions_filled::numeric / nullif(rs.total_shift_positions_scheduled, 0) * 100)::numeric, 1) as coverage_pct,
    round((rs.scheduled_hours_filled::numeric / nullif(rs.total_scheduled_hours, 0) * 100)::numeric, 1) as scheduled_hours_coverage_pct,
    round((rs.actual_hours_worked::numeric / nullif(rs.scheduled_hours_filled, 0) * 100)::numeric, 1) as hours_worked_vs_scheduled_pct,

    -- Variance metrics
    round((rs.scheduled_hours_filled - rs.actual_hours_worked)::numeric, 1) as hours_variance,

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
