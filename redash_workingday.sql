set @milestone_time:= now() + interval 7 hour;
TIMESTAMPDIFF(HOUR, "time_to_check", @milestone_time) -- total hours
  -   now_is_rest_day*hour(@milestone_time) -- loại trừ nếu hôm nay là ngày nghỉ
  -   24*((datediff(@milestone_time,latest_scan_datetime) div 7) 
          + MID('0000000100000011000001110000111100011111000000000', 7 * WEEKDAY("time_to_check") + @milestone_weekday + 1, 1)
          + ( SELECT 
                  count(date)
              FROM blocked_dates 
              WHERE TRUE 
              AND deleted_at IS NULL
              AND DATE BETWEEN DATE("time_to_check") AND DATE(@milestone_time)
              AND weekday(date)!=6
              )
          ) -- working day
