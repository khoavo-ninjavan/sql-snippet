select 
e.user_name
, e.query_id
, q.name as query_name
, e.run_count
, r.avg_runtime
from
    (select 
    cast(additional_properties as json)->>'user_name' as user_name
    ,cast(additional_properties as json)->>'query_id' as query_id
    , count(1) as run_count
    from events
    where action = 'execute_query' 
    and date(created_at) = current_date
    and cast(additional_properties as json)->>'query_id' != 'adhoc'
    group by 1,2) e
 left join   
    (select 
    q.id
    , q.name
    , avg(r.runtime) avg_runtime
    from queries q
    left join query_results r using(query_hash) 
    where date(retrieved_at) = current_date
    group by 1,2) r on r.id = cast(e.query_id as integer)
left join queries q on q.id = cast(e.query_id as integer)
order by e.run_count desc

