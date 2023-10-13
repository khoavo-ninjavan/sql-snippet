with transactions_table as(

    select
        t.id as transaction_id
        ,t.comments as reason
        ,t.order_id
        ,t.service_end_time as attempt_datetime
        ,t.updated_at
        ,t.route_id
        ,t.waypoint_id 
        ,if(substr(t.name,-5) ='(RTS)', 'RTS','DD') as "type"
        ,o.tracking_id
        ,o.shipper_id
        ,replace(t.contact,' ','' ) as callee
        ,shippers.name AS shipper_name
        ,CASE
            WHEN (LEFT(shippers.sales_person, 3) IN ('FHN', 'FTS', 'FNO', 'FSO', 'FBD')) OR (LEFT(shippers.sales_person, 3) = 'FHC' AND shippers.name NOT REGEXP 'RTL|FRC') THEN 'Field Sales'
            WHEN (LEFT(shippers.sales_person, 4) IN ('PUDO', 'NSOS')) OR (LEFT(shippers.sales_person, 3) = 'FHC' AND shippers.name REGEXP 'RTL|FRC') THEN 'Retail'
            WHEN LEFT(shippers.sales_person, 3) = 'B2B' THEN 'Corp Sales'
            WHEN LEFT(shippers.sales_person, 2) = 'XB' THEN 'Cross Border'
            WHEN TRIM(SUBSTRING_INDEX(shippers.sales_person, '-', 1)) IN ('DSND', 'SOL') THEN 'NBU'
            WHEN shippers.short_name = 'TOKGISTIC VN' THEN 'Tiktok Domestic'
            WHEN shippers.legacy_id = 824968 THEN 'Tiktok XBorder'
            WHEN lower(shippers.short_name) REGEXP '%shopee%' THEN 'Shopee'
            WHEN lower(shippers.short_name) REGEXP '%lazada%' THEN 'Lazada'
        ELSE 'Others' END AS sales_channel
        
    
    from transactions t force index (service_end_time,type,status)
    
    join orders o  on t.order_id = o.id
        and t.service_end_time <= '{{attempt_date}}' + interval 17 hour
        and t.service_end_time >= '{{attempt_date}}' - interval 7 hour
        and t.type = 'DD'
        and t.status = 'Fail'
        and o.type = 'Normal'
        and t.comments != 'GH1P thất bại'
        
    join shipper_prod_gl.shippers force index (shipper_system_id_legacy_id_idx) on shippers.legacy_id = o.shipper_id
        and shippers.system_id = 'vn'
    
    left join shipper_prod_gl.shipper_metadata meta on shippers.id = meta.shipper_id 
        and meta.key = 'source'
        and meta.value = 'DASH_MOBILE'
    )

,route_table as (
    select
        rl.legacy_id as id
        ,rl.driver_id
        ,rl.hub_id
        
    from route_prod_gl.route_logs rl

    where true
        and rl.id in (select distinct route_id from transactions_table)
        and rl.system_id = ('vn')
    )
    


,hub_table as(
    select
        hub_id
        ,name as hub_name
        ,region_name as region
    
    from sort_prod_gl.hubs h
    
    where true
        and h.system_id = 'vn'
        and h.hub_id in (select distinct hub_id from route_table)
)

,driver_table as (
    select 
        id as driver_id
        ,display_name as driver_name

    from driver_prod_gl.drivers d
    
    where true
        and d.id in (select distinct driver_id from route_table )
        and hub_id != 1 
        and d.system_id in ('VN','vn')
    )

,driver_number as (
    select distinct
        driver_id
        ,first_value(contact_details) over (partition by driver_id order by id desc) as driver_contact
    
    from driver_prod_gl.driver_contacts dc use index (system_id, driver_id)
    
    where true 
        and system_id = 'vn'
        and driver_id in (select distinct driver_id from driver_table)
        and deleted_at is null
)

,pod_table as (
    select 
        waypoint_id
        ,id as waypoint_photo_id
        ,url 
    
    from waypoint_photos w force index (waypoint_id_index)
    
    where True
        and w.waypoint_id in (select distinct waypoint_id from transactions_table)
)
    
    select 
        d.driver_name
        ,t.tracking_id
        ,dc.driver_contact
        ,t.attempt_datetime + interval 7 hour as attempt_datetime
        ,t.updated_at
        ,t.route_id
        ,t.sales_channel
        ,t.order_id
        ,t.shipper_id
        ,h.region
        ,d.driver_id
        ,coalesce(p.url, 'no photo') as pod_photo
        ,t.callee
        ,p.waypoint_photo_id
        ,t.type
        ,r.hub_id
        ,t.transaction_id
        ,t.reason
        ,h.hub_name
        ,t.waypoint_id
        ,t.shipper_name
    
    from transactions_table t 
    
    left join route_table r on t.route_id = r.id
    left join hub_table h on r.hub_id = h.hub_id
    left join driver_table d on r.driver_id = d.driver_id
    left join driver_number dc on d.driver_id = dc.driver_id
    left join pod_table p on t.waypoint_id = p.waypoint_id



