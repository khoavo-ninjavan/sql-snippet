/*
Abbreviation
h(i): sort_prod_gl.hubs | sh(i): hub_prod_gl.shipments | o: orders | is(i): inbound_scans | ws(i): warehouse_sweeps | sho(i): hub_prod_gl.shipments | s(i): shipper_prod_gl.shippers | rl(i): route_logs | t(i): transactions
-pull orders-
select id from driver_prod_gl.failure_reasons where system_id = 'vn' and description in ('Khách hẹn đổi ngày giờ giao','Khách hẹn đổi địa điểm giao','Thuê bao không liên lạc được','Đổ chuông nhưng khách không nhấc máy')
-region
103:South, 89:HN, 101:North, 91:HCM
*/
WITH 
orders_cfg AS (
    SELECT
        o.id AS order_id
        ,o.tracking_id
        ,o.created_at
        ,o.latest_warehouse_sweep_id
        ,o.latest_inbound_scan_id
        ,CASE
            WHEN substr(trim(s0.short_name),1,6) = 'Shopee' THEN 'Shopee'
            WHEN (LEFT(s0.sales_person, 4) IN ('FHN-', 'FTS-', 'FNO-', 'FSO-', 'FBD-')) OR (LEFT(s0.sales_person, 4) = 'FHC-' AND s0.name NOT REGEXP 'RTL|FRC') THEN 'FS'
            WHEN substr(trim(s0.short_name),1,6) ='Lazada' THEN 'Lazada'
            WHEN s0.legacy_id = 824968 THEN 'TikTok'
            WHEN substr(trim(s0.short_name),1,4) ='Tiki' THEN 'Tiki'
            ELSE 'Others'
        END AS shipper_group
        ,o.shipper_id
        ,o.granular_status
        ,rts
        ,first_value(h.hub_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_hub_id
        ,first_value(h.name) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_hub
        ,first_value(trim(substring(h.name,1,3))) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_province
        ,first_value(t1.seq_no) OVER (PARTITION BY t1.order_id ORDER BY if(t1.service_end_time is not null, t1.service_end_time, '2001-01-01') DESC) AS last_seq
        ,first_value(t1.seq_no) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no ASC) AS first_seq
        ,first_value(t1.service_end_time) OVER (PARTITION BY t1.order_id ORDER BY if(t1.service_end_time is not null, t1.service_end_time, '2001-01-01') DESC) AS last_txn_time
        ,first_value(t1.comments) OVER (PARTITION BY t1.order_id ORDER BY if(t1.service_end_time is not null, t1.service_end_time, '2001-01-01') DESC) AS last_comment

    FROM orders o force index (granular_status, primary, shipper_id)
    JOIN transactions t1 force index (order_id, service_end_time, type, seq_no, waypoint_id) ON o.id = t1.order_id
        AND o.granular_status IN ('On Hold','Arrived at Sorting Hub', 'On Vehicle for Delivery', 'Pending Reschedule')
        AND t1.service_end_time > now() - interval 1 week
        AND t1.type = 'DD'
        AND (t1.seq_no >=4 OR (t1.seq_no =3 AND t1.status != 'Pending'))
    JOIN waypoints wp force index (PRIMARY, created_at, waypoints_routing_zone_id_zone_type_index) ON wp.id = t1.waypoint_id
        AND wp.created_at > now() - interval 1 week
    LEFT JOIN (
        SELECT 
            hubs.name
            ,hubs.hub_id
            ,zones_view.legacy_zone_id
            
        FROM addressing_prod_gl.zones_view
        JOIN sort_prod_gl.hubs force index (system_id) ON zones_view.hub_id = hubs.hub_id
        
        WHERE TRUE
            AND zones_view.system_id = 'vn'
            AND hubs.system_id = 'vn'
            AND hubs.region_id = {{region}}
        ) h ON h.legacy_zone_id = wp.routing_zone_id 
            AND h.hub_id IS NOT NULL
    JOIN (
        SELECT
            short_name
            ,sales_person
            ,legacy_id
            ,name
            
        FROM shipper_prod_gl.shippers
        
        WHERE TRUE
            AND shippers.system_id = 'vn'
        ) s0 ON o.shipper_id = s0.legacy_id
    
    WHERE TRUE 
    
    GROUP BY 1
)
,pre AS (
    SELECT 
        orders_cfg.*
        ,t.service_end_time AS pickup_at
        ,IF(last_seq != first_seq, last_seq - first_seq + 1, IF(last_txn_time IS NOT NULL, 1, 0)) AS no_attempts
        ,CASE 
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) = COALESCE(is0.created_at, '') THEN is0.hub_id
            ELSE ws0.hub_id
        END AS last_scan_hub_id
        ,CASE 
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) = COALESCE(is0.created_at, '') THEN 'global inbound'
            ELSE 'parcel routing'
        END AS last_scan_type
        ,GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) AS last_scan_at
            
    FROM orders_cfg
    LEFT JOIN warehouse_sweeps ws0 ON ws0.id = orders_cfg.latest_warehouse_sweep_id
    LEFT JOIN inbound_scans is0 ON is0.id = orders_cfg.latest_inbound_scan_id
    JOIN (
        SELECT 
            order_id
            ,service_end_time
        FROM transactions force index (order_id, type, status)
        WHERE TRUE
            AND type = 'PP'
            AND status = 'Success'
        ) t ON t.order_id = orders_cfg.order_id

)
SELECT 
    order_id
    ,tracking_id
    ,shipper_id
    ,shipper_group
    ,pre.created_at + interval 7 hour AS created_at
    ,pickup_at + interval 7 hour AS pickup_at
    ,granular_status
    ,delivery_hub_id
    ,delivery_hub
    ,no_attempts
    ,last_comment
    ,h.name AS curr_hub
    ,trim(substring(h.name,1,3)) AS curr_province
    ,h.short_name AS curr_short_name
    ,h.region_name AS curr_region
    ,last_scan_at
    ,DATEDIFF(NOW(), pre.last_scan_at) AS days_from_last_scan
    ,DATEDIFF(NOW(), pre.pickup_at) AS days_from_pickup

FROM pre
JOIN sort_prod_gl.hubs h ON h.hub_id = pre.last_scan_hub_id 
    AND h.system_id = 'vn'
    AND h.sort_hub = 0
    
WHERE TRUE
    AND pre.last_scan_hub_id = pre.delivery_hub_id 
    OR (
        pre.last_scan_hub_id != pre.delivery_hub_id 
        AND trim(substring(h.name,1,3)) = pre.delivery_province)
