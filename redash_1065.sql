/*
Abbreviation
h(i): sort_prod_gl.hubs | sh(i): hub_prod_gl.shipments | o: orders | is(i): inbound_scans | ws(i): warehouse_sweeps | sho(i): hub_prod_gl.shipments | s(i): shipper_prod_gl.shippers | rl(i): route_logs | t(i): transactions
-pull orders-
select id from driver_prod_gl.failure_reasons where system_id = 'vn' and description in ('Khách hẹn đổi ngày giờ giao','Khách hẹn đổi địa điểm giao','Thuê bao không liên lạc được','Đổ chuông nhưng khách không nhấc máy')
-region
103:South, 89:HN, 101:North, 91:HCM
*/
WITH 
root AS (
    SELECT 
        o.id AS order_id
        ,o.tracking_id
        ,o.created_at
        ,o.latest_warehouse_sweep_id
        ,o.latest_inbound_scan_id
        ,o.from_contact
        ,CASE
            WHEN substr(trim(s0.short_name),1,6) = 'Shopee' THEN 'Shopee'
            WHEN (LEFT(s0.sales_person, 4) IN ('FHN-', 'FTS-', 'FNO-', 'FSO-', 'FBD-')) OR (LEFT(s0.sales_person, 4) = 'FHC-' AND s0.name NOT REGEXP 'RTL|FRC') THEN 'FS'
            WHEN substr(trim(s0.short_name),1,6) ='Lazada' THEN 'Lazada'
            WHEN s0.legacy_id = 824968 THEN 'TikTok XBorder'
            WHEN substr(trim(s0.short_name),1,9) ='TOKGISTIC' THEN 'TikTok Domestic'
            ELSE 'Others'
        END AS shipper_group
        ,o.shipper_id
        ,s0.name AS shipper_name
        ,o.granular_status
        ,o.cod_id
        ,rts
        ,o.type AS order_type

    FROM order_tags ot force index (order_tags_order_id_tag_id_index)
    JOIN orders o use index (primary, shipper_id, granular_status, updated_at) ON ot.order_id = o.id
        AND tag_id = 123
        AND NOT (o.granular_status IN ('Completed','Returned to Sender') AND o.updated_at < now() - interval 3 day) /* filter 1 */
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

),
orders_cfg AS (
    SELECT
        distinct
        root.*
        ,first_value(wp.latitude) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_latitude
        ,first_value(wp.longitude) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_longitude
        ,first_value(h.hub_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_hub_id
        ,first_value(h.name) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_hub
        ,first_value(trim(substring(h.name,1,3))) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_province
        ,first_value(t1.seq_no) OVER (PARTITION BY t1.order_id ORDER BY IF(t1.status='Fail' AND t1.service_end_time < curdate() - interval 7 hour, t1.seq_no, 0) DESC) AS last_seq
        ,first_value(t1.contact) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_contact
        ,first_value(t1.route_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_route
        ,first_value(route_logs.driver_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_driver
        ,first_value(t1.name) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_contact_name
        ,first_value(concat(t1.address1," - ", t1.address2)) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_contact_address
        ,first_value(transaction_failure_reason.failure_reason_id) OVER (PARTITION BY t1.order_id ORDER BY IF(t1.status='Fail' AND t1.service_end_time < curdate() - interval 7 hour, t1.seq_no, 0) DESC) AS last_failure_reason_id
        ,first_value(transaction_failure_reason.failure_reason_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS latest_failure_reason_id
        ,first_value(t1.service_end_time) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_attempt_at

    FROM root

    JOIN transactions t1 force index (order_id, service_end_time, waypoint_id, route_id, status) ON root.order_id = t1.order_id
        AND t1.service_end_time > now() - interval 10 day
        AND t1.type = 'DD'
        AND t1.status != 'Pending'

    LEFT JOIN route_prod_gl.route_logs force index (primary, created_at) ON  route_logs.legacy_id = t1.route_id
        AND system_id = 'vn'
        AND route_logs.created_at > now() - interval 10 day

    JOIN sort_prod_gl.hubs h use index (system_id, region_id) on h.hub_id = route_logs.hub_id
        AND h.system_id = 'vn'
        AND h.region_id = {{region}}
        AND h.hub_id !=1

    LEFT JOIN transaction_failure_reason ON t1.id = transaction_failure_reason.transaction_id
        AND transaction_failure_reason.created_at > now() - interval 10 day
    LEFT JOIN waypoints wp force index (PRIMARY, created_at, waypoints_routing_zone_id_zone_type_index) ON wp.id = t1.waypoint_id
        AND wp.created_at > now() - interval 10 day
        
    WHERE TRUE
    AND root.shipper_group = 'TikTok Domestic'
    AND root.order_type = 'Normal'
)
,pre AS (
    SELECT 
        orders_cfg.*
        ,DATE(orders_cfg.last_attempt_at + interval 7 hour) AS last_attempt_date
        ,SUBSTR(orders_cfg.last_contact_name, -5) AS leg
        ,t.service_end_time AS pickup_at
        ,last_seq - 1 AS no_attempts
        ,CASE 
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) = COALESCE(is0.created_at, '') THEN is0.hub_id
            ELSE ws0.hub_id
        END AS last_scan_hub_id
        ,CASE 
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) = COALESCE(is0.created_at, '') THEN 'global inbound'
            ELSE 'parcel routing'
        END AS last_scan_type
        ,GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) AS last_scan_at
        ,contact AS shipper_contact
            
    FROM orders_cfg
    LEFT JOIN warehouse_sweeps ws0 ON ws0.id = orders_cfg.latest_warehouse_sweep_id
    LEFT JOIN inbound_scans is0 ON is0.id = orders_cfg.latest_inbound_scan_id
    JOIN (
        SELECT 
            order_id
            ,contact
            ,service_end_time
            ,seq_no
            
        FROM transactions force index (order_id, type, status)
        WHERE TRUE
            AND type = 'PP'
            AND status = 'Success'
        ) t ON t.order_id = orders_cfg.order_id
    WHERE TRUE
)

SELECT 
    pre.order_id
    ,tracking_id
    ,order_details.package_content
    ,COALESCE(CAST(cods.goods_amount AS SIGNED),0) AS cod_value
    ,rts
    ,shipper_id
    ,shipper_name
    ,shipper_group
    ,shipper_contact
    ,pre.created_at + interval 7 hour AS created_at
    ,pickup_at + interval 7 hour AS pickup_at
    ,granular_status
    ,delivery_longitude
    ,delivery_latitude
    ,delivery_hub_id
    ,delivery_hub
    ,no_attempts
    ,last_attempt_at + interval 7 hour AS last_attempt_at
    ,last_failure_reason_id
    ,latest_failure_reason_id
    ,last_route
    ,last_driver
    ,last_contact
    ,last_contact_name
    ,last_contact_address
    ,h.name AS curr_hub
    ,trim(substring(h.name,1,3)) AS curr_province
    ,h.short_name AS curr_short_name
    ,h.region_name AS curr_region
    ,last_scan_at + interval 7 hour AS last_scan_at

FROM pre
LEFT JOIN order_details ON pre.order_id = order_details.order_id
LEFT JOIN cods ON pre.cod_id = cods.id
JOIN sort_prod_gl.hubs h ON h.hub_id = pre.last_scan_hub_id 
    AND h.system_id = 'vn'
    AND h.sort_hub = 0
    
WHERE TRUE
    AND NOT pre.leg = '(RTS)'
    AND NOT (rts = 1 AND pre.leg != '(RTS)' AND pre.last_attempt_date < DATE(now() + interval 7 hour))
    AND NOT (pre.granular_status IN ('Completed','Returned to Sender') AND pre.last_attempt_date < DATE(now() + interval 7 hour)) /* filter 2 */
