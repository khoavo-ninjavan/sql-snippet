/*
Abbreviation
h(i): sort_prod_gl.hubs | sh(i): hub_prod_gl.shipments | o: orders | is(i): inbound_scans | ws(i): warehouse_sweeps | sho(i): hub_prod_gl.shipments | s(i): shipper_prod_gl.shippers | rl(i): route_logs | t(i): transactions
-pull orders-
select id from driver_prod_gl.failure_reasons where system_id = 'vn' and description in ('Khách hẹn đổi ngày giờ giao','Khách hẹn đổi địa điểm giao','Thuê bao không liên lạc được','Đổ chuông nhưng khách không nhấc máy')
TikTok Parcels Reach Hub D-2 | D-1 | D0 and not completed D0
-region
103:South, 89:HN, 101:North, 91:HCM
*/

WITH 
orders_cfg AS (
    SELECT
        o.id AS order_id
        ,o.tracking_id
        ,o.shipper_id
        ,s0.name AS shipper_name
        ,'TikTok Domestic' AS shipper_group
        
        ,o.rts
        ,o.granular_status
        ,o.cod_id
        
        ,CASE 
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, ''), COALESCE(sh0.shipment_completed_at, '')) = COALESCE(is0.created_at, '') THEN is0.hub_id
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, ''), COALESCE(sh0.shipment_completed_at, '')) = COALESCE(ws0.created_at, '') THEN ws0.hub_id
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, ''), COALESCE(sh0.shipment_completed_at, '')) = COALESCE(sh0.shipment_completed_at, '') THEN sh0.curr_hub_id
        END AS last_scan_hub_id
        ,GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, ''), COALESCE(sh0.shipment_completed_at, '')) + interval 7 hour AS last_scan_at
        ,DATE(GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, ''), COALESCE(sh0.shipment_completed_at, '')) + interval 7 hour) AS last_scan_date
        
        ,DATE_FORMAT(shipment_completed_at + interval 7 hour, '%Y-%m-%d %T') AS shipment_completed_at
        ,DATE(shipment_completed_at + interval 7 hour) AS shipment_completed_date
        
    FROM orders o use index (PRIMARY)
    
    JOIN (
        SELECT
            order_id
            ,shipment_events.hub_id AS curr_hub_id
            ,shipment_events.created_at AS shipment_completed_at

        FROM hub_prod_gl.shipment_orders force index (order_country, shipment_id, updated_at) 

        JOIN hub_prod_gl.shipment_events force index (shipment_id, shipment_events_created_at_and_event) ON shipment_events.shipment_id = shipment_orders.shipment_id 

        JOIN sort_prod_gl.hubs h ON h.hub_id = shipment_events.hub_id 
            AND h.system_id = 'vn'
            AND h.sort_hub = 0
            AND h.region_id = {{region}}
            
        WHERE TRUE
            AND shipment_orders.order_country = 'vn' 
            AND shipment_events.hub_system_id = 'vn'
            AND shipment_events.event IN ('SHIPMENT_HUB_INBOUND', 'SHIPMENT_FORCE_COMPLETED')
            AND shipment_events.created_at >= DATE(date_add(now(), interval 7 hour)) - interval 2 day + interval 4 hour /* shipment after 11h N-2 */
            AND shipment_events.created_at < DATE(date_add(now(), interval 7 hour)) + interval 4 hour /* shipment before 11h N0 */

        ) sh0 ON sh0.order_id = o.id

    JOIN (
        SELECT
            short_name
            ,sales_person
            ,legacy_id
            ,shippers.id
            ,name
                    
        FROM shipper_prod_gl.shippers force index (PRIMARY,shipper_system_id_legacy_id_idx)
        LEFT JOIN shipper_prod_gl.marketplace_sellers force index (marketplace_sellers_marketplace_id_external_ref_uindex) ON shippers.id = marketplace_sellers.seller_id
        
        WHERE TRUE
            AND shippers.system_id = 'vn'
            AND marketplace_sellers.marketplace_id = 9090233
        ) s0 ON o.shipper_id = s0.legacy_id
    LEFT JOIN warehouse_sweeps ws0 force index (PRIMARY, created_at) ON ws0.id = o.latest_warehouse_sweep_id
    LEFT JOIN inbound_scans is0 force index (PRIMARY, created_at) ON is0.id = o.latest_inbound_scan_id

    WHERE TRUE
        AND o.rts=0
    )

,delivery_hub AS (
    SELECT distinct
        orders_cfg.*
        ,first_value(z.hub_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_hub_id
        ,first_value(t1.contact) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS buyer_contact
        ,first_value(t1.name) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS buyer_name
        ,first_value(t1.address1) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS address1
        ,first_value(t1.address2) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS address2
        
        ,first_value(transaction_failure_reason.failure_reason_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no ASC) AS first_failure_reason_id
        ,first_value(t1.route_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no ASC) AS first_route_id
        ,first_value(route_logs.driver_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no ASC) AS first_driver_id            
        ,first_value(DATE(t1.service_end_time + interval 7 hour)) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no ASC) AS first_service_end_date
        ,first_value(t1.status) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no ASC) AS first_status
        ,first_value(t1.seq_no) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no ASC) AS first_seq
 
        ,first_value(transaction_failure_reason.failure_reason_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_failure_reason_id
        ,first_value(t1.route_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_route_id
        ,first_value(route_logs.driver_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_driver_id       
        ,first_value(DATE(t1.service_end_time + interval 7 hour)) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_service_end_date
        ,first_value(t1.status) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_status
        ,first_value(t1.seq_no) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS last_seq
        
    FROM orders_cfg
    LEFT JOIN transactions t1 use index (order_id, created_at, type, seq_no) ON orders_cfg.order_id = t1.order_id
    LEFT JOIN route_prod_gl.route_logs force index (primary, created_at) ON  route_logs.legacy_id = t1.route_id
        AND system_id = 'vn'
        AND route_logs.created_at > now() - interval 7 day
    LEFT JOIN waypoints wp force index (primary, created_at) on wp.id = t1.waypoint_id
        AND wp.created_at > now() - interval 7 day
    LEFT JOIN addressing_prod_gl.zones_view z ON z.legacy_zone_id = wp.routing_zone_id AND z.system_id = 'vn'
    LEFT JOIN transaction_failure_reason ON t1.id = transaction_failure_reason.transaction_id
        AND transaction_failure_reason.created_at > now() - interval 7 day
    WHERE TRUE
        AND t1.created_at >= now() - interval 7 day
        AND t1.type = 'DD'
        AND t1.seq_no IN (2,3)

)

SELECT 
    delivery_hub.order_id
    ,tracking_id
    ,order_details.package_content
    ,shipper_id
    ,shipper_name
    ,granular_status
    ,shipment_completed_date
    ,shipment_completed_at
    ,last_scan_at
    
    ,h.hub_id
    ,h.short_name
    ,h.region_name
    ,trim(substring(h.name,1,3)) AS province_code
    
    ,COALESCE(CAST(c.goods_amount AS SIGNED),0) AS cod_value
    ,delivery_hub.buyer_contact
    ,delivery_hub.buyer_name
    ,concat(delivery_hub.address1, ' ', delivery_hub.address2) as buyer_address

    ,first_failure_reason_id
    ,first_route_id
    ,first_driver_id
    ,first_seq
    ,first_service_end_date
    
    ,last_failure_reason_id
    ,last_route_id
    ,last_driver_id
    ,last_seq
    ,last_service_end_date
    
    ,IF(HOUR(shipment_completed_at) < 11, 0, 1) AS kpi_type
    
FROM delivery_hub
JOIN sort_prod_gl.hubs h ON h.hub_id = delivery_hub.last_scan_hub_id 
    AND h.system_id = 'vn'
    AND h.sort_hub = 0
LEFT JOIN order_details ON delivery_hub.order_id = order_details.order_id
LEFT JOIN cods c on delivery_hub.cod_id = c.id

WHERE TRUE
    AND last_scan_hub_id = delivery_hub_id 
    /* Reach hub N-2 N-1: remove completed and 2 fail attempts */
    AND NOT (
        COALESCE(delivery_hub.last_service_end_date, delivery_hub.first_service_end_date) <= DATE(date_add(now(), interval 7 hour)) - interval 1 day
        AND (COALESCE(IF(delivery_hub.last_status != 'Pending', delivery_hub.last_status, NULL), delivery_hub.first_status) = 'Success' 
            OR (delivery_hub.last_seq = 3 AND delivery_hub.last_failure_reason_id IS NOT NULL) /* reach 2 fail attempts */
            )
        )