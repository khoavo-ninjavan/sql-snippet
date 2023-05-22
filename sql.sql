-- explain
SELECT 
    raw.*
    ,h.name AS last_hub
    ,trim(substring(h.name,1,3)) AS last_province
    ,h.short_name
    ,h.region_name AS last_region
    
FROM (
    SELECT
        o.tracking_id
        ,o.created_at
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
        ,CASE 
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) = COALESCE(is0.created_at, '') THEN is0.hub_id
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) = COALESCE(ws0.created_at, '') THEN ws0.hub_id
        END AS last_scan_hub_id
        ,CASE 
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) = COALESCE(is0.created_at, '') THEN 'global inbound'
            WHEN GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) = COALESCE(ws0.created_at, '') THEN 'parcel routing'
        END AS last_scan_type
        ,GREATEST(COALESCE(is0.created_at, ''), COALESCE(ws0.created_at, '')) AS last_scan_at
        ,first_value(h.hub_id) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_hub_id
        ,first_value(h.name) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_hub
        ,first_value(trim(substring(h.name,1,3))) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no DESC) AS delivery_province
        ,first_value(t1.seq_no) OVER (PARTITION BY t1.order_id ORDER BY if(t1.service_end_time is not null, t1.service_end_time, '2001-01-01') DESC) AS last_seq
        ,first_value(t1.seq_no) OVER (PARTITION BY t1.order_id ORDER BY t1.seq_no ASC) AS first_seq
        ,first_value(t1.service_end_time) OVER (PARTITION BY t1.order_id ORDER BY if(t1.service_end_time is not null, t1.service_end_time, '2001-01-01') DESC) AS last_txn_time
        ,first_value(t1.comments) OVER (PARTITION BY t1.order_id ORDER BY if(t1.service_end_time is not null, t1.service_end_time, '2001-01-01') DESC) AS last_comment

    FROM orders o force index (granular_status, primary, created_at)

    JOIN transactions t1 force index (order_id, type, seq_no, waypoint_id) ON o.id = t1.order_id
        AND o.rts = 0
        AND o.granular_status IN ('On Hold','Arrived at Sorting Hub', 'On Vehicle for Delivery', 'Pending Reschedule', 'Arrived at Origin Hub', 'Arrived at Distribution Point', 'Transferred to 3PL')
        AND o.created_at > now() - interval 1 month
        AND t1.type = 'DD'
        AND (t1.seq_no >=4 OR (t1.seq_no =3 AND t1.status != 'Pending'))
    JOIN waypoints wp force index (PRIMARY, waypoints_routing_zone_id_zone_type_index) ON wp.id = t1.waypoint_id 
    JOIN addressing_prod_gl.zones_view z ON z.legacy_zone_id = wp.routing_zone_id 
        AND z.system_id = 'vn'
    JOIN sort_prod_gl.hubs h force index (system_id) ON h.hub_id = z.hub_id 
        AND h.system_id = 'vn'
        AND h.sort_hub = 0
        AND h.region_name IN ({{region}})

    JOIN shipper_prod_gl.shippers s0 ON o.shipper_id = s0.legacy_id
        AND s0.country = 'vn'
        AND s0.legacy_id != 133
    -- last scan
    LEFT JOIN warehouse_sweeps ws0 ON ws0.id = o.latest_warehouse_sweep_id
    LEFT JOIN inbound_scans is0 ON is0.id = o.latest_inbound_scan_id            

    WHERE TRUE 

    ) raw
-- hub info 
JOIN sort_prod_gl.hubs h ON h.hub_id = raw.last_scan_hub_id 
    AND h.system_id = 'vn'
    AND h.sort_hub = 0
