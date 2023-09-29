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
    SELECT DISTINCT
        o.id AS order_id

    FROM orders o use index (primary, shipper_id, granular_status)
    
    JOIN ticketing_prod_gl.tickets t ON t.order_id = o.id
    
    JOIN (
        SELECT
            short_name
            ,sales_person
            ,legacy_id
            ,name
            
        FROM shipper_prod_gl.shippers
        LEFT JOIN shipper_prod_gl.marketplace_sellers ON shippers.id = marketplace_sellers.seller_id
    
        WHERE TRUE
            AND shippers.system_id = 'vn'
            AND (marketplace_sellers.marketplace_id = 9090233 OR shippers.legacy_id = 824968)
        ) s0 ON o.shipper_id = s0.legacy_id
    
    WHERE TRUE
        AND o.granular_status NOT IN ('Cancelled')
        AND NOT (o.granular_status IN ('Completed','Returned to Sender') AND o.updated_at < now() - interval 3 day) /* filter 1 */    
        AND t.country = 'vn'
        AND t.deleted_at is NULL
        AND t.type_id = 4 /* type: PARCEL EXCEPTION */
        AND t.subtype_id in (5,30) /* sub_type: CUSTOMER REJECTED / MAXIMUM ATTEMPTS (DELIVERY) */
        AND t.status_id not in (3,13) /* not in status: RESOLVED/CANCELLED */

)


,txn_cfg AS (
    SELECT 
        order_id
        ,id AS transaction_id
        ,seq_no
        ,type
        ,status
        ,route_id
        ,waypoint_id
        ,service_end_time
        ,name
        ,contact
        ,address1
        ,address2

    FROM transactions
    WHERE order_id IN (SELECT order_id FROM root)    
    
)

,route_cfg AS (
    SELECT
        legacy_id AS route_id
        ,driver_id
        ,hub_id
    FROM route_prod_gl.route_logs
    WHERE TRUE
        AND system_id = 'vn'
        AND legacy_id IN (SELECT DISTINCT route_id FROM  txn_cfg)
    
)

,fail_cfg AS (
    SELECT 
        transaction_id
        ,failure_reason_id
    FROM transaction_failure_reason
    WHERE transaction_id IN (SELECT DISTINCT transaction_id FROM txn_cfg)
)

SELECT
    txn_cfg.*
    ,route_cfg.driver_id
    ,route_cfg.hub_id AS delivery_hub_id
    ,fail_cfg.failure_reason_id
    
FROM txn_cfg
LEFT JOIN route_cfg ON route_cfg.route_id = txn_cfg.route_id
LEFT JOIN fail_cfg ON fail_cfg.transaction_id = txn_cfg.transaction_id