SELECT 
    o.id AS order_id
    ,o.tracking_id
    ,o.created_at

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
    ,COALESCE(CAST(cods.goods_amount AS SIGNED),0) AS cod_value
    ,rts
    ,o.type AS order_type
    
    ,order_details.package_content

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

LEFT JOIN order_details ON o.id = order_details.order_id
LEFT JOIN cods ON o.cod_id = cods.id

WHERE TRUE
    AND o.granular_status NOT IN ('Cancelled')
    AND NOT (o.granular_status IN ('Completed','Returned to Sender') AND o.updated_at < now() - interval 3 day) /* filter 1 */    
    AND t.country = 'vn'
    AND t.deleted_at is NULL
    AND t.type_id = 4 /* type: PARCEL EXCEPTION */
    AND t.subtype_id in (5,30) /* sub_type: CUSTOMER REJECTED / MAXIMUM ATTEMPTS (DELIVERY) */
    AND t.status_id not in (3,13) /* not in status: RESOLVED/CANCELLED */