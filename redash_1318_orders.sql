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

FROM order_tags ot force index (order_tags_order_id_tag_id_index)
JOIN orders o use index (primary, shipper_id, granular_status) ON ot.order_id = o.id
    AND tag_id = 125
    AND o.granular_status NOT IN ('Cancelled')
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
    
LEFT JOIN order_details ON o.id = order_details.order_id
LEFT JOIN cods ON o.cod_id = cods.id

WHERE TRUE