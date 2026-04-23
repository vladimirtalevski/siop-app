-- =============================================================================
-- SIOP DATA QUALITY CHECKS
-- Run against MotherDuck siop_db or Snowflake FLS_PROD_DB.MART_DYN_FO
-- For KU sign-off and data validation
-- =============================================================================


-- 1. ROW COUNTS PER MART
-- Expected: all marts should have rows > 0
-- =============================================================================
SELECT 'ONHAND_INVENTORY'    AS mart, COUNT(*) AS rows, COUNT(DISTINCT DATAAREAID) AS companies FROM MART_DYN_FO.ONHAND_INVENTORY
UNION ALL SELECT 'ITEMS',                COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.ITEMS
UNION ALL SELECT 'PURCHASE_ORDERS',      COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.PURCHASE_ORDERS
UNION ALL SELECT 'PURCHASE_ORDER_LINE',  COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.PURCHASE_ORDER_LINE
UNION ALL SELECT 'SALES_ORDERS',         COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.SALES_ORDERS
UNION ALL SELECT 'ORDER_LINES',          COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.ORDER_LINES
UNION ALL SELECT 'INVENTORY_TRANSACTIONS',COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.INVENTORY_TRANSACTIONS
UNION ALL SELECT 'DEMAND_FORECAST',      COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.DEMAND_FORECAST
UNION ALL SELECT 'BOM_VERSIONS',         COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.BOM_VERSIONS
UNION ALL SELECT 'BOM_LINES',            COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.BOM_LINES
UNION ALL SELECT 'NET_REQUIREMENTS',     COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.NET_REQUIREMENTS
ORDER BY mart;


-- 2. COMPANY COVERAGE — ON-HAND INVENTORY
-- Expected: ZA3, ZA4, US2, DK1, GH1 all present
-- =============================================================================
SELECT
    UPPER(DATAAREAID)       AS company,
    COUNT(DISTINCT ITEMID)  AS distinct_items,
    SUM(AVAILPHYSICAL)      AS total_available_qty,
    SUM(PHYSICALINVENT)     AS total_on_hand_qty,
    SUM(RESERVPHYSICAL)     AS total_reserved_qty,
    SUM(ONORDER)            AS total_on_order_qty
FROM MART_DYN_FO.ONHAND_INVENTORY
GROUP BY UPPER(DATAAREAID)
ORDER BY company;


-- 3. ON-HAND VALUE BY COMPANY
-- Expected: all companies should have a value > 0
-- =============================================================================
SELECT
    UPPER(O.DATAAREAID)                                         AS company,
    COUNT(DISTINCT O.ITEMID)                                    AS items_with_stock,
    ROUND(SUM(O.PHYSICALINVENT), 0)                             AS total_qty,
    COUNT(CASE WHEN P.PRICE IS NOT NULL THEN 1 END)             AS priced_lines,
    COUNT(CASE WHEN P.PRICE IS NULL THEN 1 END)                 AS unpriced_lines,
    ROUND(100.0 * COUNT(CASE WHEN P.PRICE IS NOT NULL THEN 1 END)
        / NULLIF(COUNT(*), 0), 1)                               AS pct_priced,
    ROUND(SUM(O.PHYSICALINVENT * COALESCE(P.PRICE, 0)), 0)      AS total_value_local
FROM MART_DYN_FO.ONHAND_INVENTORY O
LEFT JOIN (
    SELECT ITEMID, DATAAREAID, PRICE
    FROM MART_DYN_FO.PRICE
    WHERE PRICETYPE = '0'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ITEMID, DATAAREAID ORDER BY ACTIVATIONDATE DESC) = 1
) P ON O.ITEMID = P.ITEMID AND O.DATAAREAID = P.DATAAREAID
WHERE O.PHYSICALINVENT > 0
GROUP BY UPPER(O.DATAAREAID)
ORDER BY company;


-- 4. COMPLETENESS — ITEMS WITH PRICE
-- Target: > 90% of items should have a standard cost price
-- Red flag: < 70%
-- =============================================================================
SELECT
    UPPER(I.DATAAREAID)                                             AS company,
    COUNT(DISTINCT I.ITEMID)                                        AS total_items,
    COUNT(DISTINCT P.ITEMID)                                        AS items_with_price,
    COUNT(DISTINCT I.ITEMID) - COUNT(DISTINCT P.ITEMID)             AS missing_price,
    ROUND(100.0 * COUNT(DISTINCT P.ITEMID)
        / NULLIF(COUNT(DISTINCT I.ITEMID), 0), 1)                   AS pct_priced
FROM MART_DYN_FO.ITEMS I
LEFT JOIN (
    SELECT DISTINCT ITEMID, DATAAREAID
    FROM MART_DYN_FO.PRICE
    WHERE PRICETYPE = '0'
) P ON I.ITEMID = P.ITEMID AND I.DATAAREAID = P.DATAAREAID
GROUP BY UPPER(I.DATAAREAID)
ORDER BY company;


-- 5. COMPLETENESS — ITEMS WITH PRIMARY SUPPLIER
-- Target: > 90% of active items should have a primary vendor
-- =============================================================================
SELECT
    UPPER(DATAAREAID)                                               AS company,
    COUNT(*)                                                        AS total_items,
    COUNT(PRIMARYVENDORID)                                          AS items_with_supplier,
    COUNT(*) - COUNT(PRIMARYVENDORID)                               AS missing_supplier,
    ROUND(100.0 * COUNT(PRIMARYVENDORID) / NULLIF(COUNT(*), 0), 1) AS pct_with_supplier
FROM MART_DYN_FO.ITEMS
GROUP BY UPPER(DATAAREAID)
ORDER BY company;


-- 6. COMPLETENESS — OPEN PO LINES WITH CONFIRMED SHIP DATE
-- Target: > 80% of open PO lines should have a confirmed ship date
-- =============================================================================
SELECT
    UPPER(DATAAREAID)   AS company,
    COUNT(*)            AS total_open_po_lines,
    COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL
               AND CAST(CONFIRMEDSHIPDATE AS VARCHAR) NOT LIKE '1900%'
               THEN 1 END)                                          AS with_confirmed_date,
    COUNT(*) - COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL
               AND CAST(CONFIRMEDSHIPDATE AS VARCHAR) NOT LIKE '1900%'
               THEN 1 END)                                          AS missing_date,
    ROUND(100.0 * COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL
               AND CAST(CONFIRMEDSHIPDATE AS VARCHAR) NOT LIKE '1900%'
               THEN 1 END) / NULLIF(COUNT(*), 0), 1)               AS pct_complete
FROM MART_DYN_FO.PURCHASE_ORDER_LINE
WHERE PURCHSTATUS = 1
GROUP BY UPPER(DATAAREAID)
ORDER BY company;


-- 7. CONSISTENCY — ON-HAND ITEMS NOT IN ITEM MASTER (ORPHANS)
-- Expected: 0 orphan items — if > 0, data integrity issue in D365
-- =============================================================================
SELECT
    UPPER(O.DATAAREAID)         AS company,
    COUNT(DISTINCT O.ITEMID)    AS onhand_items,
    COUNT(DISTINCT I.ITEMID)    AS matched_in_master,
    COUNT(DISTINCT O.ITEMID)
        - COUNT(DISTINCT I.ITEMID) AS orphan_items
FROM MART_DYN_FO.ONHAND_INVENTORY O
LEFT JOIN MART_DYN_FO.ITEMS I
    ON O.ITEMID = I.ITEMID AND O.DATAAREAID = I.DATAAREAID
WHERE O.PHYSICALINVENT > 0
GROUP BY UPPER(O.DATAAREAID)
ORDER BY company;


-- 8. CONSISTENCY — PO LINE QTY ANOMALIES
-- Flag: remaining qty > ordered qty (data entry error)
-- =============================================================================
SELECT
    UPPER(DATAAREAID)   AS company,
    PURCHID             AS po_number,
    LINENUMBER          AS line,
    ITEMID              AS item_id,
    PURCHQTY            AS ordered_qty,
    REMAINPURCHPHYSICAL AS remaining_qty,
    REMAINPURCHPHYSICAL - PURCHQTY AS excess_qty
FROM MART_DYN_FO.PURCHASE_ORDER_LINE
WHERE REMAINPURCHPHYSICAL > PURCHQTY
    AND PURCHSTATUS = 1
ORDER BY excess_qty DESC
LIMIT 100;


-- 9. DATA FRESHNESS — DATE RANGE PER KEY MART
-- Expected: newest record within last 7 days
-- =============================================================================
SELECT 'PURCHASE_ORDERS' AS mart,
    MIN(CAST(CREATEDDATETIME AS DATE))  AS oldest_record,
    MAX(CAST(CREATEDDATETIME AS DATE))  AS newest_record,
    DATEDIFF('day', MAX(CAST(CREATEDDATETIME AS DATE)), CURRENT_DATE) AS days_since_latest
FROM MART_DYN_FO.PURCHASE_ORDERS
UNION ALL
SELECT 'SALES_ORDERS',
    MIN(CAST(CREATEDDATETIME AS DATE)),
    MAX(CAST(CREATEDDATETIME AS DATE)),
    DATEDIFF('day', MAX(CAST(CREATEDDATETIME AS DATE)), CURRENT_DATE)
FROM MART_DYN_FO.SALES_ORDERS
UNION ALL
SELECT 'INVENTORY_TRANSACTIONS',
    MIN(DATEPHYSICAL),
    MAX(DATEPHYSICAL),
    DATEDIFF('day', MAX(DATEPHYSICAL), CURRENT_DATE)
FROM MART_DYN_FO.INVENTORY_TRANSACTIONS
WHERE DATEPHYSICAL > '1900-01-02'
ORDER BY mart;


-- 10. COVERAGE — ITEMS WITH ON-HAND BUT NO DEMAND FORECAST
-- Flag: MTS items with stock but zero forecast = potential overstock risk
-- =============================================================================
SELECT
    UPPER(O.DATAAREAID)         AS company,
    COUNT(DISTINCT O.ITEMID)    AS items_with_stock,
    COUNT(DISTINCT F.ITEMID)    AS items_with_forecast,
    COUNT(DISTINCT O.ITEMID)
        - COUNT(DISTINCT F.ITEMID) AS items_no_forecast
FROM MART_DYN_FO.ONHAND_INVENTORY O
LEFT JOIN MART_DYN_FO.DEMAND_FORECAST F
    ON O.ITEMID = F.ITEMID AND O.DATAAREAID = F.DATAAREAID AND F.ACTIVE = 1
WHERE O.AVAILPHYSICAL > 0
GROUP BY UPPER(O.DATAAREAID)
ORDER BY company;
