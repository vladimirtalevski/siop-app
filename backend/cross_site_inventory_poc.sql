/* ══════════════════════════════════════════════════════════════════════════
   CROSS-SITE INVENTORY AVAILABILITY — POC
   Includes: DKK exchange rate conversion + sample drill-down
   Run in: FLS_PROD_DB context

   ⚠ VERIFY before running:
     - HFM rate value column: using xc.VALUE — confirm this is correct
     - D365 currency column:  SO.CURRENCYCODE
     - Oracle currency column: OOHA.TRANSACTIONAL_CURR_CODE
     - Epicor currency column: SO_V.SO_CURRENCY
   ══════════════════════════════════════════════════════════════════════════ */

WITH

/* ── D365: Eagle-style load date ────────────────────────────────────────── */
ranked_load_line AS (
    SELECT
        LD.INVENTTRANSID,
        LD.DATAAREAID,
        CASE
            WHEN LT.LOADSTATUS IN (0, 1, 2, 3)
                THEN CAST(LT.LOADSCHEDSHIPUTCDATETIME AS DATE)
            WHEN LT.LOADSTATUS IN (5, 6, 9)
                THEN CASE
                         WHEN LT.LOADSCHEDSHIPUTCDATETIME < LT.LOADSHIPCONFIRMUTCDATETIME
                          AND LT.LOADSCHEDSHIPUTCDATETIME <> '1900-01-01'
                         THEN CAST(LT.LOADSCHEDSHIPUTCDATETIME AS DATE)
                         ELSE CAST(LT.LOADSHIPCONFIRMUTCDATETIME AS DATE)
                     END
            ELSE CAST('9999-01-01' AS DATE)
        END AS LOADDATESUPPORT
    FROM FLS_PROD_DB.MART_DYN_FO.LOAD_DETAILS LD
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.LOADS LT
        ON LD.LOADID     = LT.LOADID
       AND LD.DATAAREAID = LT.DATAAREAID
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LD.INVENTTRANSID, LD.DATAAREAID
        ORDER BY LD.MODIFIEDDATETIME DESC
    ) = 1
),

/* ── HFM master for Oracle site mapping ─────────────────────────────────── */
mhs_deduped AS (
    SELECT COMPANY, LOCATION, COMPANY_NAME, HFM_LOCATION
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_HFM_MASTER_SALEORDER
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COMPANY ORDER BY COMPANY) = 1
),

/* ── Exchange rates → DKK (last month end rate, actual) ─────────────────── */
fx_rates AS (
    SELECT
        xc.custom1                          AS from_currency,
        xc.RATE                             AS rate_to_dkk
    FROM FLS_PROD_DB.EDW.HFM_EXCHANGE_RATE_STG xc
    WHERE xc.custom2   = 'DKK'
      AND xc.year      = YEAR(ADD_MONTHS(SYSDATE(), -1))
      AND xc.period    = TO_VARCHAR(ADD_MONTHS(SYSDATE(), -1), 'Mon')
      AND xc.account   = 'ENDRATE'
      AND xc.scenario  = 'ACT'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY xc.custom1 ORDER BY xc.custom1) = 1
),

/* ── On-hand inventory across all ERPs ──────────────────────────────────── */
inventory AS (
    SELECT
        UPPER(TRIM(inv.PARTNUM))        AS item_id,
        inv.COMPANY                     AS inv_company,
        inv."Entity"                    AS entity,
        inv.plant                       AS warehouse,
        inv."Country"                   AS country,
        inv."Region"                    AS region,
        inv.onhandqty                   AS onhand_qty,
        inv.PRODUCTFAMILY               AS product_family,
        CASE
            WHEN inv.ID LIKE '%DYNAMIC%' OR inv.ID LIKE '%D365%' THEN 'D365'
            WHEN inv.ID = 'ST01P'                                THEN 'Epicor'
            WHEN inv.ID = 'SDBP11'                               THEN 'Microsiga'
            ELSE                                                      'Oracle/Other'
        END                             AS stock_erp
    FROM EDW.INVENTORY_VW_MYINVENTORY inv
    WHERE inv.onhandqty > 0
),

/* ── Open demand: D365 ──────────────────────────────────────────────────── */
d365_demand AS (
    SELECT
        UPPER(TRIM(OL.ITEMID))                          AS item_id,
        COALESCE(ID.INVENTSITEID, OL.DATAAREAID)        AS demand_site,
        OL.DATAAREAID                                   AS dataareaid,
        CAST(OL.QTYORDERED AS FLOAT)                    AS ordered_qty,
        CAST(OL.ShippingDateConfirmed AS DATE)          AS confirmed_ship_date,
        CAST(OL.CREATEDDATETIME AS DATE)                AS created_date,
        ROUND(OL.SALESPRICE * OL.QTYORDERED, 2)        AS line_value,
        CAST(OL.SALESID AS VARCHAR)                     AS order_number,
        CAST(OL.LINENUM AS VARCHAR)                     AS line_num,
        'D365'                                          AS demand_erp,
        COALESCE(SO.CURRENCYCODE, 'USD')                AS currency,
        CASE
            WHEN CAST(OL.ShippingDateConfirmed AS DATE) < CURRENT_DATE()
            THEN 'Past Due' ELSE 'Open'
        END                                             AS demand_status
    FROM FLS_PROD_DB.MART_DYN_FO.ORDER_LINES OL
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SALES_ORDERS SO
        ON SO.SALESID    = OL.SALESID
       AND SO.DATAAREAID = OL.DATAAREAID
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.INVENTORY_DIMENSIONS ID
        ON ID.INVENTDIMID = OL.INVENTDIMID
    LEFT JOIN ranked_load_line RLL
        ON RLL.INVENTTRANSID = OL.INVENTTRANSID
       AND RLL.DATAAREAID    = OL.DATAAREAID
    WHERE OL.CREATEDDATETIME >= '2024-01-01'
      AND OL.SALESSTATUS IN (0, 1)
      AND (
          RLL.LOADDATESUPPORT IS NULL
          OR RLL.LOADDATESUPPORT = CAST('9999-01-01' AS DATE)
          OR RLL.LOADDATESUPPORT = CAST('1900-01-01' AS DATE)
      )
      AND OL.ITEMID IS NOT NULL
      AND CAST(OL.ShippingDateConfirmed AS DATE) > '1901-01-01'
      AND CAST(OL.ShippingDateConfirmed AS DATE) < '9998-01-01'
),

/* ── Open demand: Oracle CEN01 ──────────────────────────────────────────── */
oracle_demand AS (
    SELECT
        UPPER(TRIM(
            REGEXP_REPLACE(OOLA.ORDERED_ITEM, '-[A-Z]{2,4}[0-9]{0,2}$', '')
        ))                                                      AS item_id,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)          AS demand_site,
        COALESCE(MHS.COMPANY_NAME, OOD.ORGANIZATION_CODE)      AS dataareaid,
        CAST(CASE WHEN OOLA.LINE_CATEGORY_CODE = 'RETURN'
                  THEN OOLA.ORDERED_QUANTITY * -1
                  ELSE OOLA.ORDERED_QUANTITY
             END AS FLOAT)                                      AS ordered_qty,
        CAST(OOLA.PROMISE_DATE AS DATE)                        AS confirmed_ship_date,
        CAST(OOHA.BOOKED_DATE AS DATE)                         AS created_date,
        ROUND(OOLA.UNIT_SELLING_PRICE * OOLA.ORDERED_QUANTITY, 2) AS line_value,
        CAST(OOHA.ORDER_NUMBER AS VARCHAR)                     AS order_number,
        CAST(OOLA.LINE_NUMBER AS VARCHAR)                      AS line_num,
        'Oracle CEN01'                                         AS demand_erp,
        COALESCE(OOHA.TRANSACTIONAL_CURR_CODE, 'USD')          AS currency,
        CASE
            WHEN CAST(OOLA.PROMISE_DATE AS DATE) < CURRENT_DATE()
            THEN 'Past Due' ELSE 'Open'
        END                                                    AS demand_status
    FROM FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_LINES_ALL OOLA
    JOIN FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_HEADERS_ALL OOHA
        ON OOLA.HEADER_ID = OOHA.HEADER_ID
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD
        ON OOLA.SHIP_FROM_ORG_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped MHS
        ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    WHERE OOHA.BOOKED_DATE   >= '2024-01-01'
      AND OOHA.CONTEXT        = 'ORDER'
      AND OOHA.ORG_ID        IN ('18102','3241','10682','91','19480','8962')
      AND OOHA.ORDER_TYPE_ID NOT IN ('1187','1541','1405','1245','1308','1592','1201','1121')
      AND OOLA.LINE_TYPE_ID  NOT IN ('1184','1182','1181','1006')
      AND OOLA.FLOW_STATUS_CODE IN ('AWAITING_SHIPPING','BOOKED','AWAITING_RECEIPT','AWAITING_RETURN')
      AND OOLA.ACTUAL_SHIPMENT_DATE IS NULL
      AND OOLA.ORDERED_ITEM IS NOT NULL
),

/* ── Open demand: Epicor ────────────────────────────────────────────────── */
epicor_demand AS (
    SELECT
        UPPER(TRIM(SO_V.SO_ITEM_NUMBER))                AS item_id,
        SO_V.SO_PLANT                                   AS demand_site,
        SO_V.SO_COMPANY                                 AS dataareaid,
        CAST(SO_V.SO_ORDER_QTY AS FLOAT)               AS ordered_qty,
        CAST(SO_V.SO_PROMISE_DATE AS DATE)             AS confirmed_ship_date,
        CAST(SO_V.SALEORDER_ORDERDATE AS DATE)         AS created_date,
        ROUND(SO_V.SO_UNIT_PRICE * SO_V.SO_ORDER_QTY, 2) AS line_value,
        CAST(SO_V.SALEORDER_NUMBER AS VARCHAR)          AS order_number,
        CAST(SO_V.SALEORDER_LINE AS VARCHAR)            AS line_num,
        'Epicor'                                        AS demand_erp,
        COALESCE(SO_V.SO_CURRENCY, 'USD')               AS currency,
        CASE
            WHEN CAST(SO_V.SO_PROMISE_DATE AS DATE) < CURRENT_DATE()
            THEN 'Past Due' ELSE 'Open'
        END                                             AS demand_status
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.VW_EPICOR_SALE_ORDER SO_V
    WHERE SO_V.SALEORDER_ORDERDATE >= '2024-01-01'
      AND SO_V.POFULFILLMENT_DATE IS NULL
      AND SO_V.SO_LINE_STATUS NOT IN ('Complete', 'Cancelled', 'Closed')
      AND SO_V.SO_ITEM_NUMBER IS NOT NULL
),

/* ── Union all demand ───────────────────────────────────────────────────── */
all_demand AS (
    SELECT * FROM d365_demand
    UNION ALL
    SELECT * FROM oracle_demand
    UNION ALL
    SELECT * FROM epicor_demand
),

/* ── Cross-site match with DKK conversion ───────────────────────────────── */
cross_site_match AS (
    SELECT
        d.item_id,
        d.demand_site,
        d.demand_erp,
        d.order_number,
        d.line_num,
        d.ordered_qty,
        d.confirmed_ship_date,
        d.created_date,
        d.line_value,
        d.currency,
        d.demand_status,
        DATEDIFF(day, d.confirmed_ship_date, CURRENT_DATE())    AS days_overdue,

        -- DKK conversion
        COALESCE(fx.rate_to_dkk,
            CASE WHEN d.currency = 'DKK' THEN 1.0 ELSE NULL END
        )                                                       AS fx_rate,
        ROUND(d.line_value * COALESCE(fx.rate_to_dkk,
            CASE WHEN d.currency = 'DKK' THEN 1.0 ELSE NULL END, 1.0
        ), 2)                                                   AS line_value_dkk,

        i.inv_company                                           AS stock_site,
        i.entity                                                AS stock_entity,
        i.warehouse,
        i.country                                               AS stock_country,
        i.region                                                AS stock_region,
        i.stock_erp,
        i.onhand_qty,
        i.product_family,

        CASE
            WHEN i.onhand_qty >= d.ordered_qty THEN 'Full Cover'
            WHEN i.onhand_qty >  0             THEN 'Partial Cover'
            ELSE                                    'No Cover'
        END                                                     AS coverage_type,
        ROUND(i.onhand_qty - d.ordered_qty, 2)                 AS qty_surplus

    FROM all_demand d
    JOIN inventory i
        ON i.item_id = d.item_id
       AND UPPER(TRIM(i.inv_company)) <> UPPER(TRIM(d.demand_site))
    LEFT JOIN fx_rates fx
        ON fx.from_currency = d.currency
    WHERE d.ordered_qty > 0
)


/* ══════════════════════════════════════════════════════════════════════════
   QUERY 1 — DIAGNOSTIC SUMMARY WITH DKK
   Run this first to validate numbers
   ══════════════════════════════════════════════════════════════════════════ */
SELECT
    demand_erp,
    demand_status,
    coverage_type,
    COUNT(*)                                    AS row_count,
    COUNT(DISTINCT item_id)                     AS unique_items,
    COUNT(DISTINCT demand_site)                 AS demand_sites,
    COUNT(DISTINCT stock_site)                  AS stock_sites,
    COUNT(CASE WHEN fx_rate IS NULL THEN 1 END) AS missing_fx_rate,
    ROUND(SUM(line_value_dkk) / 1e6, 1)        AS total_value_dkk_M
FROM cross_site_match
WHERE coverage_type IN ('Full Cover', 'Partial Cover')
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;


/* ══════════════════════════════════════════════════════════════════════════
   QUERY 2 — SAMPLE DRILL-DOWN: Top 10 Past Due + Full Cover
   Run this separately to manually verify the matches make business sense
   Shows: what item, where it's needed, where the stock is
   ══════════════════════════════════════════════════════════════════════════ */
/*
SELECT
    item_id,
    product_family,
    currency,
    fx_rate,

    -- Demand (where it's needed)
    demand_erp,
    demand_site,
    order_number,
    line_num,
    ordered_qty,
    confirmed_ship_date,
    days_overdue,
    ROUND(line_value_dkk / 1000, 1)            AS value_dkk_K,

    -- Stock (where it sits)
    stock_site,
    stock_entity,
    stock_country,
    stock_erp,
    warehouse,
    onhand_qty,
    qty_surplus

FROM cross_site_match
WHERE coverage_type  = 'Full Cover'
  AND demand_status  = 'Past Due'
  AND days_overdue   > 0
ORDER BY
    line_value_dkk DESC NULLS LAST
LIMIT 10;
*/
