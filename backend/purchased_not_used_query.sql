/* ══════════════════════════════════════════════════════════════════════════
   PURCHASED BUT NOT USED — D365 + Oracle CEN01 + Epicor
   Items received via PO in the last 6 months where, for the same site:
     - No SO demand exists that is EITHER currently open OR was created
       within the same 6-month window (catches items used on orders that
       were already shipped/closed)
     - No MIN / MAX / ROP planning established
     - Not a DDMRP or Min/Max planning group
     - Not raw-material fabrication inputs (-WLD1 suffix, D365 only)
   Stock value converted to DKK via last-month-end ACT ENDRATE.
   ══════════════════════════════════════════════════════════════════════════ */

WITH

fx_rates AS (
    -- Hardcoded spot rates to DKK as of 05-May-2026 (source: exchangerates.org.uk)
    -- Rate = how many DKK per 1 unit of foreign currency
    SELECT from_currency, rate_to_dkk
    FROM (VALUES
        ('DKK', 1.0000),
        ('USD', 6.3880),
        ('EUR', 7.4730),
        ('GBP', 8.6560),
        ('CHF', 8.1610),
        ('AUD', 4.5920),
        ('CAD', 4.6900),
        ('NZD', 3.7640),
        ('SEK', 0.6900),
        ('NOK', 0.6910),
        ('JPY', 0.0440),
        ('CNY', 0.9350),
        ('HKD', 0.8150),   -- 1/1.227
        ('SGD', 4.7670),   -- 1/0.2097
        ('KRW', 0.004354), -- 1/229.70
        ('INR', 0.0672),   -- 1/14.884
        ('IDR', 0.000367), -- 1/2722.9
        ('MYR', 1.6129),   -- 1/0.620
        ('THB', 0.1962),   -- 1/5.098
        ('PHP', 0.1040),   -- 1/9.615
        ('PKR', 0.02291),  -- 1/43.644
        ('BRL', 1.2953),   -- 1/0.772
        ('MXN', 0.3676),   -- 1/2.720
        ('AED', 1.7391),   -- 1/0.575
        ('SAR', 1.7036),   -- 1/0.587
        ('QAR', 1.7483),   -- 1/0.572
        ('KWD', 20.833),   -- 1/0.048
        ('OMR', 16.667),   -- 1/0.060
        ('BHD', 16.949),   -- 1/0.059
        ('JOD', 9.0090),   -- 1/0.111
        ('ZAR', 0.3833),   -- 1/2.609
        ('EGP', 0.1191),   -- 1/8.395
        ('NGN', 0.004669), -- 1/214.18
        ('KES', 0.04946),  -- 1/20.219
        ('MAD', 0.6906),   -- 1/1.448
        ('DZD', 0.04823),  -- 1/20.733
        ('TND', 2.1882),   -- 1/0.457
        ('PLN', 1.7606),   -- 1/0.568
        ('CZK', 0.3064),   -- 1/3.264
        ('HUF', 0.02066),  -- 1/48.40
        ('TRY', 0.1413)    -- 1/7.079
    ) AS t(from_currency, rate_to_dkk)
),

mhs_deduped AS (
    SELECT COMPANY, LOCATION, COMPANY_NAME, HFM_LOCATION
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_HFM_MASTER_SALEORDER
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COMPANY ORDER BY COMPANY) = 1
),

/* ══════════════════════════════════════════════════════════════════════════
   RECEIPTS — items received in the last 6 months
   ══════════════════════════════════════════════════════════════════════════ */

d365_receipts AS (
    SELECT
        UPPER(TRIM(POL.ITEMID))                          AS item_id,
        POL.DATAAREAID                                   AS company,
        MAX(CAST(VPRL.DELIVERYDATE AS DATE))             AS last_receipt_date,
        MIN(CAST(POH.CREATEDDATETIME AS DATE))           AS po_created_date,
        SUM(VPRL.QTY)                                    AS total_received_qty,
        MAX(POL.PURCHPRICE)                              AS last_unit_price,
        MAX(POH.PURCHID)                                 AS sample_po,
        COUNT(DISTINCT POH.PURCHID)                      AS po_count,
        MAX(COALESCE(POH.CURRENCYCODE, 'USD'))           AS currency,
        'D365'                                           AS erp_source
    FROM FLS_PROD_DB.MART_DYN_FO.VENDOR_PRODUCT_RECEIPT_LINES VPRL
    JOIN FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDER_LINE POL
        ON VPRL.INVENTTRANSID = POL.INVENTTRANSID
       AND VPRL.DATAAREAID    = POL.DATAAREAID
    JOIN FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDERS POH
        ON POL.PURCHID    = POH.PURCHID
       AND POL.DATAAREAID = POH.DATAAREAID
    WHERE CAST(VPRL.DELIVERYDATE AS DATE) >= DATEADD('month', -6, CURRENT_DATE())
      AND VPRL.QTY   > 0
      AND POL.ITEMID NOT LIKE '%-WLD1'
    GROUP BY UPPER(TRIM(POL.ITEMID)), POL.DATAAREAID
),

oracle_receipts AS (
    SELECT
        UPPER(TRIM(MSIB.SEGMENT1))                               AS item_id,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)           AS company,
        MAX(CAST(RT.TRANSACTION_DATE AS DATE))                   AS last_receipt_date,
        MIN(CAST(PHA.CREATION_DATE AS DATE))                     AS po_created_date,
        SUM(RT.QUANTITY)                                         AS total_received_qty,
        MAX(PLA.UNIT_PRICE)                                      AS last_unit_price,
        MAX(PHA.SEGMENT1)                                        AS sample_po,
        COUNT(DISTINCT PHA.PO_HEADER_ID)                         AS po_count,
        MAX(COALESCE(PHA.CURRENCY_CODE, 'USD'))                  AS currency,
        'Oracle CEN01'                                           AS erp_source
    FROM FLS_PROD_DB.RAW_CEN01.RCV_TRANSACTIONS RT
    JOIN FLS_PROD_DB.RAW_CEN01.PO_PO_LINES_ALL PLA
        ON RT.PO_LINE_ID = PLA.PO_LINE_ID
    JOIN FLS_PROD_DB.RAW_CEN01.PO_PO_HEADERS_ALL PHA
        ON PLA.PO_HEADER_ID = PHA.PO_HEADER_ID
    JOIN FLS_PROD_DB.RAW_CEN01.APPS_MTL_SYSTEM_ITEMS_B MSIB
        ON PLA.ITEM_ID        = MSIB.INVENTORY_ITEM_ID
       AND RT.ORGANIZATION_ID = MSIB.ORGANIZATION_ID
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD
        ON RT.ORGANIZATION_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped MHS
        ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    WHERE RT.TRANSACTION_TYPE = 'RECEIVE'
      AND CAST(RT.TRANSACTION_DATE AS DATE) >= DATEADD('month', -6, CURRENT_DATE())
      AND RT.QUANTITY > 0
      AND TO_VARCHAR(RT.QUANTITY)   <> '5555555555555555555'
      AND TO_VARCHAR(PLA.UNIT_PRICE) <> '5555555555555555555'
      AND PHA.ORG_ID IN ('18102','3241','10682','91','19480','8962')
    GROUP BY UPPER(TRIM(MSIB.SEGMENT1)), COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)
),

epicor_receipts AS (
    SELECT
        UPPER(TRIM(RI.PARTNUM))                          AS item_id,
        RI.COMPANY                                       AS company,
        MAX(CAST(RH.RECEIPTDATE AS DATE))                AS last_receipt_date,
        MIN(CAST(PH.ORDERDATE AS DATE))                  AS po_created_date,
        SUM(RI.OURQTY)                                   AS total_received_qty,
        MAX(RI.OURUNITCOST)                              AS last_unit_price,
        MAX(CAST(RI.PONUM AS VARCHAR))                   AS sample_po,
        COUNT(DISTINCT RI.PONUM)                         AS po_count,
        MAX(COALESCE(PH.CURRENCYCODE, 'USD'))            AS currency,
        'Epicor'                                         AS erp_source
    FROM FLS_PROD_DB.RAW_EPICOR01.ERP_RCVHEAD RH
    JOIN FLS_PROD_DB.RAW_EPICOR01.ERP_RCVDTL RI
        ON  RH.COMPANY   = RI.COMPANY
        AND RH.VENDORNUM = RI.VENDORNUM
        AND RH.PURPOINT  = RI.PURPOINT
        AND RH.PACKSLIP  = RI.PACKSLIP
    LEFT JOIN FLS_PROD_DB.RAW_EPICOR01.ERP_POHEADER PH
        ON RI.PONUM = PH.PONUM AND RI.COMPANY = PH.COMPANY
    WHERE CAST(RH.RECEIPTDATE AS DATE) >= DATEADD('month', -6, CURRENT_DATE())
      AND RI.OURQTY > 0
      AND RI.PARTNUM IS NOT NULL
    GROUP BY UPPER(TRIM(RI.PARTNUM)), RI.COMPANY
),

all_receipts AS (
    SELECT * FROM d365_receipts
    UNION ALL
    SELECT * FROM oracle_receipts
    UNION ALL
    SELECT * FROM epicor_receipts
),

/* ══════════════════════════════════════════════════════════════════════════
   DEMAND — excludes items that are:
     (a) currently open on a sales order, OR
     (b) appeared on any SO created within the same 6-month window
         (catches orders already shipped/closed so we don't surface items
          that were legitimately consumed against a customer order)
   ══════════════════════════════════════════════════════════════════════════ */

d365_recent_demand AS (
    SELECT DISTINCT UPPER(TRIM(ITEMID)) AS item_id, DATAAREAID AS company
    FROM FLS_PROD_DB.MART_DYN_FO.ORDER_LINES
    WHERE ITEMID IS NOT NULL
      AND (
          SALESSTATUS IN (0, 1)
          OR CAST(CREATEDDATETIME AS DATE) >= DATEADD('month', -6, CURRENT_DATE())
      )
),

oracle_recent_demand AS (
    SELECT DISTINCT
        UPPER(TRIM(REGEXP_REPLACE(OOLA.ORDERED_ITEM, '-[A-Z]{2,4}[0-9]{0,2}$', ''))) AS item_id,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)                                  AS company
    FROM FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_LINES_ALL OOLA
    JOIN FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_HEADERS_ALL OOHA
        ON OOLA.HEADER_ID = OOHA.HEADER_ID
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD
        ON OOLA.SHIP_FROM_ORG_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped MHS
        ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    WHERE OOLA.ORDERED_ITEM IS NOT NULL
      AND OOHA.ORG_ID IN ('18102','3241','10682','91','19480','8962')
      AND (
          (OOLA.FLOW_STATUS_CODE IN ('AWAITING_SHIPPING','BOOKED','AWAITING_RECEIPT','AWAITING_RETURN')
           AND OOLA.ACTUAL_SHIPMENT_DATE IS NULL)
          OR CAST(OOHA.BOOKED_DATE AS DATE) >= DATEADD('month', -6, CURRENT_DATE())
      )
),

epicor_recent_demand AS (
    SELECT DISTINCT
        UPPER(TRIM(SO_V.SO_ITEM_NUMBER)) AS item_id,
        SO_V.SO_COMPANY                  AS company
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.VW_EPICOR_SALE_ORDER SO_V
    WHERE SO_V.SO_ITEM_NUMBER IS NOT NULL
      AND (
          SO_V.SO_LINE_STATUS NOT IN ('Complete', 'Cancelled', 'Closed')
          OR CAST(SO_V.SALEORDER_ORDERDATE AS DATE) >= DATEADD('month', -6, CURRENT_DATE())
      )
),

all_recent_demand AS (
    SELECT * FROM d365_recent_demand
    UNION ALL
    SELECT * FROM oracle_recent_demand
    UNION ALL
    SELECT * FROM epicor_recent_demand
),

/* ══════════════════════════════════════════════════════════════════════════
   ITEM COVERAGE — identifies HOW each item is planned in D365.
   INCLUDE (ordered per requirement — should have had a demand to consume it):
     FA_LL, Group, REQ, SA_sale, BTO AB, NULL (no group assigned)
   EXCLUDE (planned/replenished stock — intentionally held on the shelf):
     Min./Max., DDMRP variants, and anything else not in the list above.
   We pick the row with a populated REQGROUPID first to avoid reading a
   NULL coverage row when a real group exists for the same item.
   ══════════════════════════════════════════════════════════════════════════ */

d365_item_cov AS (
    SELECT
        UPPER(TRIM(IC.ITEMID))          AS item_id,
        IC.DATAAREAID                   AS company,
        COALESCE(IC.MININVENTONHAND, 0) AS min_inv,
        COALESCE(IC.MAXINVENTONHAND, 0) AS max_inv,
        COALESCE(IC.REORDERPOINT,   0)  AS rop,
        IC.REQGROUPID
    FROM FLS_PROD_DB.MART_DYN_FO.ITEM_COVERAGE IC
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY IC.ITEMID, IC.DATAAREAID
        ORDER BY IC.EFFECTIVE_FROM DESC NULLS LAST
    ) = 1
),

oracle_item_cov AS (
    SELECT
        UPPER(TRIM(MSIB.SEGMENT1))                        AS item_id,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)     AS company,
        -- sentinel 5555555555555555555 = Oracle "unlimited" → treat as 0 (not set)
        CASE WHEN MSIB.MIN_MINMAX_QUANTITY = 5555555555555555555
             THEN 0 ELSE COALESCE(MSIB.MIN_MINMAX_QUANTITY, 0) END AS min_inv,
        CASE WHEN MSIB.MAX_MINMAX_QUANTITY = 5555555555555555555
             THEN 0 ELSE COALESCE(MSIB.MAX_MINMAX_QUANTITY, 0) END AS max_inv,
        0                                                 AS rop,
        -- encode planning status into REQGROUPID so the WHERE filter works:
        -- NULL = Not Planned (passes), 'PLANNED' = actively planned (excluded)
        CASE WHEN MSIB.MRP_PLANNING_CODE = '6' OR MSIB.MRP_PLANNING_CODE IS NULL
             THEN NULL ELSE 'PLANNED' END                 AS REQGROUPID
    FROM FLS_PROD_DB.RAW_CEN01.APPS_MTL_SYSTEM_ITEMS_B MSIB
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD
        ON MSIB.ORGANIZATION_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped MHS
        ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    WHERE MSIB.ENABLED_FLAG = 'Y'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY MSIB.SEGMENT1, COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)
        ORDER BY MSIB.LAST_UPDATE_DATE DESC, MSIB.LAST_UPDATED_BY DESC
    ) = 1
),

epicor_item_cov AS (
    -- Plant-level min/max first; fall back to warehouse-level (same pattern as SpeedUp view)
    SELECT
        UPPER(TRIM(PP.PARTNUM))                                            AS item_id,
        PP.COMPANY                                                         AS company,
        COALESCE(NULLIF(PP.MINIMUMQTY, 0), NULLIF(PW.MINIMUMQTY, 0), 0)  AS min_inv,
        COALESCE(NULLIF(PP.MAXIMUMQTY, 0), NULLIF(PW.MAXIMUMQTY, 0), 0)  AS max_inv,
        0                                                                  AS rop,
        NULL                                                               AS REQGROUPID
    FROM FLS_PROD_DB.RAW_EPICOR01.ERP_PARTPLANT PP
    LEFT JOIN FLS_PROD_DB.RAW_EPICOR01.ERP_PARTWHSE PW
        ON  PW.COMPANY       = PP.COMPANY
        AND PW.PARTNUM       = PP.PARTNUM
        AND UPPER(PW.WAREHOUSECODE) = UPPER(PP.PLANT)
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PP.COMPANY, PP.PARTNUM
        ORDER BY COALESCE(PP.SYSREVID, '0') DESC
    ) = 1
),

all_item_cov AS (
    SELECT * FROM d365_item_cov
    UNION ALL
    SELECT * FROM oracle_item_cov
    UNION ALL
    SELECT * FROM epicor_item_cov
),

/* ══════════════════════════════════════════════════════════════════════════
   ON-HAND
   D365: use MART_DYN_FO.ONHAND_INVENTORY directly (AVAILPHYSICAL, matched
         by ITEMID + DATAAREAID) — the unified EDW view uses different
         company identifiers for D365 so joins return 0 stock.
   Oracle / Epicor: unified EDW view (PARTNUM + COMPANY), excluding D365 rows.
   ══════════════════════════════════════════════════════════════════════════ */

d365_on_hand AS (
    SELECT
        UPPER(TRIM(OHI.ITEMID)) AS item_id,
        OHI.DATAAREAID          AS company,
        SUM(OHI.AVAILPHYSICAL)  AS avail_qty
    FROM FLS_PROD_DB.MART_DYN_FO.ONHAND_INVENTORY OHI
    GROUP BY UPPER(TRIM(OHI.ITEMID)), OHI.DATAAREAID
),

oracle_epicor_on_hand AS (
    SELECT
        UPPER(TRIM(inv.PARTNUM)) AS item_id,
        inv.COMPANY              AS company,
        SUM(inv.onhandqty)       AS avail_qty
    FROM EDW.INVENTORY_VW_MYINVENTORY inv
    WHERE TO_VARCHAR(inv.onhandqty) <> '5555555555555555555'
      AND inv.ID NOT LIKE '%DYNAMIC%'
      AND inv.ID NOT LIKE '%D365%'
    GROUP BY UPPER(TRIM(inv.PARTNUM)), inv.COMPANY
),

on_hand AS (
    SELECT * FROM d365_on_hand
    UNION ALL
    SELECT * FROM oracle_epicor_on_hand
),

/* ══════════════════════════════════════════════════════════════════════════
   STOCK LOCATIONS — site + warehouse pairs where available stock > 0
   D365 : ONHAND_INVENTORY (INVENTSITEID > INVENTLOCATIONID)
   Oracle/Epicor : EDW unified view (plant column)
   ══════════════════════════════════════════════════════════════════════════ */

d365_stock_locs AS (
    SELECT
        item_id,
        company,
        LISTAGG(loc, ' | ') WITHIN GROUP (ORDER BY loc) AS stock_locations
    FROM (
        SELECT DISTINCT
            UPPER(TRIM(OHI.ITEMID))                              AS item_id,
            OHI.DATAAREAID                                       AS company,
            COALESCE(OHI.INVENTSITEID, OHI.DATAAREAID)
                || CASE WHEN OHI.INVENTLOCATIONID IS NOT NULL
                        THEN ' > ' || OHI.INVENTLOCATIONID
                        ELSE '' END                              AS loc
        FROM FLS_PROD_DB.MART_DYN_FO.ONHAND_INVENTORY OHI
        WHERE OHI.AVAILPHYSICAL > 0
    ) t
    GROUP BY item_id, company
),

oracle_epicor_stock_locs AS (
    SELECT
        item_id,
        company,
        LISTAGG(loc, ' | ') WITHIN GROUP (ORDER BY loc) AS stock_locations
    FROM (
        SELECT DISTINCT
            UPPER(TRIM(inv.PARTNUM)) AS item_id,
            inv.COMPANY              AS company,
            inv.plant                AS loc
        FROM EDW.INVENTORY_VW_MYINVENTORY inv
        WHERE TO_VARCHAR(inv.onhandqty) <> '5555555555555555555'
          AND inv.onhandqty > 0
          AND inv.ID NOT LIKE '%DYNAMIC%'
          AND inv.ID NOT LIKE '%D365%'
          AND inv.plant IS NOT NULL
    ) t
    GROUP BY item_id, company
),

all_stock_locs AS (
    SELECT * FROM d365_stock_locs
    UNION ALL
    SELECT * FROM oracle_epicor_stock_locs
),

/* ══════════════════════════════════════════════════════════════════════════
   ITEM DESCRIPTIONS
   ══════════════════════════════════════════════════════════════════════════ */

d365_desc AS (
    SELECT
        UPPER(TRIM(I.ITEMID)) AS item_id,
        I.DATAAREAID          AS company,
        PT.DESCRIPTION        AS item_description
    FROM FLS_PROD_DB.MART_DYN_FO.ITEMS I
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.PRODUCT_TRANSLATIONS PT
        ON PT.PRODUCT = I.PRODUCT AND PT.LANGUAGEID = 'en-US'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY I.ITEMID, I.DATAAREAID
        ORDER BY CASE WHEN PT.DESCRIPTION IS NOT NULL THEN 0 ELSE 1 END
    ) = 1
),

oracle_desc AS (
    SELECT
        UPPER(TRIM(MSIB.SEGMENT1))                        AS item_id,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)     AS company,
        MSIB.DESCRIPTION                                   AS item_description
    FROM FLS_PROD_DB.RAW_CEN01.APPS_MTL_SYSTEM_ITEMS_B MSIB
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD
        ON MSIB.ORGANIZATION_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped MHS
        ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    WHERE MSIB.ENABLED_FLAG = 'Y'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY MSIB.SEGMENT1, COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)
        ORDER BY MSIB.LAST_UPDATE_DATE DESC
    ) = 1
),

epicor_desc AS (
    SELECT
        UPPER(TRIM(P.PARTNUM)) AS item_id,
        P.COMPANY              AS company,
        P.PARTDESCRIPTION      AS item_description
    FROM FLS_PROD_DB.RAW_EPICOR01.ERP_PART P
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY P.COMPANY, P.PARTNUM
        ORDER BY COALESCE(P.SYSREVID, '0') DESC
    ) = 1
),

all_desc AS (
    SELECT * FROM d365_desc
    UNION ALL
    SELECT * FROM oracle_desc
    UNION ALL
    SELECT * FROM epicor_desc
),

/* ══════════════════════════════════════════════════════════════════════════
   LAST SALES ORDER — most recent SO ever for each item+company
   Gives context on how long ago this item was last demanded
   ══════════════════════════════════════════════════════════════════════════ */

d365_last_so AS (
    SELECT
        UPPER(TRIM(ITEMID))              AS item_id,
        DATAAREAID                       AS company,
        CAST(CREATEDDATETIME AS DATE)    AS last_so_date,
        CAST(SALESID AS VARCHAR)         AS last_so_number
    FROM FLS_PROD_DB.MART_DYN_FO.ORDER_LINES
    WHERE ITEMID IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ITEMID, DATAAREAID
        ORDER BY CAST(CREATEDDATETIME AS DATE) DESC
    ) = 1
),

oracle_last_so AS (
    SELECT
        UPPER(TRIM(REGEXP_REPLACE(OOLA.ORDERED_ITEM, '-[A-Z]{2,4}[0-9]{0,2}$', ''))) AS item_id,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)                                  AS company,
        CAST(OOHA.BOOKED_DATE AS DATE)                                                 AS last_so_date,
        CAST(OOHA.ORDER_NUMBER AS VARCHAR)                                             AS last_so_number
    FROM FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_LINES_ALL OOLA
    JOIN FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_HEADERS_ALL OOHA
        ON OOLA.HEADER_ID = OOHA.HEADER_ID
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD
        ON OOLA.SHIP_FROM_ORG_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped MHS
        ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    WHERE OOLA.ORDERED_ITEM IS NOT NULL
      AND OOHA.ORG_ID IN ('18102','3241','10682','91','19480','8962')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY OOLA.ORDERED_ITEM, COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)
        ORDER BY CAST(OOHA.BOOKED_DATE AS DATE) DESC
    ) = 1
),

epicor_last_so AS (
    SELECT
        UPPER(TRIM(SO_V.SO_ITEM_NUMBER))        AS item_id,
        SO_V.SO_COMPANY                          AS company,
        CAST(SO_V.SALEORDER_ORDERDATE AS DATE)  AS last_so_date,
        CAST(SO_V.SALEORDER_NUMBER AS VARCHAR)  AS last_so_number
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.VW_EPICOR_SALE_ORDER SO_V
    WHERE SO_V.SO_ITEM_NUMBER IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SO_V.SO_ITEM_NUMBER, SO_V.SO_COMPANY
        ORDER BY CAST(SO_V.SALEORDER_ORDERDATE AS DATE) DESC
    ) = 1
),

all_last_so AS (
    SELECT * FROM d365_last_so
    UNION ALL
    SELECT * FROM oracle_last_so
    UNION ALL
    SELECT * FROM epicor_last_so
)

/* ══════════════════════════════════════════════════════════════════════════
   FINAL OUTPUT
   ══════════════════════════════════════════════════════════════════════════ */
SELECT
    RR.company,
    RR.item_id,
    DESCR.item_description,
    RR.erp_source,
    RR.sample_po                                                      AS purchase_order,
    CAST(RR.po_created_date  AS VARCHAR)                              AS po_created_date,
    CAST(RR.last_receipt_date AS VARCHAR)                             AS last_receipt_date,
    DATEDIFF('day', RR.last_receipt_date, CURRENT_DATE())             AS days_since_receipt,
    RR.total_received_qty                                             AS received_qty,
    COALESCE(OH.avail_qty, 0)                                         AS current_stock,
    ROUND(RR.last_unit_price, 2)                                      AS unit_price,
    RR.currency,
    ROUND(COALESCE(OH.avail_qty, 0) * RR.last_unit_price, 2)         AS stock_value_local,
    ROUND(
        COALESCE(OH.avail_qty, 0) * RR.last_unit_price
        * COALESCE(
            fx.rate_to_dkk,
            CASE WHEN RR.currency = 'DKK' THEN 1.0 ELSE NULL END,
            1.0
          ),
        2
    )                                                                 AS stock_value_dkk,
    ROUND(
        COALESCE(OH.avail_qty, 0) * RR.last_unit_price
        * COALESCE(
            fx.rate_to_dkk,
            CASE WHEN RR.currency = 'DKK' THEN 1.0 ELSE NULL END,
            1.0
          ),
        2
    )                                                                 AS stock_value_dkk_today,
    COALESCE(IC.REQGROUPID, 'No Coverage Group')                      AS req_group,
    RR.po_count,
    SLOC.stock_locations,
    CAST(LSO.last_so_date AS VARCHAR)                                 AS last_so_date,
    LSO.last_so_number                                                AS last_so_number

FROM all_receipts RR
LEFT JOIN all_recent_demand OD
    ON OD.item_id = RR.item_id AND OD.company = RR.company
LEFT JOIN all_item_cov IC
    ON IC.item_id = RR.item_id AND IC.company = RR.company
LEFT JOIN on_hand OH
    ON OH.item_id = RR.item_id AND OH.company = RR.company
LEFT JOIN all_desc DESCR
    ON DESCR.item_id = RR.item_id AND DESCR.company = RR.company
LEFT JOIN fx_rates fx
    ON fx.from_currency = RR.currency
LEFT JOIN all_stock_locs SLOC
    ON SLOC.item_id = RR.item_id AND SLOC.company = RR.company
LEFT JOIN all_last_so LSO
    ON LSO.item_id = RR.item_id AND LSO.company = RR.company

WHERE OD.item_id IS NULL                              -- no SO demand (open or recent)
  AND COALESCE(IC.min_inv, 0) = 0                     -- no min-stock level set
  AND COALESCE(IC.max_inv, 0) = 0                     -- no max-stock level set
  AND COALESCE(IC.rop,     0) = 0                     -- no reorder point set
  -- Include ONLY D365 "Requirement" coverage groups — items ordered per demand,
  -- not planned/replenished stock. NULL = no group assigned (also per-requirement).
  AND (
      IC.REQGROUPID IS NULL
      OR IC.REQGROUPID IN ('FA_LL', 'Group', 'REQ', 'SA_sale', 'BTO AB')
  )

ORDER BY stock_value_dkk DESC NULLS LAST
LIMIT 5000
