"""
Export Snowflake data to JSON for Vercel deployment.
Run this script ONCE locally to refresh all dashboard data.

Usage:
    cd siop-app/backend
    python export_vercel.py

A Microsoft SSO browser popup will appear — complete the login.
All 7 JSON files will be written to frontend/public/data/
Then run: cd ../frontend && npx vercel --prod
"""
import json, os, sys, time
from decimal import Decimal
from datetime import datetime, date
from db import get_connection

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "frontend", "public", "data")
os.makedirs(OUTPUT, exist_ok=True)

def to_json(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, (datetime, date)): return obj.isoformat()
    if isinstance(obj, bytearray): return obj.hex()
    return str(obj)

def run(name, sql, desc=""):
    t0 = time.time()
    print(f"  → {desc or name}... ", end="", flush=True)
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close()
    path = os.path.join(OUTPUT, f"{name}.json")
    with open(path, "w") as f:
        json.dump(rows, f, default=to_json, indent=2)
    print(f"{len(rows)} rows ({time.time()-t0:.0f}s)")
    return rows

print("=" * 55)
print("  SIOP Manager — Vercel Data Export")
print("=" * 55)
print()
print("Connecting to Snowflake (SSO popup will open)...")
t_start = time.time()
conn = get_connection()
print(f"Connected in {time.time()-t_start:.0f}s\n")
print("Exporting data...\n")

# 1. Inventory summary
run("inventory_summary", """
    SELECT
        DATAAREAID                       AS company,
        INVENTSITEID                     AS site,
        COUNT(DISTINCT ITEMID)           AS distinct_items,
        SUM(ABS(AVAILPHYSICAL))          AS total_avail_physical,
        SUM(ABS(PHYSICALINVENT))         AS total_physical,
        SUM(ABS(ONORDER))                AS total_on_order,
        SUM(ABS(RESERVPHYSICAL))         AS total_reserved
    FROM ONHAND_INVENTORY
    WHERE AVAILPHYSICAL != 0 OR PHYSICALINVENT != 0
    GROUP BY DATAAREAID, INVENTSITEID
    ORDER BY company, site
""", "Inventory summary")

# 2. Inventory detail — enriched with item master, planning, FX to DKK
run("inventory", """
WITH
RankedPrices AS (
    SELECT
        IDIM.INVENTSITEID,
        IIPS.ITEMID,
        IIPS.PRICE,
        IIPS.DATAAREAID,
        IIPS.UNITID,
        ROW_NUMBER() OVER (
            PARTITION BY IIPS.ITEMID, IIPS.DATAAREAID, IDIM.INVENTSITEID, IIPS.PRICETYPE
            ORDER BY IIPS.ACTIVATIONDATE DESC
        ) AS rn
    FROM FLS_PROD_DB.MART_DYN_FO.PRICE IIPS
    INNER JOIN FLS_PROD_DB.MART_DYN_FO.INVENTORY_DIMENSIONS IDIM
        ON IIPS.InventDimId = IDIM.InventDimId
    WHERE IIPS.PRICETYPE = '0'
      AND IIPS.ACTIVATIONDATE <= CURRENT_DATE()
),
UniqueCurrency AS (
    SELECT ITEMID, DATAAREAID, CURRENCYCODE,
           ROW_NUMBER() OVER (PARTITION BY ITEMID, DATAAREAID ORDER BY CURRENCYCODE) AS cur_rn
    FROM FLS_PROD_DB.MART_DYN_FO.INVENTORY_TRANSACTIONS
    WHERE CURRENCYCODE IS NOT NULL
),
AggregatedInventory AS (
    SELECT
        OI.ITEMID,
        OI.DATAAREAID,
        OI.WMSLOCATIONID,
        OI.INVENTSITEID,
        ID.INVENTLOCATIONID,
        SUM(OI.POSTEDQTY + OI.RECEIVED - OI.DEDUCTED + OI.REGISTERED - OI.PICKED) AS physical_inventory,
        SUM(OI.RESERVPHYSICAL)  AS physical_reserved,
        SUM(OI.AVAILPHYSICAL)   AS available_physical,
        SUM(OI.ORDERED)         AS on_order,
        SUM(OI.ARRIVED)         AS arrived,
        SUM(OI.RESERVORDERED)   AS ordered_reserved,
        SUM(OI.AVAILORDERED)    AS total_available
    FROM FLS_PROD_DB.MART_DYN_FO.ONHAND_INVENTORY OI
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.INVENTORY_DIMENSIONS ID
        ON OI.INVENTDIMID = ID.INVENTDIMID AND OI.DATAAREAID = ID.DATAAREAID
    GROUP BY OI.ITEMID, OI.DATAAREAID, OI.WMSLOCATIONID, OI.INVENTSITEID, ID.INVENTLOCATIONID
),
CTE_CostPrice AS (
    SELECT ITEMID, DATAAREAID,
           CASE WHEN (SUM(PostedQty) - ABS(SUM(Deducted)) + SUM(Received)) = 0 THEN 0
                ELSE ABS(SUM(PostedValue) + SUM(PhysicalValue))
                     / NULLIF((SUM(PostedQty) - ABS(SUM(Deducted)) + SUM(Received)), 0)
           END AS cost_price
    FROM FLS_PROD_DB.MART_DYN_FO.ONHAND_INVENTORY
    GROUP BY ITEMID, DATAAREAID
),
CTE_Coverage AS (
    SELECT ITC.ITEMID, ITC.DATAAREAID, ITC.MININVENTONHAND, ITC.MAXINVENTONHAND,
           ITC.REQGROUPID, ITC.REORDERPOINT, IDM.INVENTLOCATIONID
    FROM FLS_PROD_DB.MART_DYN_FO.ITEM_COVERAGE ITC
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.INVENTORY_DIMENSIONS IDM
        ON ITC.COVINVENTDIMID = IDM.INVENTDIMID
),
CTE_FX AS (
    SELECT
        mfr.CURRENCY            AS CurrencyCode,
        mfr."CONVERSATION RATE" AS FxRate,
        ROW_NUMBER() OVER (
            PARTITION BY mfr.CURRENCY
            ORDER BY mfr.YEAR DESC, mfr."MONTH NO" DESC
        ) AS rn
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_MASTER_FX_RATES mfr
    WHERE mfr.YEAR * 100 + mfr."MONTH NO"
          <= YEAR(CURRENT_DATE()) * 100 + MONTH(CURRENT_DATE())
)
SELECT
    I.ITEMID                                                        AS item_id,
    PT.DESCRIPTION                                                  AS part_description,
    I.DATAAREAID                                                    AS company,
    OI.INVENTSITEID                                                 AS site,
    CONCAT(I.DATAAREAID, '-', OI.INVENTSITEID)                      AS entity,
    OI.INVENTLOCATIONID                                             AS warehouse_id,
    UPPER(OI.WMSLOCATIONID)                                         AS warehouse_location,
    DCS.BUSINESSUNITVALUE                                           AS business_unit,
    DCS.OFFERINGTYPEVALUE                                           AS order_typology,
    EA.PRODUCTTYPOLOGY                                              AS product_typology,
    EA.DRAWINGNUMBER                                                AS drawing_number,
    EA.MATERIAL                                                     AS material,
    IG.ITEMGROUPID                                                  AS class_id,
    ICU.CURRENCYCODE                                                AS local_currency,
    -- Quantities
    OI.physical_inventory                                           AS on_hand_qty,
    OI.physical_reserved                                            AS reserved_qty,
    OI.available_physical                                           AS available_qty,
    OI.on_order                                                     AS on_order_qty,
    OI.total_available                                              AS total_available_qty,
    -- Pricing & value
    IPC.PRICE                                                       AS unit_price,
    IPC.UNITID                                                      AS uom,
    CP.cost_price                                                   AS cost_price,
    CASE WHEN IPC.PRICE IS NULL THEN 'No Price' ELSE 'OK' END       AS has_item_price,
    OI.physical_inventory * COALESCE(IPC.PRICE, 0)                 AS onhand_value_local,
    OI.physical_inventory * COALESCE(IPC.PRICE, 0)
        * COALESCE(fx.FxRate, 0)                                    AS onhand_value_dkk,
    COALESCE(fx.FxRate, 0)                                          AS fx_rate_to_dkk,
    -- Warehouse
    CASE W.inventlocationtype
        WHEN 0   THEN 'Default'         WHEN 1  THEN 'Quarantine'
        WHEN 2   THEN 'Transit'         WHEN 10 THEN 'Goods in transit'
        WHEN 11  THEN 'Under'           WHEN 101 THEN 'Goods shipped'
        ELSE 'Unknown'
    END                                                             AS warehouse_type,
    W.NAME                                                          AS warehouse_name,
    -- Planning
    IPOS.LEADTIME                                                   AS lead_time,
    IPOS.LOWESTQTY                                                  AS min_order_qty,
    IPOS.HIGHESTQTY                                                 AS max_order_qty,
    IGS.REORDERPOINT                                                AS reorder_point,
    IGS.REQGROUPID                                                  AS item_type,
    IGS.MININVENTONHAND                                             AS min_qty,
    IGS.MAXINVENTONHAND                                             AS max_qty,
    CASE WHEN IGS.MAXINVENTONHAND > 0
         THEN IGS.MININVENTONHAND / IGS.MAXINVENTONHAND ELSE 0
    END                                                             AS safety_stock_ratio,
    CASE
        WHEN IGS.REQGROUPID IN ('DDMRP LL F','DDMRP LL H','DDMRP ML F',
                                'DDMRP ML H','DDMRP SL F','DDMRP SL H') THEN 'MTS'
        WHEN IGS.REQGROUPID IN ('FA','SA_sale','SAnoSale','BTO AB','BTO C') THEN 'MTO'
        ELSE 'UNMAPPED'
    END                                                             AS mts_mto,
    CASE STS.DEFAULTORDERTYPE
        WHEN 0 THEN 'Purchase' WHEN 1 THEN 'Production'
        WHEN 2 THEN 'Transfer' WHEN 3 THEN 'Kanban'
        ELSE NULL
    END                                                             AS source_type,
    ICG.REQGROUPID                                                  AS coverage_group,
    INVGRP.DESCRIPTION                                              AS buyer_group,
    IMG.ITEMGROUPID                                                 AS costing_method

FROM FLS_PROD_DB.MART_DYN_FO.ITEMS I
INNER JOIN AggregatedInventory OI
    ON I.ITEMID = OI.ITEMID AND I.DATAAREAID = OI.DATAAREAID
LEFT JOIN RankedPrices IPC
    ON IPC.itemid = I.itemid AND IPC.dataareaid = I.dataareaid
    AND IPC.INVENTSITEID = OI.INVENTSITEID AND IPC.rn = 1
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.WAREHOUSES W
    ON OI.INVENTLOCATIONID = W.INVENTLOCATIONID AND OI.DATAAREAID = W.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.PRODUCT_TRANSLATIONS PT
    ON I.PRODUCT = PT.PRODUCT AND PT.LANGUAGEID = 'en-US'
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DIMENSION_CODE_SET DCS
    ON I.DEFAULTDIMENSION = DCS.RECID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEM_PURCHASE_ORDER_SETTINGS IPOS
    ON I.ITEMID = IPOS.ITEMID AND I.DATAAREAID = IPOS.DATAAREAID AND IPOS.OVERRIDE = '1'
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.RELATIONSHIP_BETWEEN_ITEMS_AND_ITEM_GROUPS IG
    ON I.ITEMID = IG.ITEMID AND I.DATAAREAID = IG.ITEMDATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ENOVIA_ATTRIBUTES EA
    ON I.PRODUCT = EA.ECORESPRODUCT
LEFT JOIN UniqueCurrency ICU
    ON I.ITEMID = ICU.ITEMID AND I.DATAAREAID = ICU.DATAAREAID AND ICU.cur_rn = 1
LEFT JOIN CTE_Coverage IGS
    ON I.ITEMID = IGS.ITEMID AND I.DATAAREAID = IGS.DATAAREAID
    AND IGS.INVENTLOCATIONID = OI.INVENTLOCATIONID
LEFT JOIN CTE_CostPrice CP
    ON CP.itemid = I.itemid AND CP.dataareaid = I.dataareaid
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.BUYER_GROUPS INVGRP
    ON I.ITEMBUYERGROUPID = INVGRP._GROUP AND INVGRP.DATAAREAID = I.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.RELATIONSHIP_BETWEEN_ITEMS_AND_ITEM_GROUPS IMG
    ON IMG.ITEMDATAAREAID = I.DATAAREAID AND IMG.ITEMID = I.ITEMID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SUPPLY_TYPE_SETUP STS
    ON STS.ITEMID = I.ITEMID AND STS.ITEMDATAAREAID = I.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEM_COVERAGE_GROUPS ICG
    ON ICG.REQGROUPID = IGS.REQGROUPID AND ICG.DATAAREAID = IGS.DATAAREAID
LEFT JOIN CTE_FX fx
    ON fx.CurrencyCode = ICU.CURRENCYCODE AND fx.rn = 1
WHERE OI.physical_inventory > 0
ORDER BY onhand_value_dkk DESC NULLS LAST
LIMIT 500
""", "Inventory enriched (item master + planning + FX)")

# 3. BOM Explosion Forecast — WMA + Seasonal Index → 2026 monthly component demand
bom_rows = run("forecast_bom", """
WITH
CTE_Weights AS (
  SELECT 12 AS w1, 11 AS w2, 10 AS w3,  9 AS w4,
          8 AS w5,  7 AS w6,  6 AS w7,  5 AS w8,
          4 AS w9,  3 AS w10, 2 AS w11, 1 AS w12,
         78 AS total_weight
),
CTE_Sales_History AS (
  SELECT
    ol.itemid, ol.dataareaid,
    DATE_TRUNC('month', CAST(ol.shippingdaterequested AS DATE))  AS sales_month,
    MONTH(CAST(ol.shippingdaterequested AS DATE))                AS cal_month,
    DATEDIFF('month',
      DATE_TRUNC('month', CAST(ol.shippingdaterequested AS DATE)),
      DATE_TRUNC('month', CURRENT_DATE()))                       AS months_ago,
    SUM(ol.salesqty)                                             AS monthly_qty
  FROM FLS_PROD_DB.MART_DYN_FO.ORDER_LINES ol
  WHERE ol.salesstatus NOT IN (4)
    AND CAST(ol.shippingdaterequested AS DATE)
          >= DATEADD('month', -24, DATE_TRUNC('month', CURRENT_DATE()))
    AND CAST(ol.shippingdaterequested AS DATE)
          <  DATE_TRUNC('month', CURRENT_DATE())
  GROUP BY ol.itemid, ol.dataareaid,
    DATE_TRUNC('month', CAST(ol.shippingdaterequested AS DATE)),
    MONTH(CAST(ol.shippingdaterequested AS DATE)),
    DATEDIFF('month',
      DATE_TRUNC('month', CAST(ol.shippingdaterequested AS DATE)),
      DATE_TRUNC('month', CURRENT_DATE()))
),
CTE_WMA AS (
  SELECT h.itemid, h.dataareaid,
    ROUND(SUM(CASE
      WHEN h.months_ago = 1  THEN h.monthly_qty * w.w1
      WHEN h.months_ago = 2  THEN h.monthly_qty * w.w2
      WHEN h.months_ago = 3  THEN h.monthly_qty * w.w3
      WHEN h.months_ago = 4  THEN h.monthly_qty * w.w4
      WHEN h.months_ago = 5  THEN h.monthly_qty * w.w5
      WHEN h.months_ago = 6  THEN h.monthly_qty * w.w6
      WHEN h.months_ago = 7  THEN h.monthly_qty * w.w7
      WHEN h.months_ago = 8  THEN h.monthly_qty * w.w8
      WHEN h.months_ago = 9  THEN h.monthly_qty * w.w9
      WHEN h.months_ago = 10 THEN h.monthly_qty * w.w10
      WHEN h.months_ago = 11 THEN h.monthly_qty * w.w11
      WHEN h.months_ago = 12 THEN h.monthly_qty * w.w12
      ELSE 0
    END) / NULLIF(w.total_weight, 0), 4) AS wma_baseline,
    COUNT(DISTINCT h.sales_month)         AS months_with_data
  FROM CTE_Sales_History h CROSS JOIN CTE_Weights w
  WHERE h.months_ago BETWEEN 1 AND 12
  GROUP BY h.itemid, h.dataareaid, w.total_weight
),
CTE_Seasonal_Raw AS (
  SELECT itemid, dataareaid, cal_month,
    AVG(monthly_qty) AS avg_for_month,
    AVG(AVG(monthly_qty)) OVER (PARTITION BY itemid, dataareaid) AS overall_avg
  FROM CTE_Sales_History GROUP BY itemid, dataareaid, cal_month
),
CTE_Seasonal_Index AS (
  SELECT itemid, dataareaid, cal_month,
    ROUND(avg_for_month / NULLIF(overall_avg, 0), 4) AS seasonal_index
  FROM CTE_Seasonal_Raw
),
CTE_Forecast_2026 AS (
  SELECT w.itemid, w.dataareaid, w.wma_baseline, w.months_with_data,
    ROUND(w.wma_baseline * COALESCE(s1.seasonal_index,  1.0), 2) AS F_JAN,
    ROUND(w.wma_baseline * COALESCE(s2.seasonal_index,  1.0), 2) AS F_FEB,
    ROUND(w.wma_baseline * COALESCE(s3.seasonal_index,  1.0), 2) AS F_MAR,
    ROUND(w.wma_baseline * COALESCE(s4.seasonal_index,  1.0), 2) AS F_APR,
    ROUND(w.wma_baseline * COALESCE(s5.seasonal_index,  1.0), 2) AS F_MAY,
    ROUND(w.wma_baseline * COALESCE(s6.seasonal_index,  1.0), 2) AS F_JUN,
    ROUND(w.wma_baseline * COALESCE(s7.seasonal_index,  1.0), 2) AS F_JUL,
    ROUND(w.wma_baseline * COALESCE(s8.seasonal_index,  1.0), 2) AS F_AUG,
    ROUND(w.wma_baseline * COALESCE(s9.seasonal_index,  1.0), 2) AS F_SEP,
    ROUND(w.wma_baseline * COALESCE(s10.seasonal_index, 1.0), 2) AS F_OCT,
    ROUND(w.wma_baseline * COALESCE(s11.seasonal_index, 1.0), 2) AS F_NOV,
    ROUND(w.wma_baseline * COALESCE(s12.seasonal_index, 1.0), 2) AS F_DEC,
    COALESCE(s1.seasonal_index,  1.0) AS SI_JAN, COALESCE(s2.seasonal_index,  1.0) AS SI_FEB,
    COALESCE(s3.seasonal_index,  1.0) AS SI_MAR, COALESCE(s4.seasonal_index,  1.0) AS SI_APR,
    COALESCE(s5.seasonal_index,  1.0) AS SI_MAY, COALESCE(s6.seasonal_index,  1.0) AS SI_JUN,
    COALESCE(s7.seasonal_index,  1.0) AS SI_JUL, COALESCE(s8.seasonal_index,  1.0) AS SI_AUG,
    COALESCE(s9.seasonal_index,  1.0) AS SI_SEP, COALESCE(s10.seasonal_index, 1.0) AS SI_OCT,
    COALESCE(s11.seasonal_index, 1.0) AS SI_NOV, COALESCE(s12.seasonal_index, 1.0) AS SI_DEC
  FROM CTE_WMA w
  LEFT JOIN CTE_Seasonal_Index s1  ON s1.itemid=w.itemid  AND s1.dataareaid=w.dataareaid AND s1.cal_month=1
  LEFT JOIN CTE_Seasonal_Index s2  ON s2.itemid=w.itemid  AND s2.dataareaid=w.dataareaid AND s2.cal_month=2
  LEFT JOIN CTE_Seasonal_Index s3  ON s3.itemid=w.itemid  AND s3.dataareaid=w.dataareaid AND s3.cal_month=3
  LEFT JOIN CTE_Seasonal_Index s4  ON s4.itemid=w.itemid  AND s4.dataareaid=w.dataareaid AND s4.cal_month=4
  LEFT JOIN CTE_Seasonal_Index s5  ON s5.itemid=w.itemid  AND s5.dataareaid=w.dataareaid AND s5.cal_month=5
  LEFT JOIN CTE_Seasonal_Index s6  ON s6.itemid=w.itemid  AND s6.dataareaid=w.dataareaid AND s6.cal_month=6
  LEFT JOIN CTE_Seasonal_Index s7  ON s7.itemid=w.itemid  AND s7.dataareaid=w.dataareaid AND s7.cal_month=7
  LEFT JOIN CTE_Seasonal_Index s8  ON s8.itemid=w.itemid  AND s8.dataareaid=w.dataareaid AND s8.cal_month=8
  LEFT JOIN CTE_Seasonal_Index s9  ON s9.itemid=w.itemid  AND s9.dataareaid=w.dataareaid AND s9.cal_month=9
  LEFT JOIN CTE_Seasonal_Index s10 ON s10.itemid=w.itemid AND s10.dataareaid=w.dataareaid AND s10.cal_month=10
  LEFT JOIN CTE_Seasonal_Index s11 ON s11.itemid=w.itemid AND s11.dataareaid=w.dataareaid AND s11.cal_month=11
  LEFT JOIN CTE_Seasonal_Index s12 ON s12.itemid=w.itemid AND s12.dataareaid=w.dataareaid AND s12.cal_month=12
),
BOM_Explosion AS (
  SELECT 0 AS LVL, BV.ITEMID AS ROOT_ITEMID, BV.DATAAREAID AS ROOT_DATAAREAID,
    BL.ITEMID AS CHILD_ITEMID, BL.DATAAREAID AS CHILD_DATAAREAID,
    CAST(BL.BOMQTY AS FLOAT) AS TOTAL_QTY_PATH, BV.ITEMID AS BOM_PATH
  FROM FLS_PROD_DB.MART_DYN_FO.BOM_VERSIONS BV
  INNER JOIN FLS_PROD_DB.MART_DYN_FO.BOM_LINES BL
    ON BV.BOMID=BL.BOMID AND BV.DATAAREAID=BL.DATAAREAID
  WHERE BV.ACTIVE=1
  UNION ALL
  SELECT P.LVL + 1, P.ROOT_ITEMID, P.ROOT_DATAAREAID,
    BL.ITEMID, BL.DATAAREAID,
    P.TOTAL_QTY_PATH * BL.BOMQTY,
    P.BOM_PATH || ' > ' || BL.ITEMID
  FROM BOM_Explosion P
  INNER JOIN FLS_PROD_DB.MART_DYN_FO.BOM_VERSIONS BV
    ON BV.ITEMID=P.CHILD_ITEMID AND BV.DATAAREAID=P.CHILD_DATAAREAID AND BV.ACTIVE=1
  INNER JOIN FLS_PROD_DB.MART_DYN_FO.BOM_LINES BL
    ON BV.BOMID=BL.BOMID AND BL.DATAAREAID=BV.DATAAREAID
  WHERE P.LVL < 7
),
CTE_Component_2026 AS (
  SELECT
    e.CHILD_ITEMID AS COMPONENT_ITEMID, e.CHILD_DATAAREAID AS COMPONENT_DATAAREAID,
    e.ROOT_ITEMID, e.LVL AS BOM_LEVEL, e.BOM_PATH,
    e.TOTAL_QTY_PATH AS QTY_PER_ROOT_UNIT,
    f.wma_baseline AS ROOT_WMA, f.months_with_data,
    ROUND(f.F_JAN * e.TOTAL_QTY_PATH, 2) AS C_JAN,
    ROUND(f.F_FEB * e.TOTAL_QTY_PATH, 2) AS C_FEB,
    ROUND(f.F_MAR * e.TOTAL_QTY_PATH, 2) AS C_MAR,
    ROUND(f.F_APR * e.TOTAL_QTY_PATH, 2) AS C_APR,
    ROUND(f.F_MAY * e.TOTAL_QTY_PATH, 2) AS C_MAY,
    ROUND(f.F_JUN * e.TOTAL_QTY_PATH, 2) AS C_JUN,
    ROUND(f.F_JUL * e.TOTAL_QTY_PATH, 2) AS C_JUL,
    ROUND(f.F_AUG * e.TOTAL_QTY_PATH, 2) AS C_AUG,
    ROUND(f.F_SEP * e.TOTAL_QTY_PATH, 2) AS C_SEP,
    ROUND(f.F_OCT * e.TOTAL_QTY_PATH, 2) AS C_OCT,
    ROUND(f.F_NOV * e.TOTAL_QTY_PATH, 2) AS C_NOV,
    ROUND(f.F_DEC * e.TOTAL_QTY_PATH, 2) AS C_DEC,
    f.SI_JAN, f.SI_FEB, f.SI_MAR, f.SI_APR, f.SI_MAY, f.SI_JUN,
    f.SI_JUL, f.SI_AUG, f.SI_SEP, f.SI_OCT, f.SI_NOV, f.SI_DEC
  FROM BOM_Explosion e
  INNER JOIN CTE_Forecast_2026 f
    ON f.itemid=e.ROOT_ITEMID AND f.dataareaid=e.ROOT_DATAAREAID
)
SELECT
  cm.COMPONENT_ITEMID                               AS component_itemid,
  COALESCE(tr.NAME, cm.COMPONENT_ITEMID)            AS part_name,
  cm.COMPONENT_DATAAREAID                           AS company,
  cm.BOM_LEVEL                                      AS bom_level,
  cm.BOM_PATH                                       AS bom_path,
  cm.QTY_PER_ROOT_UNIT                              AS qty_per_root_unit,
  cm.ROOT_WMA                                       AS wma_baseline,
  cm.months_with_data                               AS months_with_data,
  SUM(cm.C_JAN)  AS jan_2026, SUM(cm.C_FEB)  AS feb_2026,
  SUM(cm.C_MAR)  AS mar_2026, SUM(cm.C_APR)  AS apr_2026,
  SUM(cm.C_MAY)  AS may_2026, SUM(cm.C_JUN)  AS jun_2026,
  SUM(cm.C_JUL)  AS jul_2026, SUM(cm.C_AUG)  AS aug_2026,
  SUM(cm.C_SEP)  AS sep_2026, SUM(cm.C_OCT)  AS oct_2026,
  SUM(cm.C_NOV)  AS nov_2026, SUM(cm.C_DEC)  AS dec_2026,
  SUM(cm.C_JAN + cm.C_FEB + cm.C_MAR + cm.C_APR
    + cm.C_MAY + cm.C_JUN + cm.C_JUL + cm.C_AUG
    + cm.C_SEP + cm.C_OCT + cm.C_NOV + cm.C_DEC)  AS annual_2026,
  MAX(cm.SI_JAN) AS si_jan, MAX(cm.SI_FEB) AS si_feb,
  MAX(cm.SI_MAR) AS si_mar, MAX(cm.SI_APR) AS si_apr,
  MAX(cm.SI_MAY) AS si_may, MAX(cm.SI_JUN) AS si_jun,
  MAX(cm.SI_JUL) AS si_jul, MAX(cm.SI_AUG) AS si_aug,
  MAX(cm.SI_SEP) AS si_sep, MAX(cm.SI_OCT) AS si_oct,
  MAX(cm.SI_NOV) AS si_nov, MAX(cm.SI_DEC) AS si_dec
FROM CTE_Component_2026 cm
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEMS it
  ON it.ITEMID=cm.COMPONENT_ITEMID AND UPPER(it.DATAAREAID)=UPPER(cm.COMPONENT_DATAAREAID)
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.PRODUCTS ep ON ep.RECID=it.PRODUCT
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.PRODUCT_TRANSLATIONS tr
  ON tr.PRODUCT=ep.RECID AND tr.LANGUAGEID='en-US'
GROUP BY
  cm.COMPONENT_ITEMID, cm.COMPONENT_DATAAREAID, cm.BOM_LEVEL, cm.BOM_PATH,
  cm.QTY_PER_ROOT_UNIT, cm.ROOT_WMA, cm.months_with_data,
  COALESCE(tr.NAME, cm.COMPONENT_ITEMID)
ORDER BY cm.BOM_LEVEL, annual_2026 DESC NULLS LAST
LIMIT 2000
""", "BOM explosion forecast 2026 (WMA + seasonal)")

# Derive forecast_by_month.json for Dashboard chart from BOM data
MONTH_COLS = [
    ('2026-01-01', 'jan_2026'), ('2026-02-01', 'feb_2026'), ('2026-03-01', 'mar_2026'),
    ('2026-04-01', 'apr_2026'), ('2026-05-01', 'may_2026'), ('2026-06-01', 'jun_2026'),
    ('2026-07-01', 'jul_2026'), ('2026-08-01', 'aug_2026'), ('2026-09-01', 'sep_2026'),
    ('2026-10-01', 'oct_2026'), ('2026-11-01', 'nov_2026'), ('2026-12-01', 'dec_2026'),
]
forecast_monthly = [
    {
        'company': 'ALL', 'model_id': 'BOM_WMA_SEASONAL', 'month': md,
        'total_forecast_qty': sum(float(r.get(col) or 0) for r in bom_rows),
        'total_forecast_amount': 0,
        'distinct_items': sum(1 for r in bom_rows if float(r.get(col) or 0) > 0),
    }
    for md, col in MONTH_COLS
]
with open(os.path.join(OUTPUT, 'forecast_by_month.json'), 'w') as f:
    json.dump(forecast_monthly, f, default=to_json, indent=2)
print(f"  → forecast_by_month (derived from BOM): 12 months")

# Keep forecast.json as alias for forecast_bom for backward compat
import shutil
shutil.copy(os.path.join(OUTPUT, 'forecast_bom.json'), os.path.join(OUTPUT, 'forecast.json'))

# 5. Purchase orders — proper PROD view
run("purchase_orders", """
WITH CTE_RECEIVED_QTY AS (
    SELECT
        A.DATAAREAID AS LEGALENTITY,
        A.PURCHID AS PURCHASE_ORDER,
        B.LINENUMBER AS LINE_NUMBER,
        SUM(NVL(C.QTY, 0)) AS RECEIVED_QTY,
        MAX(C.DELIVERYDATE) AS DELIVERYDATE
    FROM FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDERS A
    INNER JOIN FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDER_LINE B
        ON A.PURCHID = B.PURCHID AND A.DATAAREAID = B.DATAAREAID
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.VENDOR_PRODUCT_RECEIPT_LINES C
        ON C.INVENTTRANSID = B.INVENTTRANSID AND C.DATAAREAID = A.DATAAREAID
    GROUP BY 1, 2, 3
)
SELECT
    POH.PURCHID                                                             AS po_number,
    SOL.SALESID                                                             AS sales_order_id,
    SO.SALESNAME                                                            AS customer_name,
    POL.LINENUMBER                                                          AS po_line_number,
    POH.ORDERACCOUNT                                                        AS vendor_id,
    POL.ITEMID                                                              AS item_id,
    POL.NAME                                                                AS item_name,
    S.DATAAREAID                                                            AS company,
    S.NAME                                                                  AS site_name,
    POH.CURRENCYCODE                                                        AS currency,
    TRY_TO_DATE(LEFT(POH.CREATEDDATETIME, 10))                              AS po_created_date,
    NULLIF(LEFT(POL.REQUESTEDSHIPDATE, 10), '1900-01-01')                   AS requested_ship_date,
    NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01')                   AS confirmed_ship_date,
    NULLIF(LEFT(POL.DELIVERYDATE, 10), '1900-01-01')                        AS due_date,
    NULLIF(LEFT(RECE_QTY.DELIVERYDATE, 10), '1900-01-01')                   AS delivery_date,
    NULLIF(LEFT(POL.CONFIRMEDDLV, 10), '1900-01-01')                        AS confirmed_dlv,
    COALESCE(
        CASE WHEN (POL.DLVTERM LIKE 'F%' OR POL.DLVTERM LIKE 'C%')
                  THEN NULLIF(LEFT(POL.REQUESTEDSHIPDATE, 10), '1900-01-01')
             WHEN (POL.DLVTERM LIKE 'D%')
                  THEN NULLIF(LEFT(POL.DELIVERYDATE, 10), '1900-01-01') END,
        NULLIF(LEFT(POL.REQUESTEDSHIPDATE, 10), '1900-01-01')
    )                                                                       AS promised_date,
    CASE
        WHEN (POL.DLVTERM LIKE 'F%' OR POL.DLVTERM LIKE 'C%')
             AND MGL_PS.PURCHASE_ORDER_LINE_PURCH_STATUS IN ('Invoiced', 'Received')
             THEN NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01')
        WHEN (POL.DLVTERM LIKE 'D%')
             THEN NULLIF(LEFT(RECE_QTY.DELIVERYDATE, 10), '1900-01-01')
        ELSE NULL
    END                                                                     AS po_fulfillment_date,
    DATEDIFF(DAY, TRY_TO_DATE(LEFT(POL.CREATEDDATETIME,10)),
        TRY_TO_DATE(NULLIF(LEFT(POL.CONFIRMEDSHIPDATE,10),'1900-01-01'))
    )                                                                       AS supplier_production_lead_time,
    DATEDIFF(DAY,
        TRY_TO_DATE(NULLIF(LEFT(POL.CONFIRMEDSHIPDATE,10),'1900-01-01')),
        TRY_TO_DATE(LEFT(POL.CONFIRMEDDLV,10))
    )                                                                       AS shipping_lead_time,
    DATEDIFF(DAY, TRY_TO_DATE(LEFT(POL.CREATEDDATETIME,10)),
        TRY_TO_DATE(LEFT(POL.CONFIRMEDDLV,10))
    )                                                                       AS sifot_lead_time,
    POL.PURCHQTY                                                            AS ordered_qty,
    RECE_QTY.RECEIVED_QTY                                                   AS received_qty,
    POL.REMAINPURCHPHYSICAL                                                 AS remaining_qty,
    POL.PURCHPRICE                                                          AS unit_price,
    POL.LINEAMOUNT                                                          AS order_price,
    (RECE_QTY.RECEIVED_QTY * POL.PURCHPRICE)                               AS received_value,
    (POL.REMAINPURCHPHYSICAL * POL.PURCHPRICE)                              AS remaining_value,
    MGL_PHS.PURCHASE_ORDER_PURCH_STATUS                                     AS po_header_status,
    MGL_PS.PURCHASE_ORDER_LINE_PURCH_STATUS                                 AS po_status,
    MGL_DSTATE.PURCHASE_ORDER_DOCUMENT_STATE                                AS approval_status,
    CASE WHEN POL.FLSEXPEDITESTATUS IN ('T','K')
              AND MGL_PS.PURCHASE_ORDER_LINE_PURCH_STATUS = 'Open order'
              AND DATEADD(DAY,3,TRY_TO_DATE(LEFT(POL.CONFIRMEDDLV,10))) < CURRENT_DATE()
         THEN 'YES' ELSE 'NO' END                                           AS past_due,
    CASE
        WHEN COALESCE(
                CASE WHEN (POL.DLVTERM LIKE 'F%' OR POL.DLVTERM LIKE 'C%')
                     THEN NULLIF(LEFT(POL.REQUESTEDSHIPDATE,10),'1900-01-01')
                     WHEN (POL.DLVTERM LIKE 'D%')
                     THEN NULLIF(LEFT(POL.DELIVERYDATE,10),'1900-01-01') END,
                NULLIF(LEFT(POL.REQUESTEDSHIPDATE,10),'1900-01-01')
             ) IS NULL THEN 'Exclude'
        WHEN MGL_DSTATE.PURCHASE_ORDER_DOCUMENT_STATE IN ('Draft','In review','Rejected') THEN 'Exclude'
        WHEN POH.INVENTSITEID IS NULL OR POL.ITEMID LIKE '0TOOL%' THEN 'Exclude'
        WHEN MGL_PS.PURCHASE_ORDER_LINE_PURCH_STATUS IN ('Canceled','Open order') THEN 'Exclude'
        ELSE 'Include'
    END                                                                     AS sifot_exclusion
FROM FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDERS POH
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDER_LINE POL
    ON POH.PURCHID = POL.PURCHID AND POL.ISDELETED = 0 AND POH.DATAAREAID = POL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ORDER_LINES SOL
    ON POL.INVENTREFID = SOL.INVENTREFID AND POL.DATAAREAID = SOL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SALES_ORDERS SO
    ON SO.SALESID = SOL.SALESID AND SO.DATAAREAID = SOL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SITE S
    ON POL.DATAAREAID = S.DATAAREAID
   AND UPPER(COALESCE(NULLIF(POH.DATAAREAID,'sa1'), POH.INVENTSITEID)) = UPPER(S.SITEID)
LEFT JOIN CTE_RECEIVED_QTY RECE_QTY
    ON RECE_QTY.PURCHASE_ORDER = POL.PURCHID
   AND RECE_QTY.LINE_NUMBER = POL.LINENUMBER
   AND RECE_QTY.LEGALENTITY = POL.DATAAREAID
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS PURCHASE_ORDER_LINE_PURCH_STATUS
    FROM FLS_PROD_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    WHERE s.ENTITY_GLOBALSET_LINK_HK IN (
        SELECT entity_hk FROM FLS_PROD_DB.EDW_META.ENTITY_HUB WHERE entity = 'purchline')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_PS ON POL.PURCHSTATUS = MGL_PS.OPTION
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS PURCHASE_ORDER_PURCH_STATUS
    FROM FLS_PROD_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    WHERE s.ENTITY_GLOBALSET_LINK_HK IN (
        SELECT entity_hk FROM FLS_PROD_DB.EDW_META.ENTITY_HUB WHERE entity = 'purchTable')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_PHS ON POH.PURCHSTATUS = MGL_PHS.OPTION
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS PURCHASE_ORDER_DOCUMENT_STATE
    FROM FLS_PROD_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_DSTATE ON POH.DOCUMENTSTATE = MGL_DSTATE.OPTION
WHERE POL.PURCHSTATUS IN (1, 2, 3)
GROUP BY ALL
ORDER BY po_created_date DESC
LIMIT 1000
""", "Purchase orders (SIFOT + lead times)")

# 6. Sales order lines — DIFOT business view
run("sales_orders", """
SELECT
    DCS.BUSINESSUNITVALUE                                                   AS business_unit,
    S.DATAAREAID                                                            AS company,
    S.NAME                                                                  AS site_name,
    CR.CUSTOMER_COUNTRY_NAME                                                AS destination_country,
    CR.CUSTOMER_REGION_NAME_SERVICE                                         AS destination_region,
    SO.SALESID                                                              AS sales_order_id,
    SOL.LINENUM                                                             AS line_num,
    LEFT(SO.CREATEDDATETIME, 10)                                            AS so_created_date,
    CASE WHEN SO.MCRORDERSTOPPED = 0 THEN 'ACTIVE' ELSE 'HOLD' END         AS active_hold,
    SOL.ItemId                                                              AS item_id,
    SOL.NAME                                                                AS item_name,
    TO_DECIMAL(SOL.SALESPRICE, 38, 6)                                       AS unit_price,
    SOL.SALESQTY                                                            AS ordered_qty,
    LEFT(SOL.RECEIPTDATECONFIRMED, 10)                                      AS receipt_date_confirmed,
    SOL.LINEAMOUNT                                                          AS so_line_value,
    DS.SUPPLIER_NAME                                                        AS supplier_name,
    CPSL.QTY                                                                AS delivered_qty,
    (TO_DECIMAL(SOL.SALESPRICE, 38, 6) * NVL(CPSL.QTY, 0))                 AS invoiced_line_amount,
    SOL.CURRENCYCODE                                                        AS currency,
    SOL.DLVTERM                                                             AS inco_term,
    EA.MOCCODE                                                              AS moc,
    EA.PRODUCTTYPOLOGY                                                      AS product_typology,
    DCS.OFFERINGTYPEVALUE                                                   AS order_typology,
    SO.CustAccount                                                          AS customer_number,
    SO.SALESNAME                                                            AS customer_name,
    AD.COUNTRYREGIONID                                                      AS country,
    MGL_SOL.sales_order_sales_status                                        AS sales_status,
    MGL_SO.sales_order_header_sales_status                                  AS sales_header_status,
    COALESCE(
        NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01'),
        NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED, 10), '1900-01-01')
    )                                                                       AS promised_date,
    NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED, 10), '1900-01-01')               AS shipping_date_requested,
    NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01')               AS shipping_date_confirmed,
    CIL.INVOICEID                                                           AS invoice_number,
    NULLIF(LEFT(CPSL.CREATEDDATETIME, 10), '1900-01-01')                    AS delivery_date,
    MAX(NULLIF(LEFT(CIL.INVOICEDATE, 10), '1900-01-01'))                    AS invoiced_date,
    CASE
        WHEN NULLIF(LEFT(CPSL.CREATEDDATETIME,10),'1900-01-01') IS NOT NULL
             AND COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'),
                          NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01')) IS NOT NULL
             AND DATEDIFF(DAY,
                    NULLIF(LEFT(CPSL.CREATEDDATETIME,10),'1900-01-01')::DATE,
                    COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'),
                             NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01'))::DATE
                ) >= 0 THEN 'ON-TIME'
        WHEN COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'),
                      NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01')) <= TO_VARCHAR(CURRENT_DATE())
             AND NULLIF(LEFT(CPSL.CREATEDDATETIME,10),'1900-01-01') IS NULL THEN 'LATE'
        WHEN COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'),
                      NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01')) IS NULL THEN 'N/A'
        ELSE 'LATE'
    END                                                                     AS line_status,
    DATEDIFF(DAY,
        COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'),
                 NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01'))::DATE,
        NULLIF(LEFT(CPSL.CREATEDDATETIME,10),'1900-01-01')::DATE
    )                                                                       AS date_difference,
    CASE WHEN COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'),
                       NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01')) <= TO_VARCHAR(CURRENT_DATE())
         THEN 'INCLUDE' ELSE 'EXCLUDE' END                                  AS difot_exclusion,
    CASE WHEN CONTAINS(UPPER(SOL.DELIVERYNAME), 'FLS')
         THEN 'INTERNAL' ELSE 'EXTERNAL' END                                AS delivery_supplier_type
FROM FLS_PROD_DB.MART_DYN_FO.SALES_ORDERS SO
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ORDER_LINES SOL
    ON SO.SALESID = SOL.SALESID AND SO.DATAAREAID = SOL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEMS I
    ON SOL.ITEMID = I.ITEMID AND SOL.DATAAREAID = I.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.CUSTOMER_PACKING_SLIP_LINES CPSL
    ON SOL.SALESID = CPSL.SALESID AND SOL.LINENUM = CPSL.LINENUM AND SO.DATAAREAID = CPSL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.CUSTOMER_INVOICE_LINES CIL
    ON SOL.SALESID = CIL.SALESID AND SOL.LINENUM = CIL.LINENUM
   AND SOL.DATAAREAID = CIL.DATAAREAID AND SOL.ITEMID = CIL.ITEMID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ENOVIA_ATTRIBUTES EA ON I.PRODUCT = EA.ECORESPRODUCT
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DIMENSION_CODE_SET DCS ON SOL.DEFAULTDIMENSION = DCS.RECID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ADDRESSES AD ON SO.DELIVERYPOSTALADDRESS = AD.RECID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SITE S
    ON UPPER(SO.DATAAREAID) = UPPER(S.DATAAREAID)
   AND UPPER(CASE WHEN SO.DATAAREAID = 'sa1' THEN 'SAUDI ARABIA' ELSE SO.INVENTSITEID END) = UPPER(S.NAME)
LEFT JOIN FLS_PROD_DB.RAW_SHAREPOINT_CI.COUNTRY_REGION CR ON DCS.DESTINATIONVALUE = CR.CUSTOMER_COUNTRY_CODE
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DIM_SUPPLIER DS ON I.PRIMARYVENDORID = DS.SUPPLIER_NUMBER
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS sales_order_sales_status
    FROM FLS_DEV_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    WHERE s.ENTITY_GLOBALSET_LINK_HK IN (
        SELECT entity_hk FROM FLS_DEV_DB.EDW_META.ENTITY_HUB WHERE entity = 'salesline')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_SOL ON SOL.SALESSTATUS = MGL_SOL.OPTION
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS sales_order_header_sales_status
    FROM FLS_DEV_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    WHERE s.ENTITY_GLOBALSET_LINK_HK IN (
        SELECT entity_hk FROM FLS_DEV_DB.EDW_META.ENTITY_HUB WHERE entity = 'salestable')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_SO ON SO.SALESSTATUS = MGL_SO.OPTION
GROUP BY ALL
ORDER BY so_created_date DESC
LIMIT 800
""", "Sales order lines (DIFOT)")

# 7. Slow-moving items
run("slow_moving", """
WITH movement_summary AS (
    SELECT
        it.DATAAREAID,
        it.ITEMID,
        id.INVENTLOCATIONID,
        NULLIF(MAX(it.DATEPHYSICAL), '1900-01-01'::DATE)      AS last_physical_move,
        COUNT_IF(
            it.DATEPHYSICAL > '1900-01-01'::DATE
            AND it.DATEPHYSICAL >= DATEADD('day', -365, CURRENT_DATE())
        )                                                     AS tx_last_365_days,
        DATEDIFF('day',
            NULLIF(MAX(it.DATEPHYSICAL), '1900-01-01'::DATE),
            CURRENT_DATE()
        )                                                     AS days_since_last_move
    FROM FLS_PROD_DB.MART_DYN_FO.INVENTORY_TRANSACTIONS it
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.INVENTORY_DIMENSIONS id
        ON it.INVENTDIMID = id.INVENTDIMID AND it.DATAAREAID = id.DATAAREAID
    WHERE it.DATEPHYSICAL IS NOT NULL AND it.DATEPHYSICAL > '1900-01-01'::DATE
    GROUP BY it.DATAAREAID, it.ITEMID, id.INVENTLOCATIONID
),
classified AS (
    SELECT *,
        CASE
            WHEN last_physical_move IS NULL          THEN 'Never moved'
            WHEN days_since_last_move > 365          THEN 'Non-moving (>1yr)'
            WHEN days_since_last_move > 180
              OR tx_last_365_days < 3                THEN 'Slow-moving'
            ELSE 'Normal'
        END AS movement_category
    FROM movement_summary
),
RankedPrice AS (
    SELECT DATAAREAID, ITEMID, PRICE,
           ROW_NUMBER() OVER (PARTITION BY ITEMID, DATAAREAID ORDER BY ACTIVATIONDATE DESC) AS rn
    FROM FLS_PROD_DB.MART_DYN_FO.PRICE
    WHERE PRICETYPE = '0' AND ACTIVATIONDATE <= CURRENT_DATE()
),
AggOnHand AS (
    SELECT DATAAREAID, ITEMID, INVENTLOCATIONID,
           SUM(PHYSICALINVENT) AS on_hand_qty,
           SUM(AVAILPHYSICAL)  AS available_qty
    FROM FLS_PROD_DB.MART_DYN_FO.ONHAND_INVENTORY
    GROUP BY DATAAREAID, ITEMID, INVENTLOCATIONID
)
SELECT
    c.DATAAREAID                                            AS company,
    c.ITEMID                                                AS item_id,
    PT.DESCRIPTION                                          AS part_description,
    c.INVENTLOCATIONID                                      AS warehouse,
    c.last_physical_move,
    c.tx_last_365_days,
    c.days_since_last_move,
    c.movement_category,
    COALESCE(oh.on_hand_qty,  0)                           AS on_hand_qty,
    COALESCE(oh.available_qty, 0)                          AS available_qty,
    p.PRICE                                                 AS unit_price,
    COALESCE(oh.on_hand_qty, 0) * COALESCE(p.PRICE, 0)    AS onhand_value_local
FROM classified c
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEMS I
    ON c.ITEMID = I.ITEMID AND c.DATAAREAID = I.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.PRODUCT_TRANSLATIONS PT
    ON I.PRODUCT = PT.PRODUCT AND PT.LANGUAGEID = 'en-US'
LEFT JOIN AggOnHand oh
    ON oh.DATAAREAID = c.DATAAREAID AND oh.ITEMID = c.ITEMID
    AND oh.INVENTLOCATIONID = c.INVENTLOCATIONID
LEFT JOIN RankedPrice p
    ON p.DATAAREAID = c.DATAAREAID AND p.ITEMID = c.ITEMID AND p.rn = 1
WHERE c.movement_category <> 'Normal'
ORDER BY c.movement_category, c.days_since_last_move DESC NULLS LAST
LIMIT 1000
""", "Slow-moving items")

# 8. Supply vs demand gap
run("supply_demand_gap", """
    SELECT
        inv.DATAAREAID          AS company,
        inv.ITEMID              AS item_id,
        inv.INVENTSITEID        AS site,
        SUM(ABS(inv.AVAILPHYSICAL))  AS avail_physical,
        SUM(ABS(inv.ONORDER))        AS on_order,
        COALESCE(fc.forecast_qty, 0) AS forecast_demand,
        SUM(ABS(inv.AVAILPHYSICAL)) + SUM(ABS(inv.ONORDER))
            - COALESCE(fc.forecast_qty, 0) AS gap
    FROM ONHAND_INVENTORY inv
    LEFT JOIN (
        SELECT DATAAREAID, ITEMID, SUM(SALESQTY) AS forecast_qty
        FROM DEMAND_FORECAST
        WHERE ACTIVE = 1
          AND TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF7')
              BETWEEN CURRENT_DATE AND DATEADD('month', 3, CURRENT_DATE)
        GROUP BY DATAAREAID, ITEMID
    ) fc ON inv.DATAAREAID = fc.DATAAREAID AND inv.ITEMID = fc.ITEMID
    WHERE ABS(inv.AVAILPHYSICAL) > 0 OR ABS(inv.ONORDER) > 0
    GROUP BY inv.DATAAREAID, inv.ITEMID, inv.INVENTSITEID, fc.forecast_qty
    ORDER BY gap ASC
    LIMIT 300
""", "Supply vs demand gap")

conn.close()
elapsed = time.time() - t_start
print(f"\n{'='*55}")
print(f"  Done! All files in: frontend/public/data/")
print(f"  Total time: {elapsed:.0f}s")
print(f"{'='*55}")
print()
print("Next steps:")
print("  1. cd ../frontend")
print("  2. npx vercel --prod")
print()
