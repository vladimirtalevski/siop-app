from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from db import get_connection, rewrite_sql, USE_MOTHERDUCK
import pandas as pd
import threading
from typing import Optional
from forecast_ml import run_prophet_forecast
from chat import run_chat


def _warm_connection():
    try:
        conn = get_connection()
        if USE_MOTHERDUCK:
            import duckdb
            conn.execute("SELECT 1")
        else:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_TIMESTAMP()")
            cur.fetchone()
            cur.close()
        print("✓ Connection ready.")
    except Exception as e:
        print(f"✗ Connection failed: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start SSO auth in a background thread so uvicorn starts accepting requests
    # The SSO popup will appear in the terminal/browser immediately on server start
    t = threading.Thread(target=_warm_connection, daemon=True)
    t.start()
    yield


app = FastAPI(title="SIOP Manager API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"error": str(exc)},
        headers={"Access-Control-Allow-Origin": "*"},
    )


def run_query(sql: str, params=None) -> list[dict]:
    conn = get_connection()
    sql = rewrite_sql(sql)
    if USE_MOTHERDUCK:
        result = conn.execute(sql).fetchdf()
        return result.to_dict(orient="records")
    else:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.get("/api/debug/schemas")
def debug_schemas():
    """Returns all schemas and tables visible in the current MotherDuck connection."""
    conn = get_connection()
    tables = conn.execute("""
        SELECT table_catalog, table_schema, table_name,
               (SELECT COUNT(*) FROM information_schema.columns
                WHERE table_schema = t.table_schema AND table_name = t.table_name) AS col_count
        FROM information_schema.tables t
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_catalog, table_schema, table_name
    """).fetchdf()
    return tables.to_dict(orient="records")


# ── Inventory ──────────────────────────────────────────────────────────────

@app.get("/api/inventory")
def get_inventory(company: Optional[str] = None, limit: int = 500):
    co_filter = f"AND UPPER(I.DATAAREAID) = '{company.upper()}'" if company else ""
    sql = f"""
    WITH
    RankedPrices AS (
        SELECT IDIM.INVENTSITEID, IIPS.ITEMID, IIPS.PRICE, IIPS.DATAAREAID, IIPS.UNITID,
               ROW_NUMBER() OVER (
                   PARTITION BY IIPS.ITEMID, IIPS.DATAAREAID, IDIM.INVENTSITEID, IIPS.PRICETYPE
                   ORDER BY IIPS.ACTIVATIONDATE DESC
               ) AS rn
        FROM FLS_PROD_DB.MART_DYN_FO.PRICE IIPS
        INNER JOIN FLS_PROD_DB.MART_DYN_FO.INVENTORY_DIMENSIONS IDIM
            ON IIPS.InventDimId = IDIM.InventDimId
        WHERE IIPS.PRICETYPE = '0' AND IIPS.ACTIVATIONDATE <= CURRENT_DATE()
    ),
    UniqueCurrency AS (
        SELECT ITEMID, DATAAREAID, CURRENCYCODE,
               ROW_NUMBER() OVER (PARTITION BY ITEMID, DATAAREAID ORDER BY CURRENCYCODE) AS cur_rn
        FROM FLS_PROD_DB.MART_DYN_FO.INVENTORY_TRANSACTIONS
        WHERE CURRENCYCODE IS NOT NULL
    ),
    AggregatedInventory AS (
        SELECT OI.ITEMID, OI.DATAAREAID, OI.WMSLOCATIONID, OI.INVENTSITEID, ID.INVENTLOCATIONID,
               SUM(OI.POSTEDQTY + OI.RECEIVED - OI.DEDUCTED + OI.REGISTERED - OI.PICKED) AS physical_inventory,
               SUM(OI.RESERVPHYSICAL) AS physical_reserved,
               SUM(OI.AVAILPHYSICAL)  AS available_physical,
               SUM(OI.ORDERED)        AS on_order,
               SUM(OI.AVAILORDERED)   AS total_available
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
        SELECT mfr.CURRENCY AS CurrencyCode, mfr."CONVERSATION RATE" AS FxRate,
               ROW_NUMBER() OVER (PARTITION BY mfr.CURRENCY ORDER BY mfr.YEAR DESC, mfr."MONTH NO" DESC) AS rn
        FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_MASTER_FX_RATES mfr
        WHERE mfr.YEAR * 100 + mfr."MONTH NO" <= YEAR(CURRENT_DATE()) * 100 + MONTH(CURRENT_DATE())
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
        IG.ITEMGROUPID                                                  AS class_id,
        ICU.CURRENCYCODE                                                AS local_currency,
        OI.physical_inventory                                           AS on_hand_qty,
        OI.physical_reserved                                            AS reserved_qty,
        OI.available_physical                                           AS available_qty,
        OI.on_order                                                     AS on_order_qty,
        OI.total_available                                              AS total_available_qty,
        IPC.PRICE                                                       AS unit_price,
        IPC.UNITID                                                      AS uom,
        CP.cost_price                                                   AS cost_price,
        CASE WHEN IPC.PRICE IS NULL THEN 'No Price' ELSE 'OK' END       AS has_item_price,
        OI.physical_inventory * COALESCE(IPC.PRICE, 0)                 AS onhand_value_local,
        OI.physical_inventory * COALESCE(IPC.PRICE, 0)
            * COALESCE(fx.FxRate, 0)                                    AS onhand_value_dkk,
        COALESCE(fx.FxRate, 0)                                          AS fx_rate_to_dkk,
        CASE W.inventlocationtype
            WHEN 0 THEN 'Default' WHEN 1 THEN 'Quarantine' WHEN 2 THEN 'Transit'
            WHEN 10 THEN 'Goods in transit' WHEN 101 THEN 'Goods shipped'
            ELSE 'Unknown'
        END                                                             AS warehouse_type,
        W.NAME                                                          AS warehouse_name,
        IPOS.LEADTIME                                                   AS lead_time,
        IPOS.LOWESTQTY                                                  AS min_order_qty,
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
            WHEN 2 THEN 'Transfer' WHEN 3 THEN 'Kanban' ELSE NULL
        END                                                             AS source_type,
        ICG.REQGROUPID                                                  AS coverage_group,
        INVGRP.DESCRIPTION                                              AS buyer_group
    FROM FLS_PROD_DB.MART_DYN_FO.ITEMS I
    INNER JOIN AggregatedInventory OI ON I.ITEMID = OI.ITEMID AND I.DATAAREAID = OI.DATAAREAID
    LEFT JOIN RankedPrices IPC
        ON IPC.itemid = I.itemid AND IPC.dataareaid = I.dataareaid
        AND IPC.INVENTSITEID = OI.INVENTSITEID AND IPC.rn = 1
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.WAREHOUSES W
        ON OI.INVENTLOCATIONID = W.INVENTLOCATIONID AND OI.DATAAREAID = W.DATAAREAID
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.PRODUCT_TRANSLATIONS PT
        ON I.PRODUCT = PT.PRODUCT AND PT.LANGUAGEID = 'en-US'
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DIMENSION_CODE_SET DCS ON I.DEFAULTDIMENSION = DCS.RECID
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEM_PURCHASE_ORDER_SETTINGS IPOS
        ON I.ITEMID = IPOS.ITEMID AND I.DATAAREAID = IPOS.DATAAREAID AND IPOS.OVERRIDE = '1'
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.RELATIONSHIP_BETWEEN_ITEMS_AND_ITEM_GROUPS IG
        ON I.ITEMID = IG.ITEMID AND I.DATAAREAID = IG.ITEMDATAAREAID
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ENOVIA_ATTRIBUTES EA ON I.PRODUCT = EA.ECORESPRODUCT
    LEFT JOIN UniqueCurrency ICU
        ON I.ITEMID = ICU.ITEMID AND I.DATAAREAID = ICU.DATAAREAID AND ICU.cur_rn = 1
    LEFT JOIN CTE_Coverage IGS
        ON I.ITEMID = IGS.ITEMID AND I.DATAAREAID = IGS.DATAAREAID
        AND IGS.INVENTLOCATIONID = OI.INVENTLOCATIONID
    LEFT JOIN CTE_CostPrice CP ON CP.itemid = I.itemid AND CP.dataareaid = I.dataareaid
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.BUYER_GROUPS INVGRP
        ON I.ITEMBUYERGROUPID = INVGRP._GROUP AND INVGRP.DATAAREAID = I.DATAAREAID
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SUPPLY_TYPE_SETUP STS
        ON STS.ITEMID = I.ITEMID AND STS.ITEMDATAAREAID = I.DATAAREAID
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEM_COVERAGE_GROUPS ICG
        ON ICG.REQGROUPID = IGS.REQGROUPID AND ICG.DATAAREAID = IGS.DATAAREAID
    LEFT JOIN CTE_FX fx ON fx.CurrencyCode = ICU.CURRENCYCODE AND fx.rn = 1
    WHERE OI.physical_inventory > 0
    {co_filter}
    ORDER BY onhand_value_dkk DESC NULLS LAST
    LIMIT {limit}
    """
    return run_query(sql)


@app.get("/api/inventory/summary")
def get_inventory_summary():
    sql = """
        SELECT
            DATAAREAID AS company,
            INVENTSITEID AS site,
            COUNT(DISTINCT ITEMID) AS distinct_items,
            SUM(AVAILPHYSICAL) AS total_avail_physical,
            SUM(PHYSICALINVENT) AS total_physical,
            SUM(ONORDER) AS total_on_order,
            SUM(RESERVPHYSICAL) AS total_reserved
        FROM ONHAND_INVENTORY
        WHERE AVAILPHYSICAL != 0 OR PHYSICALINVENT != 0
        GROUP BY DATAAREAID, INVENTSITEID
        ORDER BY company, site
    """
    return run_query(sql)


# ── BOM Explosion Forecast ──────────────────────────────────────────────────

@app.get("/api/forecast/bom")
def get_bom_forecast(company: Optional[str] = None, limit: int = 2000):
    co_filter_ol  = f"AND UPPER(ol.dataareaid) = '{company.upper()}'"  if company else ""
    co_filter_bom = f"AND UPPER(BV.DATAAREAID) = '{company.upper()}'"  if company else ""
    sql = f"""
    WITH
    CTE_Weights AS (
      SELECT 12 AS w1, 11 AS w2, 10 AS w3,  9 AS w4,
              8 AS w5,  7 AS w6,  6 AS w7,  5 AS w8,
              4 AS w9,  3 AS w10, 2 AS w11, 1 AS w12, 78 AS total_weight
    ),
    CTE_Sales_History AS (
      SELECT ol.itemid, ol.dataareaid,
        DATE_TRUNC('month', CAST(ol.shippingdaterequested AS DATE)) AS sales_month,
        MONTH(CAST(ol.shippingdaterequested AS DATE))               AS cal_month,
        DATEDIFF('month',
          DATE_TRUNC('month', CAST(ol.shippingdaterequested AS DATE)),
          DATE_TRUNC('month', CURRENT_DATE()))                      AS months_ago,
        SUM(ol.salesqty)                                            AS monthly_qty
      FROM FLS_PROD_DB.MART_DYN_FO.ORDER_LINES ol
      WHERE ol.salesstatus NOT IN (4)
        {co_filter_ol}
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
          WHEN h.months_ago=1  THEN h.monthly_qty*w.w1  WHEN h.months_ago=2  THEN h.monthly_qty*w.w2
          WHEN h.months_ago=3  THEN h.monthly_qty*w.w3  WHEN h.months_ago=4  THEN h.monthly_qty*w.w4
          WHEN h.months_ago=5  THEN h.monthly_qty*w.w5  WHEN h.months_ago=6  THEN h.monthly_qty*w.w6
          WHEN h.months_ago=7  THEN h.monthly_qty*w.w7  WHEN h.months_ago=8  THEN h.monthly_qty*w.w8
          WHEN h.months_ago=9  THEN h.monthly_qty*w.w9  WHEN h.months_ago=10 THEN h.monthly_qty*w.w10
          WHEN h.months_ago=11 THEN h.monthly_qty*w.w11 WHEN h.months_ago=12 THEN h.monthly_qty*w.w12
          ELSE 0
        END) / NULLIF(w.total_weight, 0), 4) AS wma_baseline,
        COUNT(DISTINCT h.sales_month) AS months_with_data
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
        COALESCE(s1.seasonal_index,1.0) AS SI_JAN, COALESCE(s2.seasonal_index,1.0) AS SI_FEB,
        COALESCE(s3.seasonal_index,1.0) AS SI_MAR, COALESCE(s4.seasonal_index,1.0) AS SI_APR,
        COALESCE(s5.seasonal_index,1.0) AS SI_MAY, COALESCE(s6.seasonal_index,1.0) AS SI_JUN,
        COALESCE(s7.seasonal_index,1.0) AS SI_JUL, COALESCE(s8.seasonal_index,1.0) AS SI_AUG,
        COALESCE(s9.seasonal_index,1.0) AS SI_SEP, COALESCE(s10.seasonal_index,1.0) AS SI_OCT,
        COALESCE(s11.seasonal_index,1.0) AS SI_NOV, COALESCE(s12.seasonal_index,1.0) AS SI_DEC
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
      INNER JOIN FLS_PROD_DB.MART_DYN_FO.BOM_LINES BL ON BV.BOMID=BL.BOMID AND BV.DATAAREAID=BL.DATAAREAID
      WHERE BV.ACTIVE=1 {co_filter_bom}
      UNION ALL
      SELECT P.LVL+1, P.ROOT_ITEMID, P.ROOT_DATAAREAID, BL.ITEMID, BL.DATAAREAID,
        P.TOTAL_QTY_PATH*BL.BOMQTY, P.BOM_PATH||' > '||BL.ITEMID
      FROM BOM_Explosion P
      INNER JOIN FLS_PROD_DB.MART_DYN_FO.BOM_VERSIONS BV
        ON BV.ITEMID=P.CHILD_ITEMID AND BV.DATAAREAID=P.CHILD_DATAAREAID AND BV.ACTIVE=1
      INNER JOIN FLS_PROD_DB.MART_DYN_FO.BOM_LINES BL ON BV.BOMID=BL.BOMID AND BL.DATAAREAID=BV.DATAAREAID
      WHERE P.LVL < 7
    ),
    CTE_Component_2026 AS (
      SELECT e.CHILD_ITEMID AS COMPONENT_ITEMID, e.CHILD_DATAAREAID AS COMPONENT_DATAAREAID,
        e.ROOT_ITEMID, e.LVL AS BOM_LEVEL, e.BOM_PATH,
        e.TOTAL_QTY_PATH AS QTY_PER_ROOT_UNIT, f.wma_baseline AS ROOT_WMA, f.months_with_data,
        ROUND(f.F_JAN*e.TOTAL_QTY_PATH,2) AS C_JAN, ROUND(f.F_FEB*e.TOTAL_QTY_PATH,2) AS C_FEB,
        ROUND(f.F_MAR*e.TOTAL_QTY_PATH,2) AS C_MAR, ROUND(f.F_APR*e.TOTAL_QTY_PATH,2) AS C_APR,
        ROUND(f.F_MAY*e.TOTAL_QTY_PATH,2) AS C_MAY, ROUND(f.F_JUN*e.TOTAL_QTY_PATH,2) AS C_JUN,
        ROUND(f.F_JUL*e.TOTAL_QTY_PATH,2) AS C_JUL, ROUND(f.F_AUG*e.TOTAL_QTY_PATH,2) AS C_AUG,
        ROUND(f.F_SEP*e.TOTAL_QTY_PATH,2) AS C_SEP, ROUND(f.F_OCT*e.TOTAL_QTY_PATH,2) AS C_OCT,
        ROUND(f.F_NOV*e.TOTAL_QTY_PATH,2) AS C_NOV, ROUND(f.F_DEC*e.TOTAL_QTY_PATH,2) AS C_DEC,
        f.SI_JAN,f.SI_FEB,f.SI_MAR,f.SI_APR,f.SI_MAY,f.SI_JUN,
        f.SI_JUL,f.SI_AUG,f.SI_SEP,f.SI_OCT,f.SI_NOV,f.SI_DEC
      FROM BOM_Explosion e
      INNER JOIN CTE_Forecast_2026 f ON f.itemid=e.ROOT_ITEMID AND f.dataareaid=e.ROOT_DATAAREAID
    )
    SELECT
      cm.COMPONENT_ITEMID AS component_itemid,
      COALESCE(tr.NAME, cm.COMPONENT_ITEMID) AS part_name,
      cm.COMPONENT_DATAAREAID AS company,
      cm.BOM_LEVEL AS bom_level, cm.BOM_PATH AS bom_path,
      cm.QTY_PER_ROOT_UNIT AS qty_per_root_unit,
      cm.ROOT_WMA AS wma_baseline, cm.months_with_data,
      SUM(cm.C_JAN) AS jan_2026, SUM(cm.C_FEB) AS feb_2026,
      SUM(cm.C_MAR) AS mar_2026, SUM(cm.C_APR) AS apr_2026,
      SUM(cm.C_MAY) AS may_2026, SUM(cm.C_JUN) AS jun_2026,
      SUM(cm.C_JUL) AS jul_2026, SUM(cm.C_AUG) AS aug_2026,
      SUM(cm.C_SEP) AS sep_2026, SUM(cm.C_OCT) AS oct_2026,
      SUM(cm.C_NOV) AS nov_2026, SUM(cm.C_DEC) AS dec_2026,
      SUM(cm.C_JAN+cm.C_FEB+cm.C_MAR+cm.C_APR+cm.C_MAY+cm.C_JUN
        +cm.C_JUL+cm.C_AUG+cm.C_SEP+cm.C_OCT+cm.C_NOV+cm.C_DEC) AS annual_2026,
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
    LIMIT {limit}
    """
    return run_query(sql)


# ── Demand Forecast ─────────────────────────────────────────────────────────

@app.get("/api/demand-forecast")
def get_demand_forecast(
    company: Optional[str] = None,
    model_id: Optional[str] = None,
    item_id: Optional[str] = None,
    limit: int = 1000,
):
    filters = "WHERE ACTIVE = 1 AND SALESQTY > 0"
    if company:
        filters += f" AND DATAAREAID = '{company}'"
    if model_id:
        filters += f" AND MODELID = '{model_id}'"
    if item_id:
        filters += f" AND ITEMID = '{item_id}'"
    sql = f"""
        SELECT
            DATAAREAID      AS company,
            ITEMID          AS item_id,
            ITEMDESCRIPTION AS item_name,
            MODELID         AS model_id,
            CUSTACCOUNTID   AS customer_id,
            STARTDATE       AS forecast_date,
            SALESQTY        AS forecast_qty,
            SALESPRICE      AS unit_price,
            AMOUNT          AS forecast_amount,
            CURRENCY        AS currency,
            SALESUNITID     AS unit,
            CREATEDON       AS created_on
        FROM DEMAND_FORECAST
        {filters}
        ORDER BY STARTDATE DESC
        LIMIT {limit}
    """
    return run_query(sql)


@app.get("/api/demand-forecast/by-month")
def get_forecast_by_month(company: Optional[str] = None, model_id: Optional[str] = None):
    filters = "WHERE ACTIVE = 1 AND SALESQTY > 0 AND STARTDATE > '2024-01-01'"
    if company:
        filters += f" AND DATAAREAID = '{company}'"
    if model_id:
        filters += f" AND MODELID = '{model_id}'"
    sql = f"""
        SELECT
            DATAAREAID AS company,
            MODELID    AS model_id,
            DATE_TRUNC('month', TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF7')) AS month,
            SUM(SALESQTY)  AS total_forecast_qty,
            SUM(AMOUNT)    AS total_forecast_amount,
            COUNT(DISTINCT ITEMID) AS distinct_items
        FROM DEMAND_FORECAST
        {filters}
        GROUP BY company, model_id, month
        ORDER BY month
    """
    return run_query(sql)


# ── Slow-Moving Items ───────────────────────────────────────────────────────

@app.get("/api/slow-moving")
def get_slow_moving(company: Optional[str] = None, limit: int = 1000):
    co_filter = f"AND UPPER(it.DATAAREAID) = '{company.upper()}'" if company else ""
    sql = f"""
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
        {co_filter}
        GROUP BY it.DATAAREAID, it.ITEMID, id.INVENTLOCATIONID
    ),
    classified AS (
        SELECT *,
            CASE
                WHEN last_physical_move IS NULL         THEN 'Never moved'
                WHEN days_since_last_move > 365         THEN 'Non-moving (>1yr)'
                WHEN days_since_last_move > 180
                  OR tx_last_365_days < 3               THEN 'Slow-moving'
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
    LIMIT {limit}
    """
    return run_query(sql)


# ── Purchase Orders ─────────────────────────────────────────────────────────

@app.get("/api/purchase-orders/open")
def get_open_purchase_orders(company: Optional[str] = None, limit: int = 1000):
    company_filter = f"AND UPPER(S.DATAAREAID) = UPPER('{company}')" if company else ""
    sql = f"""
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
    POH.VENDORNAME                                                          AS vendor_name,
    POH.INVENTLOCATIONID                                                    AS stockroom,
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
             AND MGL_PS.PURCHASE_ORDER_LINE_PURCH_STATUS IN ('Invoiced','Received')
             THEN NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01')
        WHEN POL.DLVTERM LIKE 'D%'
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
                CASE WHEN (POL.DLVTERM LIKE 'F%' OR POL.DLVTERM LIKE 'C%') THEN NULLIF(LEFT(POL.REQUESTEDSHIPDATE,10),'1900-01-01')
                     WHEN (POL.DLVTERM LIKE 'D%') THEN NULLIF(LEFT(POL.DELIVERYDATE,10),'1900-01-01') END,
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
        SELECT entity_hk FROM FLS_PROD_DB.EDW_META.ENTITY_HUB WHERE entity = 'purchline'
    )
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_PS ON POL.PURCHSTATUS = MGL_PS.OPTION
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS PURCHASE_ORDER_PURCH_STATUS
    FROM FLS_PROD_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    WHERE s.ENTITY_GLOBALSET_LINK_HK IN (
        SELECT entity_hk FROM FLS_PROD_DB.EDW_META.ENTITY_HUB WHERE entity = 'purchTable'
    )
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_PHS ON POH.PURCHSTATUS = MGL_PHS.OPTION
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS FLSINSPECTIONFLAG
    FROM FLS_PROD_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_FG ON POL.FLSINSPECTIONFLAG = MGL_FG.OPTION
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS PURCHASE_ORDER_DOCUMENT_STATE
    FROM FLS_PROD_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_DSTATE ON POH.DOCUMENTSTATE = MGL_DSTATE.OPTION
WHERE POL.PURCHSTATUS IN (1, 2, 3) {company_filter}
GROUP BY ALL
ORDER BY po_created_date DESC
LIMIT {limit}
    """
    return run_query(sql)



@app.get("/api/purchase-orders/summary")
def get_po_summary():
    sql = """
        SELECT
            POL.DATAAREAID AS company,
            COALESCE(MGL_PS.purch_status, CAST(POL.PURCHSTATUS AS VARCHAR)) AS status,
            COUNT(DISTINCT POL.PURCHID) AS po_count,
            COUNT(*) AS line_count,
            SUM(POL.RemainPurchPhysical * POL.PURCHPRICE) AS open_value
        FROM PURCHASE_ORDER_LINE POL
        LEFT JOIN (
            SELECT s.option, s.localizedlabel AS purch_status
            FROM FLS_PROD_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
            WHERE s.ENTITY_GLOBALSET_LINK_HK IN (
                SELECT entity_globalset_link_hk FROM FLS_PROD_DB.EDW_META.ENTITY_HUB WHERE entity = 'purchline'
            )
            QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
        ) MGL_PS ON POL.PURCHSTATUS = MGL_PS.OPTION
        WHERE POL.PURCHSTATUS IN (1, 2, 3)
        GROUP BY POL.DATAAREAID, MGL_PS.purch_status, POL.PURCHSTATUS
        ORDER BY company, status
    """
    return run_query(sql)


# ── Sales Orders ─────────────────────────────────────────────────────────────

@app.get("/api/sales-orders/pipeline")
def get_sales_pipeline(company: Optional[str] = None, limit: int = 800):
    company_filter = f"AND UPPER(S.DATAAREAID) = UPPER('{company}')" if company else ""
    sql = f"""
SELECT
    DCS.BUSINESSUNITVALUE                                                   AS business_unit,
    S.DATAAREAID                                                            AS company,
    S.NAME                                                                  AS site_name,
    DCS.DESTINATIONVALUE                                                    AS destination,
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
    I.PrimaryVendorId                                                       AS primary_supplier_id,
    DS.SUPPLIER_NAME                                                        AS supplier_name,
    CPSL.QTY                                                                AS delivered_qty,
    (TO_DECIMAL(SOL.SALESPRICE, 38, 6) * NVL(CPSL.QTY, 0))                 AS invoiced_line_amount,
    SOL.CURRENCYCODE                                                        AS currency,
    SOL.DLVTERM                                                             AS inco_term,
    SOL.SALESUNIT                                                           AS uom,
    EA.MOCCODE                                                              AS moc,
    EA.PRODUCTTYPOLOGY                                                      AS product_typology,
    DCS.OFFERINGTYPEVALUE                                                   AS order_typology,
    SO.CustAccount                                                          AS customer_number,
    SO.SALESNAME                                                            AS customer_name,
    AD.COUNTRYREGIONID                                                      AS country,
    I.ITEMBUYERGROUPID                                                      AS part_expeditor,
    BG.DESCRIPTION                                                          AS part_expeditor_name,
    MGL_SOL.sales_order_sales_status                                        AS sales_status,
    MGL_SO.sales_order_header_sales_status                                  AS sales_header_status,
    MGL_ST.sales_order_document_status                                      AS doc_status,
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
             AND COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'), NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01')) IS NOT NULL
             AND DATEDIFF(DAY,
                    NULLIF(LEFT(CPSL.CREATEDDATETIME,10),'1900-01-01')::DATE,
                    COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'), NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01'))::DATE
                ) >= 0 THEN 'ON-TIME'
        WHEN COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'), NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01')) <= CURRENT_DATE()::VARCHAR
             AND NULLIF(LEFT(CPSL.CREATEDDATETIME,10),'1900-01-01') IS NULL THEN 'LATE'
        WHEN COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'), NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01')) IS NULL THEN 'N/A'
        ELSE 'LATE'
    END                                                                     AS line_status,
    DATEDIFF(DAY,
        COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'), NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01'))::DATE,
        NULLIF(LEFT(CPSL.CREATEDDATETIME,10),'1900-01-01')::DATE
    )                                                                       AS date_difference,
    CASE
        WHEN COALESCE(NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01'), NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01')) <= TO_VARCHAR(CURRENT_DATE())
        THEN 'INCLUDE' ELSE 'EXCLUDE'
    END                                                                     AS difot_exclusion,
    CASE WHEN CONTAINS(UPPER(SOL.DELIVERYNAME), 'FLS') THEN 'INTERNAL' ELSE 'EXTERNAL' END AS delivery_supplier_type
FROM FLS_PROD_DB.MART_DYN_FO.SALES_ORDERS SO
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ORDER_LINES SOL ON SO.SALESID = SOL.SALESID AND SO.DATAAREAID = SOL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEMS I ON SOL.ITEMID = I.ITEMID AND SOL.DATAAREAID = I.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.CUSTOMER_PACKING_SLIP_LINES CPSL ON SOL.SALESID = CPSL.SALESID AND SOL.LINENUM = CPSL.LINENUM AND SO.DATAAREAID = CPSL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.CUSTOMER_INVOICE_LINES CIL ON SOL.SALESID = CIL.SALESID AND SOL.LINENUM = CIL.LINENUM AND SOL.DATAAREAID = CIL.DATAAREAID AND SOL.ITEMID = CIL.ITEMID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ENOVIA_ATTRIBUTES EA ON I.PRODUCT = EA.ECORESPRODUCT
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DIMENSION_CODE_SET DCS ON SOL.DEFAULTDIMENSION = DCS.RECID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.BUYER_GROUPS BG ON I.ITEMBUYERGROUPID = BG._GROUP
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ADDRESSES AD ON SO.DELIVERYPOSTALADDRESS = AD.RECID
LEFT JOIN FLS_DEV_DB.MART_DYN_FO.MOC_AM_CAP MAC ON REGEXP_SUBSTR(EA.MOCCODE, '\\d*\\.?\\d+') = MAC."ROW LABELS"
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SITE S ON UPPER(SO.DATAAREAID) = UPPER(S.DATAAREAID)
   AND UPPER(CASE WHEN SO.DATAAREAID = 'sa1' THEN 'SAUDI ARABIA' ELSE SO.INVENTSITEID END) = UPPER(S.NAME)
LEFT JOIN FLS_PROD_DB.RAW_SHAREPOINT_CI.COUNTRY_REGION CR ON DCS.DESTINATIONVALUE = CR.CUSTOMER_COUNTRY_CODE
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DIM_SUPPLIER DS ON I.PRIMARYVENDORID = DS.SUPPLIER_NUMBER
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS sales_order_sales_status
    FROM FLS_DEV_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    WHERE s.ENTITY_GLOBALSET_LINK_HK IN (SELECT entity_hk FROM FLS_DEV_DB.EDW_META.ENTITY_HUB WHERE entity = 'salesline')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_SOL ON SOL.SALESSTATUS = MGL_SOL.OPTION
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS sales_order_header_sales_status
    FROM FLS_DEV_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    WHERE s.ENTITY_GLOBALSET_LINK_HK IN (SELECT entity_hk FROM FLS_DEV_DB.EDW_META.ENTITY_HUB WHERE entity = 'salestable')
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_SO ON SO.SALESSTATUS = MGL_SO.OPTION
LEFT JOIN (
    SELECT s.option, s.localizedlabel AS sales_order_document_status
    FROM FLS_DEV_DB.EDW_META.ENTITY_GLOBALSET_DYNFO_META_MSAT s
    QUALIFY ROW_NUMBER() OVER(PARTITION BY option ORDER BY s.load_datetime DESC) = 1
) MGL_ST ON SO.DOCUMENTSTATUS = MGL_ST.OPTION
WHERE 1=1 {company_filter}
GROUP BY ALL
ORDER BY so_created_date DESC
LIMIT {limit}
    """
    return run_query(sql)


# ── Supply vs Demand Gap ──────────────────────────────────────────────────────

@app.get("/api/supply-demand-gap")
def get_supply_demand_gap(company: Optional[str] = None):
    company_filter = f"AND inv.DATAAREAID = '{company}'" if company else ""
    sql = f"""
        SELECT
            inv.DATAAREAID  AS company,
            inv.ITEMID      AS item_id,
            inv.INVENTSITEID AS site,
            SUM(inv.AVAILPHYSICAL)  AS avail_physical,
            SUM(inv.ONORDER)        AS on_order,
            COALESCE(fc.forecast_qty, 0) AS forecast_demand,
            SUM(inv.AVAILPHYSICAL) + SUM(inv.ONORDER) - COALESCE(fc.forecast_qty, 0) AS gap
        FROM ONHAND_INVENTORY inv
        LEFT JOIN (
            SELECT DATAAREAID, ITEMID, SUM(SALESQTY) AS forecast_qty
            FROM DEMAND_FORECAST
            WHERE ACTIVE = 1
              AND TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF7') BETWEEN CURRENT_DATE AND DATEADD('month', 3, CURRENT_DATE)
            GROUP BY DATAAREAID, ITEMID
        ) fc ON inv.DATAAREAID = fc.DATAAREAID AND inv.ITEMID = fc.ITEMID
        WHERE (inv.AVAILPHYSICAL != 0 OR inv.ONORDER != 0)
        {company_filter}
        GROUP BY inv.DATAAREAID, inv.ITEMID, inv.INVENTSITEID, fc.forecast_qty
        ORDER BY gap ASC
        LIMIT 200
    """
    return run_query(sql)


# ── Meta ──────────────────────────────────────────────────────────────────────

@app.get("/api/meta/companies")
def get_companies():
    sql = "SELECT DISTINCT DATAAREAID AS company FROM ONHAND_INVENTORY ORDER BY company"
    return [r["COMPANY"] for r in run_query(sql)]


@app.get("/api/meta/sites")
def get_sites(company: Optional[str] = None):
    where = f"WHERE DATAAREAID = '{company}'" if company else ""
    sql = f"SELECT DISTINCT DATAAREAID AS company, INVENTSITEID AS site FROM ONHAND_INVENTORY {where} ORDER BY company, site"
    return run_query(sql)


@app.get("/api/meta/forecast-models")
def get_forecast_models():
    sql = "SELECT DISTINCT DATAAREAID AS company, MODELID AS model_id FROM DEMAND_FORECAST WHERE ACTIVE = 1 ORDER BY company, model_id"
    return run_query(sql)


@app.get("/api/ml/forecast")
def ml_forecast(
    item_id: Optional[str] = None,
    company: Optional[str] = None,
    periods: int = 12,
):
    return run_prophet_forecast(item_id=item_id, company=company, periods=periods)


@app.get("/api/expedite")
def get_expedite(
    company: str = "US2",
    warehouse: str = "T01",
    item_id: Optional[str] = None,
    action_status: Optional[str] = None,
    limit: int = 2000,
):
    companies_sql = f"('{company.upper()}')"
    item_filter = f"AND rt.itemid = '{item_id.upper()}'" if item_id else ""
    action_filter = f"AND action_status = '{action_status}'" if action_status else ""

    sql = f"""
WITH
CTE_Net_Requirements AS (
  SELECT *
  FROM (
    SELECT nr.*,
      MAX(nr.planversion) OVER (PARTITION BY nr.itemid, nr.dataareaid) AS max_planversion
    FROM FLS_PROD_DB.MART_DYN_FO.NET_REQUIREMENTS nr
    WHERE UPPER(nr.dataareaid) IN {companies_sql}
      AND nr.IsDelete IS NULL
      AND nr.reftype <> 14
  ) t
  WHERE planversion = max_planversion
),
CTE_Open_Sales_Orders_Lines AS (
  SELECT OL.SALESID, OL.itemid, OL.dataareaid, OL.linecreationsequencenumber, OL.linenum,
    OL.salesstatus, OL.deliveryname AS DeliveryName, dpt.namealias AS CustomerName,
    OL.custaccount, SO.deliverypostaladdress, SO.CUSTOMERREF,
    CAST(OL.createddatetime AS DATE) AS createddatetime, 'Sales' AS source,
    ROW_NUMBER() OVER (PARTITION BY OL.salesid, OL.itemid, OL.dataareaid ORDER BY OL.linenum ASC) AS rn
  FROM FLS_PROD_DB.MART_DYN_FO.ORDER_LINES OL
  LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SALES_ORDERS SO ON OL.salesid = SO.salesid AND OL.dataareaid = SO.dataareaid
  LEFT JOIN FLS_PROD_DB.MART_DYN_FO.CUSTOMERS c ON OL.custaccount = c.accountnum AND OL.dataareaid = c.dataareaid
  LEFT JOIN FLS_PROD_DB.MART_DYN_FO.GLOBAL_ADDRESS_BOOK dpt ON dpt.recid = c.party
  WHERE UPPER(OL.dataareaid) IN {companies_sql}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY OL.salesid, OL.itemid, OL.dataareaid ORDER BY OL.linenum ASC) = 1
),
CTE_Open_Production_Order_Lines AS (
  SELECT PB.Prodid, PB.dataareaid, PB.itemid, PB.linenum, PRO.inventrefid,
    CAST(PRO.createddatetime AS DATE) AS createddate, SO.DELIVERYNAME, SO.CUSTOMERREF,
    PB.backorderstatus AS ProductionStatus, 'Production' AS source,
    ROW_NUMBER() OVER (PARTITION BY PB.Prodid, PB.itemid, PB.dataareaid ORDER BY PB.linenum ASC) AS rn
  FROM FLS_PROD_DB.MART_DYN_FO.PRODUCTION_BOM PB
  LEFT JOIN FLS_PROD_DB.MART_DYN_FO.PRODUCTION_ORDERS PRO ON PRO.PRODID = PB.PRODID AND PRO.dataareaid = PB.dataareaid
  LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SALES_ORDERS SO ON SO.salesid = PRO.inventrefid AND SO.dataareaid = PRO.dataareaid
  WHERE PB.reqplanidsched = 'MP Daily' AND PB.backorderstatus = 1
    AND UPPER(PB.dataareaid) IN {companies_sql}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY PB.Prodid, PB.itemid, PB.dataareaid ORDER BY PB.linenum ASC) = 1
),
CTE_Open_Purchase_Order_Lines AS (
  SELECT PL.purchid, PL.dataareaid, PL.itemid, PL.linenumber, PL.purchstatus,
    CAST(PL.createddatetime AS DATE) AS createddatetime, PL.Purchreqlinerefid,
    PL.Inventtransid, PL.VENDACCOUNT,
    CAST(PL.ConfirmedDlv AS DATE) AS CONFIRMED_RECEIPT_DATE,
    PL.FLSEXPEDITESTATUS AS EXPEDITE_STATUS,
    DPT.name AS VendName, DR.Notes, DR.Name AS RefName,
    ORD.ORDERER, REQ.REQUESTER AS EXPEDITOR,
    PL.currencycode, PL.CONFIRMEDSHIPDATE, PL.PURCHREQLINEREFID,
    PL.purchprice, PL.recid, V.party, DPT.recid AS DPTRecid,
    'Purchase' AS source
  FROM FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDER_LINE PL
  LEFT JOIN (
    SELECT PO.PURCHID, PO.dataareaid, CONCAT(PN.FIRSTNAME, PN.LASTNAME) AS ORDERER
    FROM FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDERS PO
    JOIN FLS_PROD_DB.MART_DYN_FO.WORKER W ON PO.WORKERPURCHPLACER = W.RECID
    JOIN FLS_PROD_DB.MART_DYN_FO.PERSON_NAME PN ON PN.PERSON = W.PERSON
    WHERE VALIDTO >= CURRENT_DATE()
  ) ORD ON ORD.PURCHID = PL.PURCHID AND ORD.dataareaid = PL.dataareaid
  LEFT JOIN (
    SELECT PO.PURCHID, PO.dataareaid, CONCAT(PN.FIRSTNAME, PN.LASTNAME) AS REQUESTER
    FROM FLS_PROD_DB.MART_DYN_FO.PURCHASE_ORDERS PO
    JOIN FLS_PROD_DB.MART_DYN_FO.WORKER W ON PO.Requester = W.RECID
    JOIN FLS_PROD_DB.MART_DYN_FO.PERSON_NAME PN ON PN.PERSON = W.PERSON
    WHERE VALIDTO >= CURRENT_DATE()
  ) REQ ON REQ.PURCHID = PL.PURCHID AND REQ.dataareaid = PL.dataareaid
  LEFT JOIN FLS_PROD_DB.MART_DYN_FO.VENDORS V ON PL.vendaccount = V.accountnum AND UPPER(PL.dataareaid) = UPPER(V.dataareaid)
  LEFT JOIN FLS_PROD_DB.MART_DYN_FO.GLOBAL_ADDRESS_BOOK DPT ON V.Party = DPT.recid
  LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DOCUMENT_REFERENCES DR
    ON PL.tableid = DR.reftableid AND PL.recid = DR.refrecid AND UPPER(PL.dataareaid) = UPPER(DR.RefCompanyId)
  WHERE PL.linedeliverytype NOT IN (1) AND PL.isdeleted = 0
    AND UPPER(PL.dataareaid) IN {companies_sql}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY PL.PurchId, PL.LineNumber, PL.dataareaid ORDER BY DR.CREATEDDATETIME DESC NULLS LAST) = 1
),
CTE_ITEM_COVERAGE AS (
  SELECT ITEMID, DATAAREAID, MININVENTONHAND AS MIN_ON_HAND, MAXINVENTONHAND AS MAX_ON_HAND,
    REQGROUPID, covinventdimid, EFFECTIVE_FROM
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY ITEMID, DATAAREAID, covinventdimid ORDER BY EFFECTIVE_FROM DESC) AS rn
    FROM FLS_PROD_DB.MART_DYN_FO.ITEM_COVERAGE
    WHERE UPPER(dataareaid) IN {companies_sql}
  ) t WHERE rn = 1
),
CTE_ITO_Dedup AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY recid, dataareaid ORDER BY recid) AS ito_rn
    FROM FLS_PROD_DB.MART_DYN_FO.INVENTORY_TRANSACTIONS_ORIGINATOR
    WHERE UPPER(dataareaid) IN {companies_sql}
  ) t WHERE ito_rn = 1
),
EndPO_ReqDate AS (
  SELECT itemid, dataareaid, MAX(reqdate) AS End_PO_ReqDate
  FROM CTE_Net_Requirements WHERE reftype = 8 GROUP BY itemid, dataareaid
),
EndPO_Qty AS (
  SELECT nr.itemid, nr.dataareaid, COALESCE(SUM(nr.qty), 0) AS End_PO_Qty, epr.End_PO_ReqDate
  FROM CTE_Net_Requirements nr
  JOIN EndPO_ReqDate epr ON nr.itemid = epr.itemid AND nr.dataareaid = epr.dataareaid
  WHERE nr.reftype = 8 AND nr.reqdate = epr.End_PO_ReqDate
  GROUP BY nr.itemid, nr.dataareaid, epr.End_PO_ReqDate
),
CTE_Inventory_Dimensions AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY inventdimid, dataareaid ORDER BY inventdimid) AS id_rn
    FROM FLS_PROD_DB.MART_DYN_FO.INVENTORY_DIMENSIONS
    WHERE UPPER(dataareaid) IN {companies_sql}
  ) t WHERE id_rn = 1
),
CTE_Items AS (
  SELECT * FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY itemid, dataareaid ORDER BY itemid) AS it_rn
    FROM FLS_PROD_DB.MART_DYN_FO.ITEMS
    WHERE UPPER(dataareaid) IN {companies_sql}
  ) t WHERE it_rn = 1
),
BaseData AS (
  SELECT
    ID.inventlocationid, rt.itemid, rt.dataareaid,
    COALESCE(OL.createddatetime, POS.createddatetime, PORDL.createddate) AS PO_SO_Prod_CreationDate,
    rt.reftype AS REFERENCE_TYPE, POS.linenumber, rt.futuresdays,
    rt.reqdate AS REQ_DATE, POS.currencycode, POS.confirmedshipdate,
    rt.qty AS QTY, IC.MIN_ON_HAND, IC.MAX_ON_HAND, rt.covinventdimid,
    rt.IsDelete, rt.recid AS nr_recid,
    SUM(rt.qty) OVER (
      PARTITION BY rt.itemid, rt.dataareaid, rt.covinventdimid
      ORDER BY rt.reqdate,
        CASE rt.reftype WHEN 1 THEN 1 WHEN 10 THEN 2 WHEN 8 THEN 3 WHEN 12 THEN 4 WHEN 9 THEN 5 WHEN 32 THEN 6 ELSE 7 END,
        rt.refid ASC, COALESCE(rt.INVENTTRANSORIGIN, 0) ASC, rt.recid ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS Accumulated,
    POS.VENDACCOUNT AS VEND_ACCT, POS.VendName AS VendorName,
    POS.CONFIRMED_RECEIPT_DATE, POS.EXPEDITE_STATUS, POS.ORDERER, POS.EXPEDITOR,
    POS.NOTES, POS.PurchStatus, rt.INVENTTRANSORIGIN, rt.refid,
    ITO.recid AS itorecid, ITO.itemid AS ITOItemid, POS.party, POS.recid,
    IT.itembuyergroupid, IT.itemtype AS item_itemtype, POS.PurchPrice,
    OL.DeliveryName, OL.CustomerName, OL.CUSTOMERREF, OL.DeliveryPostalAddress,
    PORDL.CUSTOMERREF AS prodordCustomerref, PORDL.DeliveryName AS prodordDeliveryPostalAddress,
    rt.planversion
  FROM CTE_Net_Requirements rt
  LEFT JOIN CTE_Inventory_Dimensions ID ON rt.covinventdimid = ID.inventdimid AND UPPER(rt.dataareaid) = UPPER(ID.dataareaid)
  LEFT JOIN CTE_ITEM_COVERAGE IC ON rt.itemid = IC.itemid AND UPPER(rt.dataareaid) = UPPER(IC.dataareaid) AND rt.covinventdimid = IC.covinventdimid
  LEFT JOIN CTE_Open_Sales_Orders_Lines OL ON rt.refid = OL.salesid AND UPPER(rt.dataareaid) = UPPER(OL.dataareaid) AND rt.itemid = OL.itemid
  LEFT JOIN CTE_ITO_Dedup ITO ON rt.INVENTTRANSORIGIN = ITO.recid AND UPPER(rt.dataareaid) = UPPER(ITO.dataareaid)
  LEFT JOIN CTE_Open_Purchase_Order_Lines POS ON POS.Inventtransid = ITO.INVENTTRANSID AND UPPER(POS.dataareaid) = UPPER(ITO.dataareaid) AND POS.itemid = ITO.itemid
  LEFT JOIN CTE_Open_Production_Order_Lines PORDL ON rt.REFID = PORDL.PRODID AND UPPER(rt.dataareaid) = UPPER(PORDL.dataareaid) AND PORDL.itemid = rt.itemid
  LEFT JOIN CTE_Items IT ON IT.itemid = rt.itemid AND IT.dataareaid = rt.dataareaid
  WHERE (rt.reftype <> 8 OR (rt.reftype = 8 AND POS.Purchstatus = 1))
    AND ID.inventlocationid = '{warehouse.upper()}'
    AND COALESCE(IT.itemtype, 0) = 0
    AND rt.itemid NOT IN ('999805', 'DTOOL-CAPITAL', 'DTOOL-EXPENSE')
    {item_filter}
),
WithPrev AS (
  SELECT bd.*,
    LAG(bd.Accumulated) OVER (
      PARTITION BY bd.itemid, bd.dataareaid, bd.covinventdimid
      ORDER BY bd.REQ_DATE,
        CASE bd.REFERENCE_TYPE WHEN 1 THEN 1 WHEN 10 THEN 2 WHEN 8 THEN 3 WHEN 12 THEN 4 WHEN 9 THEN 5 WHEN 32 THEN 6 ELSE 7 END,
        bd.refid ASC, COALESCE(bd.INVENTTRANSORIGIN, 0) ASC, bd.nr_recid ASC
    ) AS PrevAccumulated,
    MIN(bd.Accumulated) OVER (
      PARTITION BY bd.itemid, bd.dataareaid, bd.covinventdimid
      ORDER BY bd.REQ_DATE,
        CASE bd.REFERENCE_TYPE WHEN 1 THEN 1 WHEN 10 THEN 2 WHEN 8 THEN 3 WHEN 12 THEN 4 WHEN 9 THEN 5 WHEN 32 THEN 6 ELSE 7 END,
        bd.refid ASC, COALESCE(bd.INVENTTRANSORIGIN, 0) ASC, bd.nr_recid ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS MinAccumulated,
    ROW_NUMBER() OVER (
      PARTITION BY bd.itemid, bd.dataareaid, bd.covinventdimid
      ORDER BY bd.REQ_DATE,
        CASE bd.REFERENCE_TYPE WHEN 1 THEN 1 WHEN 10 THEN 2 WHEN 8 THEN 3 WHEN 12 THEN 4 WHEN 9 THEN 5 WHEN 32 THEN 6 ELSE 7 END,
        bd.refid ASC, COALESCE(bd.INVENTTRANSORIGIN, 0) ASC, bd.nr_recid ASC
    ) AS RowSortRank,
    MIN(CASE WHEN bd.Accumulated < 0 THEN bd.REQ_DATE END)
      OVER (PARTITION BY bd.itemid, bd.dataareaid, bd.covinventdimid) AS First_Shortage_Date
  FROM BaseData bd
),
DefaultMIN AS (
  SELECT itemid, dataareaid, lowestqty
  FROM FLS_PROD_DB.EDW_RV.INVENTITEMPURCHSETUP_DYNFO_SAT
  WHERE sequence = 0 AND UPPER(dataareaid) IN {companies_sql}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY itemid, dataareaid ORDER BY MODIFIEDDATETIME DESC) = 1
),
Final AS (
  SELECT
    rt.inventlocationid,
    rt.itemid,
    rt.dataareaid,
    rt.PO_SO_Prod_CreationDate,
    CASE rt.REFERENCE_TYPE
      WHEN 1  THEN 'On-hand'      WHEN 8  THEN 'Purchase order'
      WHEN 9  THEN 'Production'   WHEN 10 THEN 'Sales order'
      WHEN 12 THEN 'Production line' WHEN 14 THEN 'Safety stock'
      WHEN 16 THEN 'Transfer Order'  WHEN 32 THEN 'BOM line'
    END AS REFERENCE_TYPE,
    rt.linenumber, rt.futuresdays, rt.notes, rt.REQ_DATE,
    rt.currencycode, rt.confirmedshipdate, rt.QTY,
    rt.MIN_ON_HAND, rt.MAX_ON_HAND,
    rt.PrevAccumulated, rt.MinAccumulated, rt.Accumulated,
    rt.CUSTOMERREF, rt.deliverypostaladdress,
    rt.prodordCustomerref, rt.prodordDeliveryPostalAddress,
    MAX(rt.REQ_DATE) OVER (PARTITION BY rt.itemid, rt.covinventdimid) AS MaxReqDate,
    rt.planversion,
    COALESCE(ep.End_PO_Qty, 0) AS End_PO_Qty,
    ep.End_PO_ReqDate, rt.First_Shortage_Date,
    CASE
      WHEN rt.REFERENCE_TYPE <> 8 THEN 'No Action'
      WHEN COALESCE(rt.PrevAccumulated, 0) < 0 THEN
        CASE WHEN rt.REQ_DATE > rt.First_Shortage_Date THEN
          CASE WHEN rt.EXPEDITE_STATUS IN ('ARQ','AFRQ','ERQ','RRQ','TRQ','T') THEN 'No Action' ELSE 'Expedite Shortage' END
        ELSE 'No Action' END
      WHEN COALESCE(rt.PrevAccumulated, 0) < COALESCE(rt.MIN_ON_HAND, 0) THEN
        CASE WHEN rt.EXPEDITE_STATUS IS NULL OR rt.EXPEDITE_STATUS IN ('C','O','W','FLSD','SSD') THEN 'ROP Shortage' ELSE 'No Action' END
      WHEN COALESCE(rt.PrevAccumulated, 0) > COALESCE(rt.MAX_ON_HAND, 0) AND COALESCE(rt.MAX_ON_HAND, 0) > 0 THEN
        CASE WHEN (COALESCE(rt.Accumulated,0) - (COALESCE(rt.MAX_ON_HAND,0) + COALESCE(DF.lowestqty,0))) <= COALESCE(rt.QTY,0) THEN 'Decrease'
          ELSE CASE WHEN rt.EXPEDITE_STATUS IN ('O','OT') THEN 'No Action' ELSE 'Cancel' END END
      WHEN (COALESCE(rt.QTY,0) + COALESCE(rt.PrevAccumulated,0)) > COALESCE(rt.MAX_ON_HAND,0) AND COALESCE(rt.MAX_ON_HAND,0) > 0 THEN
        CASE WHEN (COALESCE(rt.Accumulated,0) - (COALESCE(rt.MAX_ON_HAND,0) + COALESCE(DF.lowestqty,0))) <= COALESCE(rt.QTY,0) THEN 'Decrease'
          ELSE CASE WHEN rt.EXPEDITE_STATUS IN ('O','OT') THEN 'No Action' ELSE 'Cancel' END END
      WHEN COALESCE(rt.Accumulated,0) > (COALESCE(rt.MAX_ON_HAND,0) + COALESCE(DF.lowestqty,0)) AND COALESCE(rt.MAX_ON_HAND,0) > 0 THEN
        CASE WHEN (COALESCE(rt.Accumulated,0) - (COALESCE(rt.MAX_ON_HAND,0) + COALESCE(DF.lowestqty,0))) <= COALESCE(rt.QTY,0) THEN 'Decrease'
          ELSE CASE WHEN rt.EXPEDITE_STATUS IN ('O','OT') THEN 'No Action' ELSE 'Cancel' END END
      ELSE 'No Action'
    END AS Action_Status,
    rt.VEND_ACCT, rt.VendorName, rt.CONFIRMED_RECEIPT_DATE,
    rt.EXPEDITE_STATUS, rt.ORDERER, rt.EXPEDITOR, rt.NOTES, rt.PurchStatus,
    rt.INVENTTRANSORIGIN, rt.refid, rt.itembuyergroupid, rt.PurchPrice,
    rt.DeliveryName, rt.CustomerName, DF.lowestqty,
    COALESCE(rt.MAX_ON_HAND,0) + COALESCE(DF.lowestqty,0) AS MAX_PLUS_MOQ,
    COALESCE(rt.Accumulated,0) - (COALESCE(rt.MAX_ON_HAND,0) + COALESCE(DF.lowestqty,0)) AS END_ACC_MINUS_MAX_MOQ
  FROM WithPrev rt
  LEFT JOIN DefaultMIN DF ON rt.itemid = DF.ITEMID AND UPPER(rt.dataareaid) = UPPER(DF.DATAAREAID)
  LEFT JOIN EndPO_Qty ep ON rt.itemid = ep.itemid AND UPPER(rt.dataareaid) = UPPER(ep.dataareaid)
  WHERE UPPER(rt.dataareaid) IN {companies_sql}
    AND rt.inventlocationid = '{warehouse.upper()}'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY rt.itemid, rt.dataareaid, rt.REFERENCE_TYPE, rt.REQ_DATE,
      COALESCE(rt.refid,''), COALESCE(rt.linenumber,0),
      COALESCE(rt.INVENTTRANSORIGIN,0), COALESCE(rt.QTY,0),
      COALESCE(rt.covinventdimid,''), rt.nr_recid
    ORDER BY CASE WHEN rt.VEND_ACCT IS NOT NULL THEN 0 ELSE 1 END ASC, rt.refid ASC
  ) = 1
)
SELECT * FROM Final
WHERE 1=1 {action_filter}
ORDER BY itemid, dataareaid, covinventdimid,
  CASE WHEN REFERENCE_TYPE = 'On-hand' THEN 0 ELSE 1 END,
  REQ_DATE ASC,
  CASE REFERENCE_TYPE WHEN 'Sales order' THEN 2 WHEN 'Purchase order' THEN 3 WHEN 'Production line' THEN 4 WHEN 'Production' THEN 5 WHEN 'BOM line' THEN 6 ELSE 7 END,
  refid ASC
LIMIT {limit}
    """
    return run_query(sql)


# ── AI Chat Assistant ───────────────────────────────────────────────────────

class ChatRequest(BaseModel):
    messages: list[dict]
    company: str = "US2"

@app.post("/api/chat")
def chat(body: ChatRequest):
    return run_chat(body.messages, run_query, body.company)


@app.get("/api/data-quality")
def get_data_quality():
    checks = {}

    # ── Row counts per mart ──────────────────────────────────────────────────
    mart_counts_sql = """
    SELECT 'ONHAND_INVENTORY' AS mart, COUNT(*) AS rows, COUNT(DISTINCT DATAAREAID) AS companies FROM MART_DYN_FO.ONHAND_INVENTORY
    UNION ALL SELECT 'ITEMS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.ITEMS
    UNION ALL SELECT 'PURCHASE_ORDERS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.PURCHASE_ORDERS
    UNION ALL SELECT 'PURCHASE_ORDER_LINE', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.PURCHASE_ORDER_LINE
    UNION ALL SELECT 'SALES_ORDERS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.SALES_ORDERS
    UNION ALL SELECT 'ORDER_LINES', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.ORDER_LINES
    UNION ALL SELECT 'INVENTORY_TRANSACTIONS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.INVENTORY_TRANSACTIONS
    UNION ALL SELECT 'DEMAND_FORECAST', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.DEMAND_FORECAST
    UNION ALL SELECT 'BOM_VERSIONS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.BOM_VERSIONS
    UNION ALL SELECT 'BOM_LINES', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.BOM_LINES
    UNION ALL SELECT 'NET_REQUIREMENTS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM MART_DYN_FO.NET_REQUIREMENTS
    """
    checks["mart_counts"] = run_query(mart_counts_sql)

    # ── Company coverage ─────────────────────────────────────────────────────
    company_coverage_sql = """
    SELECT UPPER(DATAAREAID) AS company,
        COUNT(DISTINCT ITEMID) AS items,
        SUM(AVAILPHYSICAL) AS total_available,
        SUM(PHYSICALINVENT) AS total_on_hand
    FROM MART_DYN_FO.ONHAND_INVENTORY
    GROUP BY UPPER(DATAAREAID)
    ORDER BY company
    """
    checks["company_coverage"] = run_query(company_coverage_sql)

    # ── Completeness: items missing price ────────────────────────────────────
    missing_price_sql = """
    SELECT UPPER(I.DATAAREAID) AS company,
        COUNT(DISTINCT I.ITEMID) AS total_items,
        COUNT(DISTINCT P.ITEMID) AS items_with_price,
        COUNT(DISTINCT I.ITEMID) - COUNT(DISTINCT P.ITEMID) AS missing_price,
        ROUND(100.0 * COUNT(DISTINCT P.ITEMID) / NULLIF(COUNT(DISTINCT I.ITEMID), 0), 1) AS pct_priced
    FROM MART_DYN_FO.ITEMS I
    LEFT JOIN (SELECT DISTINCT ITEMID, DATAAREAID FROM MART_DYN_FO.PRICE WHERE PRICETYPE = '0') P
        ON I.ITEMID = P.ITEMID AND I.DATAAREAID = P.DATAAREAID
    GROUP BY UPPER(I.DATAAREAID)
    ORDER BY company
    """
    checks["missing_price"] = run_query(missing_price_sql)

    # ── Completeness: items missing supplier ─────────────────────────────────
    missing_supplier_sql = """
    SELECT UPPER(DATAAREAID) AS company,
        COUNT(*) AS total_items,
        COUNT(PRIMARYVENDORID) AS items_with_supplier,
        COUNT(*) - COUNT(PRIMARYVENDORID) AS missing_supplier,
        ROUND(100.0 * COUNT(PRIMARYVENDORID) / NULLIF(COUNT(*), 0), 1) AS pct_with_supplier
    FROM MART_DYN_FO.ITEMS
    GROUP BY UPPER(DATAAREAID)
    ORDER BY company
    """
    checks["missing_supplier"] = run_query(missing_supplier_sql)

    # ── Completeness: PO lines missing confirmed ship date ───────────────────
    missing_shipdate_sql = """
    SELECT UPPER(DATAAREAID) AS company,
        COUNT(*) AS total_po_lines,
        COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL AND CAST(CONFIRMEDSHIPDATE AS VARCHAR) NOT LIKE '1900%' THEN 1 END) AS with_confirmed_date,
        COUNT(*) - COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL AND CAST(CONFIRMEDSHIPDATE AS VARCHAR) NOT LIKE '1900%' THEN 1 END) AS missing_date,
        ROUND(100.0 * COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL AND CAST(CONFIRMEDSHIPDATE AS VARCHAR) NOT LIKE '1900%' THEN 1 END) / NULLIF(COUNT(*), 0), 1) AS pct_complete
    FROM MART_DYN_FO.PURCHASE_ORDER_LINE
    WHERE PURCHSTATUS = 1
    GROUP BY UPPER(DATAAREAID)
    ORDER BY company
    """
    checks["missing_shipdate"] = run_query(missing_shipdate_sql)

    # ── Consistency: on-hand items not in item master ────────────────────────
    orphan_items_sql = """
    SELECT UPPER(O.DATAAREAID) AS company,
        COUNT(DISTINCT O.ITEMID) AS onhand_items,
        COUNT(DISTINCT I.ITEMID) AS matched_in_master,
        COUNT(DISTINCT O.ITEMID) - COUNT(DISTINCT I.ITEMID) AS orphan_items
    FROM MART_DYN_FO.ONHAND_INVENTORY O
    LEFT JOIN MART_DYN_FO.ITEMS I ON O.ITEMID = I.ITEMID AND O.DATAAREAID = I.DATAAREAID
    WHERE O.PHYSICALINVENT > 0
    GROUP BY UPPER(O.DATAAREAID)
    ORDER BY company
    """
    checks["orphan_items"] = run_query(orphan_items_sql)

    # ── Freshness: date ranges per key mart ──────────────────────────────────
    freshness_sql = """
    SELECT 'PURCHASE_ORDERS' AS mart,
        MIN(CAST(CREATEDDATETIME AS DATE)) AS oldest_record,
        MAX(CAST(CREATEDDATETIME AS DATE)) AS newest_record,
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
    """
    checks["freshness"] = run_query(freshness_sql)

    # ── On-hand value by company ─────────────────────────────────────────────
    onhand_value_sql = """
    SELECT UPPER(O.DATAAREAID) AS company,
        COUNT(DISTINCT O.ITEMID) AS items_with_stock,
        ROUND(SUM(O.PHYSICALINVENT), 0) AS total_qty,
        COUNT(CASE WHEN P.PRICE IS NOT NULL THEN 1 END) AS priced_lines,
        COUNT(CASE WHEN P.PRICE IS NULL THEN 1 END) AS unpriced_lines,
        ROUND(SUM(O.PHYSICALINVENT * COALESCE(P.PRICE, 0)), 0) AS total_value_local
    FROM MART_DYN_FO.ONHAND_INVENTORY O
    LEFT JOIN (
        SELECT ITEMID, DATAAREAID, PRICE
        FROM MART_DYN_FO.PRICE
        WHERE PRICETYPE = '0'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY ITEMID, DATAAREAID ORDER BY ACTIVATIONDATE DESC) = 1
    ) P ON O.ITEMID = P.ITEMID AND O.DATAAREAID = P.DATAAREAID
    WHERE O.PHYSICALINVENT > 0
    GROUP BY UPPER(O.DATAAREAID)
    ORDER BY company
    """
    checks["onhand_value"] = run_query(onhand_value_sql)

    return checks


@app.get("/health")
def health():
    return {"status": "ok"}
