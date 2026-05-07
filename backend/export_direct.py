# -*- coding: utf-8 -*-
"""
Direct MotherDuck export — no backend needed.
Connects straight to MotherDuck and writes JSON files for Vercel static build.

Usage:
    cd siop-app/backend
    python export_direct.py
"""
import json, os, time
import duckdb
from dotenv import load_dotenv

load_dotenv()

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "frontend", "public", "data")
os.makedirs(OUTPUT, exist_ok=True)

token = os.getenv("MOTHERDUCK_TOKEN")
print("Connecting to MotherDuck...")
conn = duckdb.connect(f"md:siop_db?motherduck_token={token}")
print("Connected.\n")

def save(name, sql, desc=""):
    t0 = time.time()
    label = desc or name
    print(f"  {label}... ", end="", flush=True)
    try:
        df = conn.execute(sql).fetchdf()
        rows = json.loads(df.to_json(orient="records", date_format="iso"))
        path = os.path.join(OUTPUT, f"{name}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, default=str)
        print(f"{len(rows)} rows ({time.time()-t0:.0f}s)")
        return rows
    except Exception as e:
        print(f"FAILED: {e}")
        return []

print("Exporting data...\n")

# 1. Inventory summary
save("inventory_summary", """
    SELECT
        UPPER(DATAAREAID)            AS company,
        INVENTSITEID                 AS site,
        COUNT(DISTINCT ITEMID)       AS distinct_items,
        SUM(ABS(AVAILPHYSICAL))      AS total_avail_physical,
        SUM(ABS(PHYSICALINVENT))     AS total_physical,
        SUM(ABS(ONORDER))            AS total_on_order,
        SUM(ABS(RESERVPHYSICAL))     AS total_reserved
    FROM siop_db.MART_DYN_FO.ONHAND_INVENTORY
    WHERE AVAILPHYSICAL != 0 OR PHYSICALINVENT != 0
    GROUP BY UPPER(DATAAREAID), INVENTSITEID
    ORDER BY company, site
""", "Inventory summary")

# 2. Inventory detail
save("inventory", """
    WITH Prices AS (
        SELECT ITEMID, DATAAREAID, PRICE, UNITID,
               ROW_NUMBER() OVER (PARTITION BY ITEMID, DATAAREAID ORDER BY TRY_CAST(ACTIVATIONDATE AS DATE) DESC) AS rn
        FROM siop_db.MART_DYN_FO.PRICE
        WHERE PRICETYPE = '0'
    )
    SELECT
        I.ITEMID                                        AS item_id,
        PT.DESCRIPTION                                  AS part_description,
        UPPER(I.DATAAREAID)                             AS company,
        OI.INVENTSITEID                                 AS site,
        SUM(OI.PHYSICALINVENT)                          AS on_hand_qty,
        SUM(OI.AVAILPHYSICAL)                           AS available_qty,
        SUM(OI.RESERVPHYSICAL)                          AS reserved_qty,
        SUM(OI.ONORDER)                                 AS on_order_qty,
        P.PRICE                                         AS unit_price,
        P.UNITID                                        AS uom,
        SUM(OI.PHYSICALINVENT) * COALESCE(P.PRICE, 0)  AS onhand_value_local,
        I.PRIMARYVENDORID                               AS primary_supplier,
        DCS.BUSINESSUNITVALUE                           AS business_unit,
        EA.PRODUCTTYPOLOGY                              AS product_typology
    FROM siop_db.MART_DYN_FO.ONHAND_INVENTORY OI
    JOIN siop_db.MART_DYN_FO.ITEMS I ON OI.ITEMID = I.ITEMID AND OI.DATAAREAID = I.DATAAREAID
    LEFT JOIN siop_db.MART_DYN_FO.PRODUCT_TRANSLATIONS PT ON I.PRODUCT = PT.PRODUCT AND PT.LANGUAGEID = 'en-US'
    LEFT JOIN Prices P ON P.ITEMID = I.ITEMID AND P.DATAAREAID = I.DATAAREAID AND P.rn = 1
    LEFT JOIN siop_db.MART_DYN_FO.DIMENSION_CODE_SET DCS ON I.DEFAULTDIMENSION = DCS.RECID
    LEFT JOIN siop_db.MART_DYN_FO.ENOVIA_ATTRIBUTES EA ON I.PRODUCT = EA.ECORESPRODUCT
    WHERE OI.PHYSICALINVENT > 0
    GROUP BY I.ITEMID, PT.DESCRIPTION, I.DATAAREAID, OI.INVENTSITEID, P.PRICE, P.UNITID,
             I.PRIMARYVENDORID, DCS.BUSINESSUNITVALUE, EA.PRODUCTTYPOLOGY
    ORDER BY onhand_value_local DESC NULLS LAST
    LIMIT 500
""", "Inventory detail")

# 3. Forecast by month
save("forecast_by_month", """
    SELECT
        UPPER(DATAAREAID)           AS company,
        MODELID                     AS model_id,
        LEFT(STARTDATE, 7)          AS month,
        SUM(SALESQTY)               AS total_forecast_qty,
        COUNT(DISTINCT ITEMID)      AS distinct_items
    FROM siop_db.MART_DYN_FO.DEMAND_FORECAST
    WHERE ACTIVE = 1 AND STARTDATE >= '2024-01-01'
    GROUP BY UPPER(DATAAREAID), MODELID, LEFT(STARTDATE, 7)
    ORDER BY company, month
""", "Forecast by month")

# 4. Demand forecast detail
save("forecast", """
    SELECT
        UPPER(DATAAREAID)   AS company,
        MODELID             AS model_id,
        ITEMID              AS item_id,
        LEFT(STARTDATE, 10) AS start_date,
        SALESQTY            AS forecast_qty
    FROM siop_db.MART_DYN_FO.DEMAND_FORECAST
    WHERE ACTIVE = 1 AND STARTDATE >= '2025-01-01'
    ORDER BY company, start_date DESC
    LIMIT 500
""", "Demand forecast")

# 5. Purchase orders
save("purchase_orders", """
    SELECT
        POH.PURCHID                                                 AS po_number,
        UPPER(POH.DATAAREAID)                                       AS company,
        POH.ORDERACCOUNT                                            AS vendor_id,
        POH.VENDORREF                                               AS vendor_name,
        POL.ITEMID                                                  AS item_id,
        POL.NAME                                                    AS item_name,
        POH.CURRENCYCODE                                            AS currency,
        LEFT(POH.CREATEDDATETIME, 10)                               AS po_created_date,
        NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01')       AS confirmed_ship_date,
        NULLIF(LEFT(POL.CONFIRMEDDLV, 10), '1900-01-01')            AS confirmed_dlv,
        NULLIF(LEFT(POL.REQUESTEDSHIPDATE, 10), '1900-01-01')       AS requested_ship_date,
        POL.PURCHQTY                                                AS ordered_qty,
        POL.REMAINPURCHPHYSICAL                                     AS remaining_qty,
        POL.PURCHPRICE                                              AS unit_price,
        POL.LINEAMOUNT                                              AS order_price,
        POL.REMAINPURCHPHYSICAL * POL.PURCHPRICE                    AS remaining_value,
        CASE POL.PURCHSTATUS
            WHEN 1 THEN 'Open order' WHEN 2 THEN 'Received'
            WHEN 3 THEN 'Invoiced'   ELSE 'Other' END              AS po_status,
        CASE POH.DOCUMENTSTATE
            WHEN 0 THEN 'Draft' WHEN 1 THEN 'In review'
            WHEN 2 THEN 'Approved' WHEN 3 THEN 'Rejected'
            ELSE 'Unknown' END                                      AS approval_status,
        CASE WHEN NULLIF(LEFT(POL.CONFIRMEDDLV,10),'1900-01-01') < CAST(CURRENT_DATE AS VARCHAR)
             THEN 'YES' ELSE 'NO' END                               AS past_due
    FROM siop_db.MART_DYN_FO.PURCHASE_ORDERS POH
    JOIN siop_db.MART_DYN_FO.PURCHASE_ORDER_LINE POL
        ON POH.PURCHID = POL.PURCHID AND POH.DATAAREAID = POL.DATAAREAID AND POL.ISDELETED = 0
    WHERE POL.PURCHSTATUS IN (1, 2, 3)
    ORDER BY po_created_date DESC
    LIMIT 1000
""", "Purchase orders")

# 6. Sales orders
save("sales_orders", """
    SELECT
        UPPER(SO.DATAAREAID)                                        AS company,
        SO.SALESID                                                  AS sales_order_id,
        SOL.LINENUM                                                 AS line_num,
        LEFT(SO.CREATEDDATETIME, 10)                                AS so_created_date,
        CASE WHEN SO.MCRORDERSTOPPED = 0 THEN 'ACTIVE' ELSE 'HOLD' END AS active_hold,
        SOL.ITEMID                                                  AS item_id,
        SOL.NAME                                                    AS item_name,
        SOL.SALESPRICE                                              AS unit_price,
        SOL.SALESQTY                                                AS ordered_qty,
        SOL.LINEAMOUNT                                              AS so_line_value,
        SOL.CURRENCYCODE                                            AS currency,
        SO.SALESNAME                                                AS customer_name,
        SO.CUSTACCOUNT                                              AS customer_number,
        NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01')  AS shipping_date_confirmed,
        NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED, 10), '1900-01-01')  AS shipping_date_requested,
        COALESCE(
            NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01'),
            NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED, 10), '1900-01-01')
        )                                                           AS promised_date,
        CASE SOL.SALESSTATUS
            WHEN 1 THEN 'Open order' WHEN 2 THEN 'Delivered'
            WHEN 3 THEN 'Invoiced'   WHEN 4 THEN 'Cancelled'
            ELSE CAST(SOL.SALESSTATUS AS VARCHAR) END              AS sales_status,
        EA.PRODUCTTYPOLOGY                                          AS product_typology,
        DCS.BUSINESSUNITVALUE                                       AS business_unit,
        DCS.OFFERINGTYPEVALUE                                       AS order_typology
    FROM siop_db.MART_DYN_FO.SALES_ORDERS SO
    JOIN siop_db.MART_DYN_FO.ORDER_LINES SOL
        ON SO.SALESID = SOL.SALESID AND SO.DATAAREAID = SOL.DATAAREAID
    LEFT JOIN siop_db.MART_DYN_FO.ITEMS I
        ON SOL.ITEMID = I.ITEMID AND SOL.DATAAREAID = I.DATAAREAID
    LEFT JOIN siop_db.MART_DYN_FO.ENOVIA_ATTRIBUTES EA ON I.PRODUCT = EA.ECORESPRODUCT
    LEFT JOIN siop_db.MART_DYN_FO.DIMENSION_CODE_SET DCS ON SOL.DEFAULTDIMENSION = DCS.RECID
    WHERE SOL.SALESSTATUS IN (1, 2, 3)
    ORDER BY so_created_date DESC
    LIMIT 800
""", "Sales orders")

# 7. Supply vs demand gap
save("supply_demand_gap", """
    SELECT
        UPPER(inv.DATAAREAID)           AS company,
        inv.ITEMID                      AS item_id,
        inv.INVENTSITEID                AS site,
        SUM(inv.AVAILPHYSICAL)          AS avail_physical,
        SUM(inv.ONORDER)                AS on_order,
        COALESCE(fc.forecast_qty, 0)    AS forecast_demand,
        SUM(inv.AVAILPHYSICAL) + SUM(inv.ONORDER) - COALESCE(fc.forecast_qty, 0) AS gap
    FROM siop_db.MART_DYN_FO.ONHAND_INVENTORY inv
    LEFT JOIN (
        SELECT DATAAREAID, ITEMID, SUM(SALESQTY) AS forecast_qty
        FROM siop_db.MART_DYN_FO.DEMAND_FORECAST
        WHERE ACTIVE = 1
          AND STARTDATE >= CAST(CURRENT_DATE AS VARCHAR)
          AND STARTDATE <= CAST(CURRENT_DATE + INTERVAL '3 months' AS VARCHAR)
        GROUP BY DATAAREAID, ITEMID
    ) fc ON inv.DATAAREAID = fc.DATAAREAID AND inv.ITEMID = fc.ITEMID
    WHERE inv.AVAILPHYSICAL != 0 OR inv.ONORDER != 0
    GROUP BY inv.DATAAREAID, inv.ITEMID, inv.INVENTSITEID, fc.forecast_qty
    ORDER BY gap ASC
    LIMIT 300
""", "Supply vs demand gap")

# 8. Slow moving
save("slow_moving", """
    WITH movement AS (
        SELECT DATAAREAID, ITEMID,
               MAX(DATEPHYSICAL)   AS last_move,
               COUNT(*)            AS tx_count,
               DATEDIFF('day', MAX(TRY_CAST(DATEPHYSICAL AS DATE)), CURRENT_DATE) AS days_since
        FROM siop_db.MART_DYN_FO.INVENTORY_TRANSACTIONS
        WHERE DATEPHYSICAL > '1900-01-02'
          AND TRY_CAST(DATEPHYSICAL AS DATE) >= CURRENT_DATE - INTERVAL '3 years'
        GROUP BY DATAAREAID, ITEMID
    ),
    stock AS (
        SELECT DATAAREAID, ITEMID,
               SUM(PHYSICALINVENT) AS on_hand_qty,
               SUM(AVAILPHYSICAL)  AS available_qty
        FROM siop_db.MART_DYN_FO.ONHAND_INVENTORY
        GROUP BY DATAAREAID, ITEMID
    ),
    prices AS (
        SELECT ITEMID, DATAAREAID, PRICE,
               ROW_NUMBER() OVER (PARTITION BY ITEMID, DATAAREAID ORDER BY TRY_CAST(ACTIVATIONDATE AS DATE) DESC) AS rn
        FROM siop_db.MART_DYN_FO.PRICE WHERE PRICETYPE = '0'
    )
    SELECT
        UPPER(m.DATAAREAID)     AS company,
        m.ITEMID                AS item_id,
        PT.DESCRIPTION          AS part_description,
        m.last_move             AS last_physical_move,
        m.tx_count              AS tx_last_365_days,
        m.days_since            AS days_since_last_move,
        CASE
            WHEN m.last_move IS NULL        THEN 'Never moved'
            WHEN m.days_since > 365         THEN 'Non-moving (>1yr)'
            WHEN m.days_since > 180         THEN 'Slow-moving'
            ELSE 'Normal'
        END                     AS movement_category,
        COALESCE(s.on_hand_qty, 0)   AS on_hand_qty,
        COALESCE(s.available_qty, 0) AS available_qty,
        p.PRICE                      AS unit_price,
        COALESCE(s.on_hand_qty, 0) * COALESCE(p.PRICE, 0) AS onhand_value_local
    FROM movement m
    JOIN siop_db.MART_DYN_FO.ITEMS I ON m.ITEMID = I.ITEMID AND m.DATAAREAID = I.DATAAREAID
    LEFT JOIN siop_db.MART_DYN_FO.PRODUCT_TRANSLATIONS PT ON I.PRODUCT = PT.PRODUCT AND PT.LANGUAGEID = 'en-US'
    LEFT JOIN stock s ON s.DATAAREAID = m.DATAAREAID AND s.ITEMID = m.ITEMID
    LEFT JOIN prices p ON p.DATAAREAID = m.DATAAREAID AND p.ITEMID = m.ITEMID AND p.rn = 1
    WHERE m.days_since > 180 AND COALESCE(s.on_hand_qty, 0) > 0
    ORDER BY m.days_since DESC
    LIMIT 500
""", "Slow moving items")

# 9. Expedite report
save("expedite", """
    SELECT
        UPPER(NR.DATAAREAID)            AS company,
        NR.ITEMID                       AS item_id,
        PT.DESCRIPTION                  AS part_description,
        NR.REQDATE                      AS requirement_date,
        NR.FUTURESDATE                  AS future_req_date,
        NR.QTY                          AS required_qty,
        NR.ACTIONTYPE                   AS action_type,
        CASE NR.ACTIONTYPE
            WHEN 0 THEN 'No Action'
            WHEN 1 THEN 'Expedite Shortage'
            WHEN 2 THEN 'ROP Shortage'
            WHEN 4 THEN 'Decrease'
            WHEN 9 THEN 'Cancel'
            ELSE 'Other'
        END                             AS action_status,
        NR.REFID                        AS ref_id,
        NR.DIRECTION                    AS direction,
        NR.OPENSTATUS                   AS open_status
    FROM siop_db.MART_DYN_FO.NET_REQUIREMENTS NR
    LEFT JOIN siop_db.MART_DYN_FO.ITEMS I ON NR.ITEMID = I.ITEMID AND NR.DATAAREAID = I.DATAAREAID
    LEFT JOIN siop_db.MART_DYN_FO.PRODUCT_TRANSLATIONS PT ON I.PRODUCT = PT.PRODUCT AND PT.LANGUAGEID = 'en-US'
    WHERE NR.ACTIONTYPE IN (1, 2, 4, 9)
      AND UPPER(NR.DATAAREAID) IN ('US2','ZA4','ZA3','DK1','GH1')
    ORDER BY NR.REQDATE ASC
    LIMIT 2000
""", "Expedite report")

# 10. Data quality
dq = {}

dq["mart_counts"] = save("_dq_mart_counts", """
    SELECT 'ONHAND_INVENTORY' AS mart, COUNT(*) AS rows, COUNT(DISTINCT DATAAREAID) AS companies FROM siop_db.MART_DYN_FO.ONHAND_INVENTORY
    UNION ALL SELECT 'ITEMS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM siop_db.MART_DYN_FO.ITEMS
    UNION ALL SELECT 'PURCHASE_ORDERS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM siop_db.MART_DYN_FO.PURCHASE_ORDERS
    UNION ALL SELECT 'PURCHASE_ORDER_LINE', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM siop_db.MART_DYN_FO.PURCHASE_ORDER_LINE
    UNION ALL SELECT 'SALES_ORDERS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM siop_db.MART_DYN_FO.SALES_ORDERS
    UNION ALL SELECT 'ORDER_LINES', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM siop_db.MART_DYN_FO.ORDER_LINES
    UNION ALL SELECT 'DEMAND_FORECAST', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM siop_db.MART_DYN_FO.DEMAND_FORECAST
    UNION ALL SELECT 'NET_REQUIREMENTS', COUNT(*), COUNT(DISTINCT DATAAREAID) FROM siop_db.MART_DYN_FO.NET_REQUIREMENTS
    ORDER BY mart
""", "Data quality - mart counts")

dq["company_coverage"] = save("_dq_company", """
    SELECT UPPER(DATAAREAID) AS company, COUNT(DISTINCT ITEMID) AS items,
           ROUND(SUM(AVAILPHYSICAL),0) AS total_available, ROUND(SUM(PHYSICALINVENT),0) AS total_on_hand
    FROM siop_db.MART_DYN_FO.ONHAND_INVENTORY GROUP BY UPPER(DATAAREAID) ORDER BY company
""", "Data quality - company coverage")

dq["missing_price"] = save("_dq_price", """
    SELECT UPPER(I.DATAAREAID) AS company, COUNT(DISTINCT I.ITEMID) AS total_items,
           COUNT(DISTINCT P.ITEMID) AS items_with_price,
           COUNT(DISTINCT I.ITEMID) - COUNT(DISTINCT P.ITEMID) AS missing_price,
           ROUND(100.0 * COUNT(DISTINCT P.ITEMID) / NULLIF(COUNT(DISTINCT I.ITEMID),0), 1) AS pct_priced
    FROM siop_db.MART_DYN_FO.ITEMS I
    LEFT JOIN (SELECT DISTINCT ITEMID, DATAAREAID FROM siop_db.MART_DYN_FO.PRICE WHERE PRICETYPE='0') P
        ON I.ITEMID=P.ITEMID AND I.DATAAREAID=P.DATAAREAID
    GROUP BY UPPER(I.DATAAREAID) ORDER BY company
""", "Data quality - missing price")

dq["missing_supplier"] = save("_dq_supplier", """
    SELECT UPPER(DATAAREAID) AS company, COUNT(*) AS total_items,
           COUNT(PRIMARYVENDORID) AS items_with_supplier,
           COUNT(*) - COUNT(PRIMARYVENDORID) AS missing_supplier,
           ROUND(100.0 * COUNT(PRIMARYVENDORID) / NULLIF(COUNT(*),0), 1) AS pct_with_supplier
    FROM siop_db.MART_DYN_FO.ITEMS GROUP BY UPPER(DATAAREAID) ORDER BY company
""", "Data quality - missing supplier")

dq["missing_shipdate"] = save("_dq_shipdate", """
    SELECT UPPER(DATAAREAID) AS company, COUNT(*) AS total_po_lines,
           COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL AND LEFT(CAST(CONFIRMEDSHIPDATE AS VARCHAR),4) != '1900' THEN 1 END) AS with_confirmed_date,
           COUNT(*) - COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL AND LEFT(CAST(CONFIRMEDSHIPDATE AS VARCHAR),4) != '1900' THEN 1 END) AS missing_date,
           ROUND(100.0 * COUNT(CASE WHEN CONFIRMEDSHIPDATE IS NOT NULL AND LEFT(CAST(CONFIRMEDSHIPDATE AS VARCHAR),4) != '1900' THEN 1 END) / NULLIF(COUNT(*),0), 1) AS pct_complete
    FROM siop_db.MART_DYN_FO.PURCHASE_ORDER_LINE WHERE PURCHSTATUS=1
    GROUP BY UPPER(DATAAREAID) ORDER BY company
""", "Data quality - missing ship date")

dq["orphan_items"] = save("_dq_orphans", """
    SELECT UPPER(O.DATAAREAID) AS company,
           COUNT(DISTINCT O.ITEMID) AS onhand_items,
           COUNT(DISTINCT I.ITEMID) AS matched_in_master,
           COUNT(DISTINCT O.ITEMID) - COUNT(DISTINCT I.ITEMID) AS orphan_items
    FROM siop_db.MART_DYN_FO.ONHAND_INVENTORY O
    LEFT JOIN siop_db.MART_DYN_FO.ITEMS I ON O.ITEMID=I.ITEMID AND O.DATAAREAID=I.DATAAREAID
    WHERE O.PHYSICALINVENT > 0 GROUP BY UPPER(O.DATAAREAID) ORDER BY company
""", "Data quality - orphan items")

dq["freshness"] = save("_dq_freshness", """
    SELECT 'PURCHASE_ORDERS' AS mart,
           MIN(LEFT(CREATEDDATETIME,10)) AS oldest_record,
           MAX(LEFT(CREATEDDATETIME,10)) AS newest_record,
           DATEDIFF('day', MAX(TRY_CAST(LEFT(CREATEDDATETIME,10) AS DATE)), CURRENT_DATE) AS days_since_latest
    FROM siop_db.MART_DYN_FO.PURCHASE_ORDERS
    UNION ALL
    SELECT 'SALES_ORDERS',
           MIN(LEFT(CREATEDDATETIME,10)), MAX(LEFT(CREATEDDATETIME,10)),
           DATEDIFF('day', MAX(TRY_CAST(LEFT(CREATEDDATETIME,10) AS DATE)), CURRENT_DATE)
    FROM siop_db.MART_DYN_FO.SALES_ORDERS
    ORDER BY mart
""", "Data quality - freshness")

# Save combined data quality file
with open(os.path.join(OUTPUT, "data_quality.json"), "w", encoding="utf-8") as f:
    json.dump(dq, f, indent=2, default=str)
print("  Data quality combined file saved.")

# Clean up temp files
for name in ["_dq_mart_counts","_dq_company","_dq_price","_dq_supplier","_dq_shipdate","_dq_orphans","_dq_freshness"]:
    p = os.path.join(OUTPUT, f"{name}.json")
    if os.path.exists(p): os.remove(p)

# 11. Speed Up Dashboard — DIFOT & Order-to-Ship using actual goods issue date
save("speed_up", """
    WITH packing AS (
        SELECT
            DATAAREAID,
            SALESID,
            LINENUM,
            MAX(LEFT(DELIVERYDATE, 10))     AS goods_issue_date,
            SUM(QTY)                        AS shipped_qty
        FROM siop_db.MART_DYN_FO.CUSTOMER_PACKING_SLIP_LINES
        WHERE DELIVERYDATE > '1900-01-02'
          AND COALESCE(ISDELETE, 0) = 0
        GROUP BY DATAAREAID, SALESID, LINENUM
    ),
    item_stock AS (
        -- Stock if min or max inventory coverage is set (> 0), else Non Stock
        SELECT
            ITEMID, DATAAREAID,
            CASE WHEN MAX(COALESCE(MININVENTONHAND, 0)) > 0
                      OR MAX(COALESCE(MAXINVENTONHAND, 0)) > 0
                 THEN 'Stock' ELSE 'Non Stock' END AS stock_non_stock
        FROM siop_db.MART_DYN_FO.ITEM_COVERAGE
        GROUP BY ITEMID, DATAAREAID
    )
    SELECT
        UPPER(SO.DATAAREAID)                                         AS company,
        SO.SALESID                                                   AS sales_order_id,
        SOL.LINENUM                                                  AS line_num,
        SOL.ITEMID                                                   AS item_id,
        LEFT(SO.CREATEDDATETIME, 10)                                 AS created_date,
        NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED, 10), '1900-01-01')   AS requested_ship_date,
        NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01')   AS confirmed_ship_date,
        PSL.goods_issue_date                                         AS goods_issue_date,
        SOL.SALESQTY                                                 AS ordered_qty,
        COALESCE(PSL.shipped_qty, 0)                                 AS shipped_qty,
        SOL.LINEAMOUNT                                               AS line_value,
        SOL.CURRENCYCODE                                             AS currency,
        CASE SOL.SALESSTATUS
            WHEN 1 THEN 'Open' WHEN 2 THEN 'Delivered'
            WHEN 3 THEN 'Invoiced' WHEN 4 THEN 'Cancelled'
            ELSE CAST(SOL.SALESSTATUS AS VARCHAR) END                AS sales_status,
        COALESCE(DCS.BUSINESSUNITVALUE, 'Unknown')                   AS business_unit,
        COALESCE(EA.PRODUCTTYPOLOGY, 'Unknown')                      AS product_typology,
        COALESCE(IST.stock_non_stock, 'Non Stock')                   AS stock_non_stock,
        'D365'                                                       AS erp_source,
        NULL                                                         AS business_line,
        NULL                                                         AS offering_type,
        NULL                                                         AS product_line_name,
        NULL                                                         AS reference,
        CASE SOL.INVENTREFTYPE
            WHEN 0 THEN NULL
            WHEN 1 THEN 'Sales order'
            WHEN 2 THEN 'PO'
            WHEN 3 THEN 'Production'
            WHEN 4 THEN 'Production line'
            WHEN 5 THEN 'Inventory journal'
            WHEN 6 THEN 'Sales quotation'
            WHEN 7 THEN 'Transfer order'
            WHEN 8 THEN 'Fixed asset'
            ELSE 'Unknown'
        END                                                          AS execution_type,
        COALESCE(ID.INVENTSITEID, SO.INVENTLOCATIONID)               AS site,
        SO.SALESNAME                                                 AS customer_name,
        -- Order-to-Ship lead time: CreatedDate -> Goods Issue Date (actual ship)
        CASE
            WHEN PSL.goods_issue_date IS NOT NULL
            THEN DATEDIFF('day',
                TRY_CAST(LEFT(SO.CREATEDDATETIME, 10) AS DATE),
                TRY_CAST(PSL.goods_issue_date AS DATE))
            ELSE NULL
        END                                                          AS lead_time_days,
        -- DIFOT: goods issue date <= ConfirmedShippingDate AND shipped >= 95% of ordered
        CASE
            WHEN PSL.goods_issue_date IS NOT NULL
                 AND NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01') IS NOT NULL
                 AND PSL.goods_issue_date <= NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01')
                 AND COALESCE(PSL.shipped_qty, 0) >= SOL.SALESQTY * 0.95
                 THEN 'DIFOT'
            WHEN PSL.goods_issue_date IS NOT NULL
                 AND NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01') IS NOT NULL
                 AND PSL.goods_issue_date > NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01')
                 THEN 'Late'
            WHEN PSL.goods_issue_date IS NOT NULL
                 AND COALESCE(PSL.shipped_qty, 0) < SOL.SALESQTY * 0.95
                 THEN 'Partial'
            WHEN PSL.goods_issue_date IS NULL
                 AND NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01') IS NOT NULL
                 AND LEFT(SOL.SHIPPINGDATECONFIRMED, 10) < CAST(CURRENT_DATE AS VARCHAR)
                 THEN 'Past Due'
            ELSE 'Open'
        END                                                          AS difot_status
    FROM siop_db.MART_DYN_FO.SALES_ORDERS SO
    JOIN siop_db.MART_DYN_FO.ORDER_LINES SOL
        ON SO.SALESID = SOL.SALESID AND SO.DATAAREAID = SOL.DATAAREAID
    LEFT JOIN packing PSL
        ON SO.DATAAREAID = PSL.DATAAREAID
        AND SO.SALESID = PSL.SALESID
        AND SOL.LINENUM = PSL.LINENUM
    LEFT JOIN siop_db.MART_DYN_FO.ITEMS I
        ON SOL.ITEMID = I.ITEMID AND SOL.DATAAREAID = I.DATAAREAID
    LEFT JOIN siop_db.MART_DYN_FO.ENOVIA_ATTRIBUTES EA
        ON I.PRODUCT = EA.ECORESPRODUCT
    LEFT JOIN siop_db.MART_DYN_FO.DIMENSION_CODE_SET DCS
        ON SOL.DEFAULTDIMENSION = DCS.RECID
    LEFT JOIN siop_db.MART_DYN_FO.INVENTORY_DIMENSIONS ID
        ON SOL.INVENTDIMID = ID.INVENTDIMID AND SOL.DATAAREAID = ID.DATAAREAID
    LEFT JOIN item_stock IST
        ON SOL.ITEMID = IST.ITEMID AND SOL.DATAAREAID = IST.DATAAREAID
    WHERE SOL.SALESSTATUS IN (1, 2, 3)
      AND SO.CREATEDDATETIME >= '2023-01-01'
    ORDER BY created_date DESC
    LIMIT 3000
""", "Speed Up - DIFOT / Order-to-Ship (goods issue date)")

# 12. Speed Up Data Quality — missing dates & key fields
save("speed_up_quality", """
    SELECT
        UPPER(SO.DATAAREAID)  AS company,
        COUNT(*)              AS total_lines,
        COUNT(CASE WHEN NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01') IS NULL THEN 1 END) AS missing_requested_date,
        COUNT(CASE WHEN NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01') IS NULL THEN 1 END) AS missing_confirmed_date,
        COUNT(CASE WHEN NULLIF(LEFT(SO.CREATEDDATETIME,10),'1900-01-01') IS NULL THEN 1 END)        AS missing_created_date,
        COUNT(CASE WHEN SOL.SALESQTY IS NULL OR SOL.SALESQTY = 0 THEN 1 END)                        AS missing_qty,
        COUNT(CASE WHEN SOL.SALESSTATUS IS NULL THEN 1 END)                                          AS missing_status,
        COUNT(CASE WHEN SO.SALESNAME IS NULL OR SO.SALESNAME = '' THEN 1 END)                        AS missing_customer,
        ROUND(100.0 * COUNT(CASE WHEN NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED,10),'1900-01-01') IS NOT NULL THEN 1 END) / NULLIF(COUNT(*),0), 1) AS pct_with_confirmed_date,
        ROUND(100.0 * COUNT(CASE WHEN NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED,10),'1900-01-01') IS NOT NULL THEN 1 END) / NULLIF(COUNT(*),0), 1) AS pct_with_requested_date
    FROM siop_db.MART_DYN_FO.SALES_ORDERS SO
    JOIN siop_db.MART_DYN_FO.ORDER_LINES SOL
        ON SO.SALESID = SOL.SALESID AND SO.DATAAREAID = SOL.DATAAREAID
    WHERE SOL.SALESSTATUS IN (1, 2, 3)
      AND SO.CREATEDDATETIME >= '2023-01-01'
    GROUP BY UPPER(SO.DATAAREAID)
    ORDER BY company
""", "Speed Up - Data quality by company")

conn.close()
print("\nAll files saved to frontend/public/data/")
print("Run deploy next.")
