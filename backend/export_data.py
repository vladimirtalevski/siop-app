"""
Export Snowflake data to JSON for the Vercel deployment.
Usage: python3 export_data.py
"""
import json, os
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

def run(name, sql):
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    path = os.path.join(OUTPUT, f"{name}.json")
    with open(path, "w") as f:
        json.dump(rows, f, default=to_json, indent=2)
    print(f"  {name}.json — {len(rows)} rows")
    return rows

conn = get_connection()
print("Connected. Exporting...\n")

# 1. Inventory summary — ABS() on all qty fields
print("[1/7] Inventory summary")
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
""")

# 2. Inventory detail — ABS() on qty, exclude zero rows
print("[2/7] Inventory detail")
run("inventory", """
    SELECT
        DATAAREAID                  AS company,
        ITEMID                      AS item_id,
        INVENTSITEID                AS site,
        INVENTLOCATIONID            AS warehouse,
        INVENTSTATUSID              AS status,
        ABS(AVAILPHYSICAL)          AS avail_physical,
        ABS(AVAILORDERED)           AS avail_ordered,
        ABS(PHYSICALINVENT)         AS physical_qty,
        ABS(ONORDER)                AS on_order,
        ABS(RESERVPHYSICAL)         AS reserved_physical,
        MODIFIEDON                  AS last_updated
    FROM ONHAND_INVENTORY
    WHERE ABS(AVAILPHYSICAL) > 0 OR ABS(PHYSICALINVENT) > 0
    ORDER BY ABS(AVAILPHYSICAL) DESC
    LIMIT 500
""")

# 3. Forecast by month
print("[3/7] Demand forecast by month")
run("forecast_by_month", """
    SELECT
        DATAAREAID  AS company,
        MODELID     AS model_id,
        DATE_TRUNC('month', TRY_TO_DATE(STARTDATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF7')) AS month,
        SUM(SALESQTY)           AS total_forecast_qty,
        SUM(AMOUNT)             AS total_forecast_amount,
        COUNT(DISTINCT ITEMID)  AS distinct_items
    FROM DEMAND_FORECAST
    WHERE ACTIVE = 1
      AND SALESQTY > 0
      AND STARTDATE > '2024-01-01'
    GROUP BY company, model_id, month
    ORDER BY month
""")

# 4. Forecast detail
print("[4/7] Demand forecast detail")
run("forecast", """
    SELECT
        DATAAREAID          AS company,
        ITEMID              AS item_id,
        ITEMDESCRIPTION     AS item_name,
        MODELID             AS model_id,
        CUSTACCOUNTID       AS customer_id,
        STARTDATE           AS forecast_date,
        SALESQTY            AS forecast_qty,
        SALESPRICE          AS unit_price,
        AMOUNT              AS forecast_amount,
        CURRENCY            AS currency,
        SALESUNITID         AS unit
    FROM DEMAND_FORECAST
    WHERE ACTIVE = 1 AND SALESQTY > 0
    ORDER BY STARTDATE DESC
    LIMIT 500
""")

# 5. Purchase orders — proper PROD view (SIFOT, lead times, promised date, LINEAMOUNT)
print("[5/7] Purchase orders (PROD business view)")
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
    POH.VENDORNAME                                                          AS vendor_name,
    POH.INVENTLOCATIONID                                                    AS stockroom,
    POL.ITEMID                                                              AS item_id,
    POL.NAME                                                                AS item_name,
    S.DATAAREAID                                                            AS company,
    S.NAME                                                                  AS site_name,
    POH.CURRENCYCODE                                                        AS currency,

    -- Dates
    TRY_TO_DATE(LEFT(POH.CREATEDDATETIME, 10))                              AS po_created_date,
    TRY_TO_DATE(LEFT(POL.CREATEDDATETIME, 10))                              AS po_line_created_date,
    NULLIF(LEFT(POL.REQUESTEDSHIPDATE, 10), '1900-01-01')                   AS requested_ship_date,
    NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01')                   AS confirmed_ship_date,
    NULLIF(LEFT(POL.DELIVERYDATE, 10), '1900-01-01')                        AS due_date,
    NULLIF(LEFT(RECE_QTY.DELIVERYDATE, 10), '1900-01-01')                   AS delivery_date,
    NULLIF(LEFT(POL.CONFIRMEDDLV, 10), '1900-01-01')                        AS confirmed_dlv,

    -- Promised date (DLVTERM-based)
    COALESCE(
        CASE WHEN (POL.DLVTERM LIKE 'F%' OR POL.DLVTERM LIKE 'C%')
                  THEN NULLIF(LEFT(POL.REQUESTEDSHIPDATE, 10), '1900-01-01')
             WHEN (POL.DLVTERM LIKE 'D%')
                  THEN NULLIF(LEFT(POL.DELIVERYDATE, 10), '1900-01-01')
        END,
        NULLIF(LEFT(POL.REQUESTEDSHIPDATE, 10), '1900-01-01')
    )                                                                       AS promised_date,

    -- PO Fulfillment Date
    CASE
        WHEN (POL.DLVTERM LIKE 'F%' OR POL.DLVTERM LIKE 'C%')
             AND MGL_PS.PURCHASE_ORDER_LINE_PURCH_STATUS IN ('Invoiced', 'Received')
             THEN NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01')
        WHEN (POL.DLVTERM LIKE 'D%')
             THEN NULLIF(LEFT(RECE_QTY.DELIVERYDATE, 10), '1900-01-01')
        ELSE NULL
    END                                                                     AS po_fulfillment_date,

    -- Lead times
    DATEDIFF(DAY, TRY_TO_DATE(LEFT(POH.CREATEDDATETIME, 10)),
        TRY_TO_DATE(NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01'))
    )                                                                       AS actual_supplier_lead_time_header,
    DATEDIFF(DAY, TRY_TO_DATE(LEFT(POL.CREATEDDATETIME, 10)),
        TRY_TO_DATE(NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01'))
    )                                                                       AS supplier_production_lead_time,
    DATEDIFF(DAY,
        TRY_TO_DATE(NULLIF(LEFT(POL.CONFIRMEDSHIPDATE, 10), '1900-01-01')),
        TRY_TO_DATE(LEFT(POL.CONFIRMEDDLV, 10))
    )                                                                       AS shipping_lead_time,
    DATEDIFF(DAY, TRY_TO_DATE(LEFT(POL.CREATEDDATETIME, 10)),
        TRY_TO_DATE(LEFT(POL.CONFIRMEDDLV, 10))
    )                                                                       AS sifot_lead_time,

    -- Quantities & values
    POL.PURCHQTY                                                            AS ordered_qty,
    RECE_QTY.RECEIVED_QTY                                                   AS received_qty,
    POL.REMAINPURCHPHYSICAL                                                 AS remaining_qty,
    POL.PURCHPRICE                                                          AS unit_price,
    POL.LINEAMOUNT                                                          AS order_price,
    (RECE_QTY.RECEIVED_QTY * POL.PURCHPRICE)                               AS received_value,
    (POL.REMAINPURCHPHYSICAL * POL.PURCHPRICE)                              AS remaining_value,

    -- Statuses
    MGL_PHS.PURCHASE_ORDER_PURCH_STATUS                                     AS po_header_status,
    MGL_PS.PURCHASE_ORDER_LINE_PURCH_STATUS                                 AS po_status,
    MGL_DSTATE.PURCHASE_ORDER_DOCUMENT_STATE                                AS approval_status,
    MGL_FG.FLSINSPECTIONFLAG                                                AS inspection_flag,

    -- Past due
    CASE WHEN POL.FLSEXPEDITESTATUS IN ('T', 'K')
              AND MGL_PS.PURCHASE_ORDER_LINE_PURCH_STATUS = 'Open order'
              AND DATEADD(DAY, 3, TRY_TO_DATE(LEFT(POL.CONFIRMEDDLV, 10))) < CURRENT_DATE()
         THEN 'YES' ELSE 'NO'
    END                                                                     AS past_due,

    -- SIFOT exclusion (promised_date inlined to avoid lateral alias issues)
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
   AND UPPER(COALESCE(NULLIF(POH.DATAAREAID, 'sa1'), POH.INVENTSITEID)) = UPPER(S.SITEID)
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

WHERE POL.PURCHSTATUS IN (1, 2, 3)
GROUP BY ALL
ORDER BY po_created_date DESC
LIMIT 1000
""")

# 6. Sales order lines — full business view with DIFOT logic
print("[6/7] Sales order lines (business view)")
run("sales_orders", """
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
    SO.PurchOrderFormNum                                                    AS aurora_po_number,
    SO.CustomerRef                                                          AS aurora_so_number,
    SOL.ItemId                                                              AS item_id,
    SOL.NAME                                                                AS item_name,
    TO_DECIMAL(SOL.SALESPRICE, 38, 6)                                       AS unit_price,
    SOL.SALESQTY                                                            AS ordered_qty,
    LEFT(SOL.RECEIPTDATECONFIRMED, 10)                                      AS receipt_date_confirmed,
    SOL.LINEAMOUNT                                                          AS so_line_value,
    SOL.INVENTREFID                                                         AS supply_doc_id,
    I.PrimaryVendorId                                                       AS primary_supplier_id,
    DS.SUPPLIER_NAME                                                        AS supplier_name,
    CPSL.QTY                                                                AS delivered_qty,
    (TO_DECIMAL(SOL.SALESPRICE, 38, 6) * NVL(CPSL.QTY, 0))                 AS invoiced_line_amount,
    SOL.CURRENCYCODE                                                        AS currency,
    SOL.DLVTERM                                                             AS inco_term,
    SOL.SALESUNIT                                                           AS uom,
    SO.INVENTLOCATIONID                                                     AS stockroom,
    EA.MOCCODE                                                              AS moc,
    EA.PRODUCTTYPOLOGY                                                      AS product_typology,
    DCS.OFFERINGTYPEVALUE                                                   AS order_typology,
    SO.CustAccount                                                          AS customer_number,
    SO.SALESNAME                                                            AS customer_name,
    SOL.DELIVERYNAME                                                        AS delivery_name,
    AD.COUNTRYREGIONID                                                      AS country,
    I.ITEMBUYERGROUPID                                                      AS part_expeditor,
    BG.DESCRIPTION                                                          AS part_expeditor_name,
    MGL_SOL.sales_order_sales_status                                        AS sales_status,
    MGL_SO.sales_order_header_sales_status                                  AS sales_header_status,
    MGL_ST.sales_order_document_status                                      AS doc_status,
    MAC."SET"                                                               AS business_type,

    -- Promised date: confirmed ship > requested ship
    COALESCE(
        NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01'),
        NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED, 10), '1900-01-01')
    )                                                                       AS promised_date,
    NULLIF(LEFT(SOL.SHIPPINGDATEREQUESTED, 10), '1900-01-01')               AS shipping_date_requested,
    NULLIF(LEFT(SOL.SHIPPINGDATECONFIRMED, 10), '1900-01-01')               AS shipping_date_confirmed,
    CIL.INVOICEID                                                           AS invoice_number,
    NULLIF(LEFT(CPSL.CREATEDDATETIME, 10), '1900-01-01')                    AS delivery_date,
    MAX(NULLIF(LEFT(CIL.INVOICEDATE, 10), '1900-01-01'))                    AS invoiced_date,

    -- DIFOT line status
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
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ORDER_LINES SOL
    ON SO.SALESID = SOL.SALESID AND SO.DATAAREAID = SOL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ITEMS I
    ON SOL.ITEMID = I.ITEMID AND SOL.DATAAREAID = I.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.CUSTOMER_PACKING_SLIP_LINES CPSL
    ON SOL.SALESID = CPSL.SALESID AND SOL.LINENUM = CPSL.LINENUM AND SO.DATAAREAID = CPSL.DATAAREAID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.CUSTOMER_INVOICE_LINES CIL
    ON SOL.SALESID = CIL.SALESID AND SOL.LINENUM = CIL.LINENUM
   AND SOL.DATAAREAID = CIL.DATAAREAID AND SOL.ITEMID = CIL.ITEMID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ENOVIA_ATTRIBUTES EA
    ON I.PRODUCT = EA.ECORESPRODUCT
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DIMENSION_CODE_SET DCS
    ON SOL.DEFAULTDIMENSION = DCS.RECID
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.BUYER_GROUPS BG
    ON I.ITEMBUYERGROUPID = BG._GROUP
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.ADDRESSES AD
    ON SO.DELIVERYPOSTALADDRESS = AD.RECID
LEFT JOIN FLS_DEV_DB.MART_DYN_FO.MOC_AM_CAP MAC
    ON REGEXP_SUBSTR(EA.MOCCODE, '\\d*\\.?\\d+') = MAC."ROW LABELS"
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SITE S
    ON UPPER(SO.DATAAREAID) = UPPER(S.DATAAREAID)
   AND UPPER(CASE WHEN SO.DATAAREAID = 'sa1' THEN 'SAUDI ARABIA' ELSE SO.INVENTSITEID END) = UPPER(S.NAME)
LEFT JOIN FLS_PROD_DB.RAW_SHAREPOINT_CI.COUNTRY_REGION CR
    ON DCS.DESTINATIONVALUE = CR.CUSTOMER_COUNTRY_CODE
LEFT JOIN FLS_PROD_DB.MART_DYN_FO.DIM_SUPPLIER DS
    ON I.PRIMARYVENDORID = DS.SUPPLIER_NUMBER
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
GROUP BY ALL
ORDER BY so_created_date DESC
LIMIT 800
""")

# 7. Supply vs demand gap — ABS() on inventory
print("[7/7] Supply vs demand gap")
run("supply_demand_gap", """
    SELECT
        inv.DATAAREAID          AS company,
        inv.ITEMID              AS item_id,
        inv.INVENTSITEID        AS site,
        SUM(ABS(inv.AVAILPHYSICAL))  AS avail_physical,
        SUM(ABS(inv.ONORDER))        AS on_order,
        COALESCE(fc.forecast_qty, 0) AS forecast_demand,
        SUM(ABS(inv.AVAILPHYSICAL))
            + SUM(ABS(inv.ONORDER))
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
""")

conn.close()
print(f"\nAll done. Files in: {OUTPUT}")
