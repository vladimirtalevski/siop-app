# -*- coding: utf-8 -*-
"""
Unified Snowflake export — D365 + Oracle CEN01 + Epicor.
Includes Business Line classification (PCV / Service / Products),
Execution Type, Reference, Offering Type, Product Line.

Run AFTER export_direct.py (which handles inventory, forecast, etc. from MotherDuck).
A browser SSO popup will open once.

Usage:
    cd siop-app/backend
    python export_snowflake_erp.py
"""
import json, os, time
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "frontend", "public", "data")

print("=" * 60)
print("  SIOP Speed Up -- Unified Snowflake Export")
print("  D365 + Oracle CEN01 + Epicor | Business Line | Execution")
print("=" * 60)
print()
print("Connecting to Snowflake (browser SSO popup will open)...")

conn = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    authenticator="externalbrowser",
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_ROLE"),
    database=os.getenv("SNOWFLAKE_DATABASE", "FLS_PROD_DB"),
    schema="PUBLIC",
    login_timeout=90,
    network_timeout=300,
)
print("Connected.\n")


def run_query(label, sql):
    t0 = time.time()
    print(f"  {label}... ", end="", flush=True)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        rows = []
        for row in cur.fetchall():
            rows.append(dict(zip(cols, [str(v) if v is not None else None for v in row])))
        print(f"{len(rows)} rows ({time.time()-t0:.0f}s)")
        return rows
    except Exception as e:
        print(f"FAILED: {e}")
        return []


_sql_path = os.path.join(os.path.dirname(__file__), "speed_up_query.sql")
with open(_sql_path, "r", encoding="utf-8") as _f:
    UNIFIED_SQL = _f.read()

if False:
 _x = """
WITH hca_party AS (
    SELECT cust_account_id, party_name, country
    FROM (
        SELECT
            hca.cust_account_id,
            hp.party_name,
            hl.country,
            ROW_NUMBER() OVER (
                PARTITION BY hca.cust_account_id
                ORDER BY hp.last_update_date DESC
            ) AS rn
        FROM FLS_PROD_DB.RAW_CEN01.AR_HZ_CUST_ACCOUNTS       hca
        JOIN FLS_PROD_DB.RAW_CEN01.AR_HZ_PARTIES              hp
          ON hca.party_id = hp.party_id
        JOIN FLS_PROD_DB.RAW_CEN01.AR_HZ_CUST_ACCT_SITES_ALL hcas
          ON hca.cust_account_id = hcas.cust_account_id
        JOIN FLS_PROD_DB.RAW_CEN01.AR_HZ_PARTY_SITES          hps
          ON hcas.party_site_id = hps.party_site_id
        JOIN FLS_PROD_DB.RAW_CEN01.AR_HZ_LOCATIONS            hl
          ON hps.location_id = hl.location_id
        WHERE hcas.status = 'A' OR hcas.status IS NULL
    ) t
    WHERE rn = 1
),
minmax AS (
    SELECT segment1, inventory_item_id, organization_id,
           min_minmax_quantity, max_minmax_quantity
    FROM FLS_PROD_DB.RAW_CEN01.APPS_MTL_SYSTEM_ITEMS_B
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY inventory_item_id, organization_id
        ORDER BY last_update_date DESC, last_updated_by DESC
    ) = 1
),
ship_to AS (
    SELECT header_id, ship_to_name
    FROM (
        SELECT ooha.header_id, hps_st.party_name AS ship_to_name,
               ROW_NUMBER() OVER (
                   PARTITION BY ooha.header_id
                   ORDER BY hps_st.last_update_date DESC NULLS LAST
               ) AS rn
        FROM FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_HEADERS_ALL ooha
        JOIN FLS_PROD_DB.RAW_CEN01.AR_HZ_CUST_ACCOUNTS      hca_st
          ON ooha.ship_to_org_id = hca_st.cust_account_id
        JOIN FLS_PROD_DB.RAW_CEN01.AR_HZ_PARTIES            hps_st
          ON hca_st.party_id = hps_st.party_id
        WHERE ooha.ship_to_org_id IS NOT NULL
    ) t
    WHERE rn = 1
),
difot_deduped AS (
    SELECT
        TO_CHAR(order_number) || TO_CHAR(line_number) AS order_concat,
        MAX(difot_date)                                AS difot_date,
        MAX(logistics_readiness)                       AS logistics_readiness
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.VW_DIFOT_DATE_ORACLE_NEW_TEMP
    GROUP BY 1
),
enovia_typology AS (
    SELECT e.OBJECT_NAME, d.PRODUCT_LINE_NAME, d.PRODUCT_TYPOLOGY_NAME
    FROM FLS_PROD_DB.MART_ENOVIA.EDW_ENOVIA_VW_PARTS_CURR       e
    LEFT JOIN FLS_DEV_DB.MART_DATA_QUALITY.DIM_Product_Typology d
      ON d.PRODUCT_TYPOLOGY_NAME = e.PRODUCT_CODE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY e.OBJECT_NAME
        ORDER BY e.modification_info_datetime DESC
    ) = 1
),
mhs_deduped AS (
    SELECT COMPANY, LOCATION, COMPANY_NAME, HFM_LOCATION
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_HFM_MASTER_SALEORDER
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COMPANY ORDER BY COMPANY) = 1
),
xott_deduped AS (
    SELECT ID, CONCATENATED_VALUE
    FROM FLS_PROD_DB.RAW_CEN01.XXFLS_XXFLS_ORDER_TYPE_TYPOLOGY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY CONCATENATED_VALUE) = 1
),
mot_deduped AS (
    SELECT CODE, "ORDER OFFERING (LEVEL 2)", "HFM MAPPING"
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_MASTER_ORDERTYPOLOGY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CODE ORDER BY CODE) = 1
),
mtl_deduped AS (
    SELECT CATEGORY_ID, DESCRIPTION, STRUCTURE_NAME, ENABLED_FLAG
    FROM FLS_PROD_DB.RAW_CEN01.APPS_MTL_CATEGORIES_V
    WHERE STRUCTURE_NAME = 'Product Code'
      AND ENABLED_FLAG   = 'Y'
      AND DESCRIPTION    IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CATEGORY_ID ORDER BY CATEGORY_ID) = 1
),
ptm_deduped AS (
    SELECT "PRODUCT TYPOLOGY", "DIVISION CODE"
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_PRODUCTYPOLOGY_MAPPING
    QUALIFY ROW_NUMBER() OVER (PARTITION BY "PRODUCT TYPOLOGY" ORDER BY "DIVISION CODE") = 1
),
po_chain AS (
    SELECT PRDA.ATTRIBUTE13 AS OOLA_LINE_ID_TXT,
           PHA.SEGMENT1     AS PO_NUMBER,
           PLA.LINE_NUM     AS PO_LINE_NUM,
           PLA.PO_LINE_ID
    FROM FLS_PROD_DB.RAW_CEN01.PO_PO_REQ_DISTRIBUTIONS_ALL PRDA
    JOIN FLS_PROD_DB.RAW_CEN01.PO_PO_DISTRIBUTIONS_ALL     PDA
      ON PRDA.DISTRIBUTION_ID = PDA.REQ_DISTRIBUTION_ID
    JOIN FLS_PROD_DB.RAW_CEN01.PO_PO_LINES_ALL             PLA
      ON PDA.PO_LINE_ID = PLA.PO_LINE_ID
    JOIN FLS_PROD_DB.RAW_CEN01.PO_PO_HEADERS_ALL           PHA
      ON PLA.PO_HEADER_ID = PHA.PO_HEADER_ID
    WHERE PRDA.ATTRIBUTE13 IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY PRDA.ATTRIBUTE13
        ORDER BY PLA.LAST_UPDATE_DATE DESC
    ) = 1
),
ewbexp_deduped AS (
    SELECT "PO NUMBER", "PO LINE NUMBER", "PACKSLIP CREATION DATE"
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.EXPEDITING_DATA_EWB
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "PO NUMBER", "PO LINE NUMBER"
        ORDER BY "PACKSLIP CREATION DATE" DESC NULLS LAST
    ) = 1
),
sifot_deduped AS (
    SELECT "PO_NUMBER", "PO_LINE_NUMBER", ACTUAL_RECEIPT_DATE
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.VW_SIFOTDATE_TMS_EWB
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY "PO_NUMBER", "PO_LINE_NUMBER"
        ORDER BY ACTUAL_RECEIPT_DATE DESC NULLS LAST
    ) = 1
),
per_deduped AS (
    SELECT PERSON_ID, FULL_NAME
    FROM FLS_PROD_DB.RAW_CEN01.PER_ALL_PEOPLE_F
    WHERE CURRENT_DATE BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY EFFECTIVE_START_DATE DESC) = 1
),
ranked_load_line AS (
    SELECT
        LD.INVENTTRANSID,
        LD.DATAAREAID,
        LT.LOADSTATUS,
        LT.LOADSCHEDSHIPUTCDATETIME,
        LT.LOADSHIPCONFIRMUTCDATETIME,
        CASE
            WHEN LT.LOADSTATUS IN (0, 1, 2, 3) THEN CAST(LT.LOADSCHEDSHIPUTCDATETIME AS DATE)
            WHEN LT.LOADSTATUS IN (5, 6, 9) THEN
                CASE
                    WHEN LT.LOADSCHEDSHIPUTCDATETIME < LT.LOADSHIPCONFIRMUTCDATETIME
                     AND LT.LOADSCHEDSHIPUTCDATETIME <> '1900-01-01'
                    THEN CAST(LT.LOADSCHEDSHIPUTCDATETIME AS DATE)
                    ELSE CAST(LT.LOADSHIPCONFIRMUTCDATETIME AS DATE)
                END
            ELSE CAST('9999-01-01' AS DATE)
        END AS LOADDATESUPPORT
    FROM FLS_PROD_DB.MART_DYN_FO.LOAD_DETAILS LD
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.LOADS   LT
      ON LD.LOADID     = LT.LOADID
     AND LD.DATAAREAID = LT.DATAAREAID
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LD.INVENTTRANSID, LD.DATAAREAID
        ORDER BY LD.MODIFIEDDATETIME DESC
    ) = 1
),
item_bl_profile AS (
    SELECT
        OBL.ORDERED_ITEM,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'PCV'      THEN 1 ELSE 0 END) AS pcv_n,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'Service'  THEN 1 ELSE 0 END) AS service_n,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'Products' THEN 1 ELSE 0 END) AS products_n,
        COUNT(*) AS total_n
    FROM FLS_DEV_DB.MART_MY_ORDERS.ORDER_WITH_BUSINESS_LINE OBL
    WHERE OBL.assigned_business_line_name IN ('PCV','Service','Products')
      AND OBL.ORDERED_ITEM IS NOT NULL
    GROUP BY OBL.ORDERED_ITEM
),
productline_bl_profile AS (
    SELECT
        ENO.PRODUCT_LINE_NAME,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'PCV'      THEN 1 ELSE 0 END) AS pcv_n,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'Service'  THEN 1 ELSE 0 END) AS service_n,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'Products' THEN 1 ELSE 0 END) AS products_n,
        COUNT(*) AS total_n
    FROM FLS_DEV_DB.MART_MY_ORDERS.ORDER_WITH_BUSINESS_LINE OBL
    JOIN enovia_typology ENO ON ENO.OBJECT_NAME = OBL.ORDERED_ITEM
    WHERE OBL.assigned_business_line_name IN ('PCV','Service','Products')
      AND ENO.PRODUCT_LINE_NAME IS NOT NULL
    GROUP BY ENO.PRODUCT_LINE_NAME
),
inv_company_mapping AS (
    SELECT ORDER_DATAAREAID, INV_COMPANY
    FROM (
        VALUES
            ('FLS158','FLS158'),('FLS309','FLS309'),('FLS440','FLS440'),('FLS936A','FLS936A'),
            ('103 FLSmidth AS','103 FLSmidth AS'),('114 FLSmidth Germany GmbH','114 FLSmidth Germany GmbH'),
            ('287 FLSmidth A/S','287 FLSmidth A/S'),('311 FLSmidth Ltd.','311 FLSmidth Ltd.'),
            ('451 FLSmidth Cemento Mexico S.A. de C.V.','451 FLSmidth Cemento Mexico S.A. de C.V.'),
            ('453 FLSmidth Cement India LLP','453 FLSmidth Cement India LLP'),
            ('525 FLS USA Inc.','525 FLS USA Inc.'),('532 FLSmidth S.A. de C.V.','532 FLSmidth S.A. de C.V.'),
            ('544 FLSmidth Private Limited','544 FLSmidth Private Limited'),
            ('923 FLSmidth Inc.','923 FLSmidth Inc.'),('923 FLSmidth Inc. Minerals','923 FLSmidth Inc. Minerals'),
            ('us2','FLSmidth Inc.'),('in2','FLSmidth Private Ltd.'),('mx1','FLSmidth S.A. de C.V.'),
            ('sa1','Saudi FLSmidth Co'),('za4','FLSmidth (Pty) Ltd - South Africa'),
            ('za3','FLSmidth (Pty) Ltd.'),('pcpl',NULL),('gh1',NULL)
    ) AS m(ORDER_DATAAREAID, INV_COMPANY)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(ORDER_DATAAREAID) ORDER BY INV_COMPANY) = 1
),
inv_enriched AS (
    SELECT
        inv.COMPANY AS INV_COMPANY, inv.PARTNUM AS INV_PARTNUM,
        inv."Country" AS INV_COUNTRY, inv."Region" AS INV_REGION,
        inv.plant AS INV_PLANT, inv.PRODUCTFAMILY AS INV_PRODUCT_FAMILY,
        inv."Entity" AS INV_ENTITY, inv.hfm AS INV_HFM,
        COALESCE(
            prod1.business_line, prod2.business_line, prod3.business_line, prod4.business_line,
            CASE SPLIT_PART(inv.PRODUCTFAMILY, '.', 1)
                WHEN 'Pumps, Cyclones & Valves' THEN 'PCV'
                WHEN 'Common Parts' THEN 'Service'
                WHEN 'Bags and Cages (AFT)' THEN 'Service'
                WHEN 'Crushing & Screening' THEN 'Products'
                WHEN 'Milling & Grinding' THEN 'Products'
                WHEN 'Separation' THEN 'Products'
                WHEN 'Thickening & Filtration' THEN 'Products'
                WHEN 'Mill & Crusher Liners' THEN 'Products'
                ELSE NULL
            END,
            CASE MIC.NOME_SEGME
                WHEN 'PUMPS' THEN 'PCV' WHEN 'CYCLONES' THEN 'PCV' WHEN 'VALVES' THEN 'PCV'
                WHEN 'WEAR & CONSUMABLES' THEN 'Service' WHEN 'LINERS & PERFORMANCE PARTS' THEN 'Service'
                ELSE NULL
            END
        ) AS INV_BUSINESS_LINE
    FROM EDW.INVENTORY_VW_MYINVENTORY inv
    LEFT JOIN (
        SELECT * FROM FLS_PROD_DB.MART_ENOVIA.EDW_ENOVIA_VW_PARTS_CURR
        QUALIFY ROW_NUMBER() OVER (PARTITION BY OBJECT_NAME ORDER BY MODIFICATION_INFO_DATETIME DESC) = 1
    ) ENO ON ENO.OBJECT_NAME = inv.PARTNUM
    LEFT JOIN (
        SELECT C7_PRODUTO, MAIN_PROD, NOME_SEGME, 'SDBP11' AS id
        FROM FLS_PROD_DB.RAW_MICROSIGA.DBO_VW_SIEVO_PO_EXTRACTION
        WHERE MAIN_PROD IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY C7_PRODUTO ORDER BY MAIN_PROD) = 1
    ) MIC ON MIC.C7_PRODUTO = inv.partnum AND MIC.ID = inv.id
    LEFT JOIN (
        SELECT product_code, business_line FROM FLS_PROD_DB.RAW_SHAREPOINT.PRODUCT_TYPOLOGY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_code) ORDER BY business_line) = 1
    ) prod1 ON UPPER(ENO.PRODUCT_CODE) = UPPER(prod1.product_code)
    LEFT JOIN (
        SELECT product_code, business_line FROM FLS_PROD_DB.RAW_SHAREPOINT.PRODUCT_TYPOLOGY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_code) ORDER BY business_line) = 1
    ) prod2 ON prod1.product_code IS NULL AND UPPER(inv.product_typology) = UPPER(prod2.product_code)
    LEFT JOIN (
        SELECT product_code, business_line FROM FLS_PROD_DB.RAW_SHAREPOINT.PRODUCT_TYPOLOGY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_code) ORDER BY business_line) = 1
    ) prod3 ON prod1.product_code IS NULL AND prod2.product_code IS NULL
           AND UPPER(inv.product_typology) = UPPER(prod3.product_code)
    LEFT JOIN (
        SELECT product_code, business_line FROM FLS_PROD_DB.RAW_SHAREPOINT.PRODUCT_TYPOLOGY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_code) ORDER BY business_line) = 1
    ) prod4 ON prod1.product_code IS NULL AND prod2.product_code IS NULL AND prod3.product_code IS NULL
           AND UPPER(inv.product_typology) = UPPER(prod4.product_code)
    WHERE inv.onhandqty > 0
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY inv.COMPANY, inv.PARTNUM
        ORDER BY inv.onhandqty DESC NULLS LAST, inv.plant
    ) = 1
),
epicor_partplant AS (
    SELECT COMPANY, PARTNUM, PLANT, MINIMUMQTY, MAXIMUMQTY
    FROM FLS_PROD_DB.RAW_EPICOR01.ERP_PARTPLANT
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COMPANY, PARTNUM, PLANT
        ORDER BY COALESCE(SYSREVID, '0') DESC
    ) = 1
),
epicor_partwhse AS (
    SELECT COMPANY, PARTNUM, WAREHOUSECODE, MINIMUMQTY, MAXIMUMQTY
    FROM FLS_PROD_DB.RAW_EPICOR01.ERP_PARTWHSE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COMPANY, PARTNUM, WAREHOUSECODE
        ORDER BY COALESCE(SYSREVID, '0') DESC
    ) = 1
),
obl_deduped AS (
    SELECT COMPANY, ORDER_NUMBER, LINE_ID, LINE_NUMBER, ORDERED_ITEM, assigned_business_line_name
    FROM FLS_DEV_DB.MART_MY_ORDERS.ORDER_WITH_BUSINESS_LINE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COMPANY, ORDER_NUMBER, LINE_ID, LINE_NUMBER, ORDERED_ITEM
        ORDER BY CASE WHEN assigned_business_line_name IS NOT NULL THEN 0 ELSE 1 END
    ) = 1
),
ORACLE_ERP_CTE AS (
    SELECT
        CAST(OOLA.LINE_ID AS VARCHAR)                                    AS LINE_ID,
        CAST(OOHA.ORDER_NUMBER AS VARCHAR)                               AS ORDER_NUMBER,
        CAST(OOLA.LINE_NUMBER AS VARCHAR)                                AS LINE_NUMBER,
        CAST(OOLA.ORDERED_ITEM AS VARCHAR)                               AS ORDERED_ITEM,
        OOHA.BOOKED_DATE                                             AS CREATEDDATETIME,
        MM.MAX_MINMAX_QUANTITY                                       AS MAXINVENTONHAND,
        MM.MIN_MINMAX_QUANTITY                                       AS MININVENTONHAND,
        SUBQUERYPAF2.FULL_NAME                                       AS MODIFIEDBY,
        NULL                                                         AS MODIFIEDDATETIME,
        CASE
            WHEN OOLA.FLOW_STATUS_CODE IN ('AWAITING_RETURN','AWAITING_RECEIPT',
                                           'AWAITING_SHIPPING','BOOKED')
              AND DDO.DIFOT_DATE IS NULL     THEN 'Open'
            WHEN OOLA.FLOW_STATUS_CODE IN ('AWAITING_RETURN','AWAITING_RECEIPT',
                                           'AWAITING_SHIPPING','BOOKED')
              AND DDO.DIFOT_DATE IS NOT NULL THEN 'Closed'
            WHEN OOLA.FLOW_STATUS_CODE IN ('CLOSED','FULFILLED')    THEN 'Closed'
            WHEN OOLA.FLOW_STATUS_CODE = 'CANCELLED'                THEN 'Cancelled'
            ELSE NULL
        END                                                          AS SALESSTATUS,
        NULL                                                         AS DELIVERYTYPE,
        MOT."ORDER OFFERING (LEVEL 2)"                              AS OFFERINGTYPEVALUE,
        OOLA.UNIT_SELLING_PRICE                                      AS SALESPRICE,
        ROUND(CASE WHEN OOLA.LINE_CATEGORY_CODE = 'RETURN'
                   THEN OOLA.ORDERED_QUANTITY * -1
                   ELSE OOLA.ORDERED_QUANTITY END, 2)               AS ORDERED_QUANTITY,
        CAST(OOHA.BOOKED_DATE AS DATE)                               AS CREATEDDATE,
        CAST(OOLA.REQUEST_DATE AS DATE)                              AS REQUESTSHIPPINGDATE,
        CAST(OOLA.PROMISE_DATE AS DATE)                              AS CONFIRMEDSHIPPINGDATE,
        NULL                                                         AS REQUESTEDRECEIPTDATE,
        CAST(OOLA.ACTUAL_SHIPMENT_DATE AS DATE)                      AS REQUESTEDCONFIRMEDDATE,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)                AS INVENTSITEID,
        NULL                                                         AS INVENTLOCATIONID,
        COALESCE(MHS.COMPANY_NAME, OOD.ORGANIZATION_CODE)            AS DATAAREAID,
        HCA_PARTY.PARTY_NAME                                         AS CUSTNAME,
        CAST(OOHA.CUST_PO_NUMBER AS VARCHAR)                             AS CUST_PO_NUMBER,
        SHIP_TO.SHIP_TO_NAME                                         AS DELIVERYNAME,
        OOHA.FOB_POINT_CODE                                          AS DLVTERMID,
        NULL                                                         AS LOADID,
        CAST(OOHA.BOOKED_DATE AS DATE)                               AS SHIPMENTCREATEDDATE,
        NULL                                                         AS SHIPMENTID,
        CASE WHEN POC.PO_NUMBER IS NOT NULL THEN 'PO' ELSE 'Stock' END AS REFERENCE,
        CASE WHEN POC.PO_NUMBER IS NOT NULL THEN 'Purchase' ELSE 'Inventory' END AS EXECUTIONTYPE,
        COALESCE(
            SIFOT.ACTUAL_RECEIPT_DATE,
            EWBEXP."PACKSLIP CREATION DATE",
            LEAST(
                TO_DATE(LEFT(SHP.SUPPLIER_SHIP_DATE, 10)),
                TO_DATE(LEFT(RCT.ACTUAL_RECEIPT_DATE, 10))
            )
        )                                                            AS TURNEDINDATE,
        COALESCE(MHS.COMPANY_NAME, OOD.ORGANIZATION_CODE)            AS ENTITY_NAME,
        ENO_O.PRODUCT_LINE_NAME
    FROM FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_LINES_ALL               OOLA
    JOIN FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_HEADERS_ALL             OOHA ON OOLA.HEADER_ID = OOHA.HEADER_ID
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD ON OOLA.SHIP_FROM_ORG_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped     MHS      ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    LEFT JOIN xott_deduped    XOTT     ON COALESCE(OOLA.ATTRIBUTE8, OOHA.ATTRIBUTE15) = XOTT.ID
    LEFT JOIN mot_deduped     MOT      ON XOTT.CONCATENATED_VALUE = MOT.CODE
    LEFT JOIN mtl_deduped     MTL      ON TRIM(OOLA.ATTRIBUTE20) = MTL.CATEGORY_ID
    LEFT JOIN ptm_deduped     PTM      ON MTL.DESCRIPTION = PTM."PRODUCT TYPOLOGY"
    LEFT JOIN hca_party       HCA_PARTY ON OOHA.SOLD_TO_ORG_ID = HCA_PARTY.CUST_ACCOUNT_ID
    LEFT JOIN ship_to         SHIP_TO  ON OOHA.HEADER_ID = SHIP_TO.HEADER_ID
    LEFT JOIN minmax          MM       ON MM.SEGMENT1 = OOLA.ORDERED_ITEM AND MM.ORGANIZATION_ID = OOD.ORGANIZATION_ID
    LEFT JOIN per_deduped     SUBQUERYPAF2 ON OOHA.ATTRIBUTE16 = SUBQUERYPAF2.PERSON_ID
    LEFT JOIN difot_deduped   DDO      ON TO_CHAR(OOHA.ORDER_NUMBER) || TO_CHAR(OOLA.LINE_NUMBER) = DDO.ORDER_CONCAT
    LEFT JOIN po_chain        POC      ON OOLA.LINE_ID = TO_NUMBER(POC.OOLA_LINE_ID_TXT)
    LEFT JOIN ewbexp_deduped  EWBEXP   ON POC.PO_NUMBER = EWBEXP."PO NUMBER" AND POC.PO_LINE_NUM = EWBEXP."PO LINE NUMBER"
    LEFT JOIN sifot_deduped   SIFOT    ON POC.PO_NUMBER = SIFOT."PO_NUMBER" AND POC.PO_LINE_NUM = SIFOT."PO_LINE_NUMBER"
    LEFT JOIN (
        SELECT RSL.PO_LINE_ID, MAX(RSH.SHIPPED_DATE) AS SUPPLIER_SHIP_DATE
        FROM FLS_PROD_DB.RAW_CEN01.RCV_SHIPMENT_LINES   RSL
        JOIN FLS_PROD_DB.RAW_CEN01.RCV_SHIPMENT_HEADERS RSH ON RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID
        WHERE RSH.SHIPPED_DATE IS NOT NULL GROUP BY RSL.PO_LINE_ID
    ) SHP ON POC.PO_LINE_ID = SHP.PO_LINE_ID
    LEFT JOIN (
        SELECT RT.PO_LINE_ID, MAX(RT.TRANSACTION_DATE) AS ACTUAL_RECEIPT_DATE
        FROM FLS_PROD_DB.RAW_CEN01.RCV_TRANSACTIONS RT
        WHERE RT.TRANSACTION_TYPE = 'RECEIVE' GROUP BY RT.PO_LINE_ID
    ) RCT ON POC.PO_LINE_ID = RCT.PO_LINE_ID
    LEFT JOIN enovia_typology ENO_O ON ENO_O.OBJECT_NAME = REGEXP_REPLACE(OOLA.ORDERED_ITEM, '-[A-Z]{2,4}[0-9]{0,2}$', '')
    WHERE OOHA.BOOKED_DATE   >= '2025-01-01'
      AND OOHA.CONTEXT        = 'ORDER'
      AND OOHA.ORG_ID        IN ('18102','3241','10682','91','19480','8962')
      AND OOHA.ORDER_TYPE_ID NOT IN ('1187','1541','1405','1245','1308','1592','1201','1121')
      AND OOLA.LINE_TYPE_ID  NOT IN ('1184','1182','1181','1006')
      AND OOLA.LINE_ID        IS NOT NULL
),
D365_CTE AS (
    SELECT
        CAST(OL.SALESID AS STRING)                   AS LINE_ID,
        SO.SALESID                                   AS ORDER_NUMBER,
        OL.LINENUM                                   AS LINE_NUMBER,
        OL.ITEMID                                    AS ORDERED_ITEM,
        SO.CREATEDDATETIME,
        IC.MAXINVENTONHAND,
        IC.MININVENTONHAND,
        IC.MODIFIEDBY,
        IC.MODIFIEDDATETIME,
        CASE OL.SALESSTATUS
            WHEN 3 THEN 'Invoiced'  WHEN 1 THEN 'Open Order'
            WHEN 4 THEN 'Canceled'  WHEN 0 THEN NULL
            WHEN 2 THEN 'Delivered'
        END                                          AS SALESSTATUS,
        CASE OL.DELIVERYTYPE
            WHEN 0 THEN 'None' WHEN 1 THEN 'DropShip'
        END                                          AS DELIVERYTYPE,
        COALESCE(DCS_LINE.ORDERTYPOLOGYVALUE, DCS_LINE.OFFERINGTYPEVALUE) AS OFFERINGTYPEVALUE,
        OL.SALESPRICE,
        OL.QTYORDERED                                AS ORDERED_QUANTITY,
        CAST(OL.CREATEDDATETIME AS DATE)             AS CREATEDDATE,
        CAST(OL.SHIPPINGDATEREQUESTED AS DATE)       AS REQUESTSHIPPINGDATE,
        CAST(OL.ShippingDateConfirmed AS DATE)       AS CONFIRMEDSHIPPINGDATE,
        CAST(OL.ReceiptDateRequested AS DATE)        AS REQUESTEDRECEIPTDATE,
        CAST(OL.receiptdateconfirmed AS DATE)        AS REQUESTEDCONFIRMEDDATE,
        ID.INVENTSITEID,
        ID.INVENTLOCATIONID,
        OL.DATAAREAID,
        dpt.name                                     AS CUSTNAME,
        C.ACCOUNTNUM                                 AS CUST_PO_NUMBER,
        S.deliveryname                               AS DELIVERYNAME,
        S.DLVTERMID,
        S.LOADID,
        CAST(S.CREATEDDATETIME AS DATE)              AS SHIPMENTCREATEDDATE,
        S.SHIPMENTID,
        CASE OL.inventreftype
            WHEN 0 THEN NULL             WHEN 1 THEN 'Sales order'
            WHEN 2 THEN 'Purchase order' WHEN 3 THEN 'Production'
            WHEN 4 THEN 'Production line' WHEN 5 THEN 'Inventory journal'
            WHEN 6 THEN 'Sales quotation' WHEN 7 THEN 'Transfer order'
            WHEN 8 THEN 'Fixed asset'    ELSE 'Unknown'
        END                                          AS REFERENCE,
        CASE OL.INVENTREFTYPE
            WHEN 1 THEN 'Sales order-driven'
            WHEN 3 THEN 'Production-driven'
            WHEN 0 THEN 'Standard / No reference'
            ELSE NULL
        END                                          AS EXECUTIONTYPE,
        CASE
            WHEN RLL.LOADDATESUPPORT IS NULL
              OR RLL.LOADDATESUPPORT = '1900-01-01'
              OR RLL.LOADDATESUPPORT = '9999-01-01'
            THEN NULL
            ELSE RLL.LOADDATESUPPORT
        END                                          AS TURNEDINDATE,
        COALESCE(ID.INVENTSITEID, OL.DATAAREAID)     AS ENTITY_NAME,
        ENO_D.PRODUCT_LINE_NAME
    FROM "FLS_PROD_DB"."MART_DYN_FO".ORDER_LINES               OL
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".SALES_ORDERS         SO  ON SO.SALESID = OL.SALESID AND SO.DATAAREAID = OL.DATAAREAID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".CUSTOMERS            C   ON SO.INVOICEACCOUNT = C.ACCOUNTNUM AND SO.DATAAREAID = C.DATAAREAID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".GLOBAL_ADDRESS_BOOK  dpt ON dpt.RECID = C.PARTY
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".DIMENSION_CODE_SET   DCS_LINE ON OL.DEFAULTDIMENSION = DCS_LINE.RECID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".INVENTORY_DIMENSIONS ID  ON ID.INVENTDIMID = OL.INVENTDIMID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".LOAD_DETAILS         LD  ON LD.INVENTTRANSID = OL.INVENTTRANSID AND LD.DATAAREAID = OL.DATAAREAID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".SHIPMENTS            S   ON S.SHIPMENTID = LD.SHIPMENTID AND S.DATAAREAID = LD.DATAAREAID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".ITEM_COVERAGE        IC  ON IC.ITEMID = OL.ITEMID AND IC.DATAAREAID = OL.DATAAREAID
    LEFT JOIN enovia_typology ENO_D ON ENO_D.OBJECT_NAME = OL.ITEMID
    LEFT JOIN ranked_load_line RLL  ON RLL.INVENTTRANSID = OL.INVENTTRANSID AND RLL.DATAAREAID = OL.DATAAREAID
    WHERE OL.CREATEDDATETIME >= '2025-01-01'
),
EPICOR_CTE AS (
    SELECT
        CAST(SALEORDER_NUMBER AS STRING) || '-' || CAST(SALEORDER_LINE AS STRING) AS LINE_ID,
        CAST(SALEORDER_NUMBER AS STRING)                 AS ORDER_NUMBER,
        SALEORDER_LINE                                   AS LINE_NUMBER,
        SO_ITEM_NUMBER                                   AS ORDERED_ITEM,
        CAST(SALEORDER_ORDERDATE AS TIMESTAMP)           AS CREATEDDATETIME,
        COALESCE(NULLIF(PP.MAXIMUMQTY, 0), NULLIF(PW.MAXIMUMQTY, 0)) AS MAXINVENTONHAND,
        COALESCE(NULLIF(PP.MINIMUMQTY, 0), NULLIF(PW.MINIMUMQTY, 0)) AS MININVENTONHAND,
        ORDER_HANDLER                                    AS MODIFIEDBY,
        NULL                                             AS MODIFIEDDATETIME,
        SO_LINE_STATUS                                   AS SALESSTATUS,
        CASE WHEN DROPSHIP = 'true' THEN 'DropShip' ELSE 'None' END AS DELIVERYTYPE,
        SO_ORDER_OFFERING                                AS OFFERINGTYPEVALUE,
        SO_UNIT_PRICE                                    AS SALESPRICE,
        SO_ORDER_QTY                                     AS ORDERED_QUANTITY,
        CAST(SALEORDER_ORDERDATE AS DATE)                AS CREATEDDATE,
        CAST(SO_NEEDBY_DATE AS DATE)                     AS REQUESTSHIPPINGDATE,
        CAST(SO_PROMISE_DATE AS DATE)                    AS CONFIRMEDSHIPPINGDATE,
        NULL                                             AS REQUESTEDRECEIPTDATE,
        CAST(ACTUAL_SHIP_DATE AS DATE)                   AS REQUESTEDCONFIRMEDDATE,
        SO_PLANT                                         AS INVENTSITEID,
        NULL                                             AS INVENTLOCATIONID,
        SO_COMPANY                                       AS DATAAREAID,
        SO_CUSTOMER_NAME                                 AS CUSTNAME,
        CUSTOMER_PO_NUMBER                               AS CUST_PO_NUMBER,
        NULL                                             AS DELIVERYNAME,
        INCOTERM                                         AS DLVTERMID,
        NULL                                             AS LOADID,
        CAST(ACTUAL_SHIP_DATE AS DATE)                   AS SHIPMENTCREATEDDATE,
        NULL                                             AS SHIPMENTID,
        SO_LINE_SOURCE                                   AS REFERENCE,
        CASE SO_LINE_SOURCE
            WHEN 'Make'  THEN 'Manufacturing'
            WHEN 'PO'    THEN 'Purchase'
            WHEN 'Stock' THEN 'Inventory'
            ELSE SO_LINE_SOURCE
        END                                              AS EXECUTIONTYPE,
        CAST(POFULFILLMENT_DATE AS DATE)                 AS TURNEDINDATE,
        CASE SO_COMPANY
            WHEN 'FLS309'  THEN 'AU, Welshpool & Pinkenba & Beresfield'
            WHEN 'FLS158'  THEN 'CL, La Negra & Renca'
            WHEN 'FLS936A' THEN 'PE, Lima'
            WHEN 'FLS440'  THEN 'ID, Surabaya'
            ELSE SO_COMPANY
        END                                              AS ENTITY_NAME,
        ENO_E.PRODUCT_LINE_NAME
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.VW_EPICOR_SALE_ORDER SO_V
    LEFT JOIN epicor_partplant PP
      ON PP.COMPANY = SO_V.SO_COMPANY AND PP.PARTNUM = SO_V.SO_ITEM_NUMBER AND UPPER(PP.PLANT) = UPPER(SO_V.SO_PLANT)
    LEFT JOIN epicor_partwhse PW
      ON PW.COMPANY = SO_V.SO_COMPANY AND PW.PARTNUM = SO_V.SO_ITEM_NUMBER AND UPPER(PW.WAREHOUSECODE) = UPPER(SO_V.SO_PLANT)
    LEFT JOIN enovia_typology ENO_E ON ENO_E.OBJECT_NAME = SO_V.SO_ITEM_NUMBER
    WHERE SALEORDER_ORDERDATE >= '2025-01-01'
),
UNIONED AS (
    SELECT *, 'D365'       AS SOURCE FROM D365_CTE
    UNION ALL
    SELECT *, 'ORACLE_ERP' AS SOURCE FROM ORACLE_ERP_CTE
    UNION ALL
    SELECT *, 'EPICOR'     AS SOURCE FROM EPICOR_CTE
),
site_bl_profile AS (
    SELECT
        SV.INVENTSITEID,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'PCV'      THEN 1 ELSE 0 END) AS pcv_n,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'Service'  THEN 1 ELSE 0 END) AS service_n,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'Products' THEN 1 ELSE 0 END) AS products_n,
        COUNT(*) AS total_n
    FROM UNIONED SV
    JOIN obl_deduped OBL
      ON OBL.COMPANY = SV.DATAAREAID AND OBL.ORDER_NUMBER = SV.ORDER_NUMBER
     AND OBL.LINE_ID = SV.LINE_ID    AND OBL.LINE_NUMBER  = SV.LINE_NUMBER
     AND OBL.ORDERED_ITEM = SV.ORDERED_ITEM
    WHERE OBL.assigned_business_line_name IN ('PCV','Service','Products')
      AND SV.INVENTSITEID IS NOT NULL
    GROUP BY SV.INVENTSITEID
),
FINAL_WITH_DUPS AS (
    SELECT
        SV.LINE_ID, SV.ORDER_NUMBER, SV.LINE_NUMBER, SV.ORDERED_ITEM,
        SV.CREATEDDATETIME, SV.MAXINVENTONHAND, SV.MININVENTONHAND,
        SV.MODIFIEDBY, SV.MODIFIEDDATETIME, SV.SALESSTATUS, SV.DELIVERYTYPE,
        SV.OFFERINGTYPEVALUE, SV.SALESPRICE, SV.ORDERED_QUANTITY,
        SV.CREATEDDATE, SV.REQUESTSHIPPINGDATE, SV.CONFIRMEDSHIPPINGDATE,
        SV.REQUESTEDRECEIPTDATE, SV.REQUESTEDCONFIRMEDDATE,
        SV.INVENTSITEID, SV.INVENTLOCATIONID, SV.DATAAREAID,
        SV.CUSTNAME, SV.CUST_PO_NUMBER, SV.DELIVERYNAME, SV.DLVTERMID,
        SV.LOADID, SV.SHIPMENTCREATEDDATE, SV.SHIPMENTID,
        SV.REFERENCE, SV.EXECUTIONTYPE, SV.TURNEDINDATE, SV.ENTITY_NAME,
        COALESCE(
            OBL.assigned_business_line_name,
            INV.INV_BUSINESS_LINE,
            CASE
                WHEN IBL.total_n > 0
                 AND GREATEST(IBL.pcv_n, IBL.service_n, IBL.products_n) * 1.0 / IBL.total_n >= 0.95
                THEN CASE
                    WHEN IBL.pcv_n     >= IBL.service_n AND IBL.pcv_n     >= IBL.products_n THEN 'PCV'
                    WHEN IBL.service_n >= IBL.pcv_n     AND IBL.service_n >= IBL.products_n THEN 'Service'
                    ELSE 'Products' END
            END,
            CASE
                WHEN PBL.total_n >= 50
                 AND GREATEST(PBL.pcv_n, PBL.service_n, PBL.products_n) * 1.0 / PBL.total_n >= 0.95
                THEN CASE
                    WHEN PBL.pcv_n     >= PBL.service_n AND PBL.pcv_n     >= PBL.products_n THEN 'PCV'
                    WHEN PBL.service_n >= PBL.pcv_n     AND PBL.service_n >= PBL.products_n THEN 'Service'
                    ELSE 'Products' END
            END,
            CASE
                WHEN SBL.total_n >= 100
                 AND GREATEST(SBL.pcv_n, SBL.service_n, SBL.products_n) * 1.0 / SBL.total_n >= 0.95
                THEN CASE
                    WHEN SBL.pcv_n     >= SBL.service_n AND SBL.pcv_n     >= SBL.products_n THEN 'PCV'
                    WHEN SBL.service_n >= SBL.pcv_n     AND SBL.service_n >= SBL.products_n THEN 'Service'
                    ELSE 'Products' END
            END,
            CASE
                WHEN IBL.total_n > 0
                 AND GREATEST(IBL.pcv_n, IBL.service_n, IBL.products_n) * 1.0 / IBL.total_n >= 0.70
                THEN CASE
                    WHEN IBL.pcv_n     >= IBL.service_n AND IBL.pcv_n     >= IBL.products_n THEN 'PCV'
                    WHEN IBL.service_n >= IBL.pcv_n     AND IBL.service_n >= IBL.products_n THEN 'Service'
                    ELSE 'Products' END
            END,
            CASE
                WHEN SBL.total_n >= 100
                 AND GREATEST(SBL.pcv_n, SBL.service_n, SBL.products_n) * 1.0 / SBL.total_n >= 0.70
                THEN CASE
                    WHEN SBL.pcv_n     >= SBL.service_n AND SBL.pcv_n     >= SBL.products_n THEN 'PCV'
                    WHEN SBL.service_n >= SBL.pcv_n     AND SBL.service_n >= SBL.products_n THEN 'Service'
                    ELSE 'Products' END
            END
        ) AS BUSINESS_LINE,
        SV.PRODUCT_LINE_NAME,
        SV.SOURCE
    FROM UNIONED SV
    LEFT JOIN obl_deduped OBL
      ON OBL.COMPANY = SV.DATAAREAID AND OBL.ORDER_NUMBER = SV.ORDER_NUMBER
     AND OBL.LINE_ID = SV.LINE_ID    AND OBL.LINE_NUMBER  = SV.LINE_NUMBER
     AND OBL.ORDERED_ITEM = SV.ORDERED_ITEM
    LEFT JOIN inv_company_mapping MAP ON UPPER(MAP.ORDER_DATAAREAID) = UPPER(SV.DATAAREAID)
    LEFT JOIN inv_enriched        INV ON UPPER(INV.INV_COMPANY) = UPPER(MAP.INV_COMPANY) AND INV.INV_PARTNUM = SV.ORDERED_ITEM
    LEFT JOIN item_bl_profile        IBL ON IBL.ORDERED_ITEM      = SV.ORDERED_ITEM
    LEFT JOIN productline_bl_profile PBL ON PBL.PRODUCT_LINE_NAME = SV.PRODUCT_LINE_NAME
    LEFT JOIN site_bl_profile        SBL ON SBL.INVENTSITEID      = SV.INVENTSITEID
)
SELECT
    CAST(LINE_ID AS VARCHAR)                                              AS line_id,
    CAST(ORDER_NUMBER AS VARCHAR)                                         AS sales_order_id,
    CAST(LINE_NUMBER AS VARCHAR)                                          AS line_num,
    ORDERED_ITEM                                                          AS item_id,
    CAST(CREATEDDATE AS VARCHAR)                                          AS created_date,
    CAST(REQUESTSHIPPINGDATE AS VARCHAR)                                  AS requested_ship_date,
    CAST(CONFIRMEDSHIPPINGDATE AS VARCHAR)                                AS confirmed_ship_date,
    CAST(TURNEDINDATE AS VARCHAR)                                         AS goods_issue_date,
    CAST(ORDERED_QUANTITY AS VARCHAR)                                     AS ordered_qty,
    NULL                                                                  AS shipped_qty,
    CAST(ROUND(SALESPRICE * ORDERED_QUANTITY, 2) AS VARCHAR)              AS line_value,
    NULL                                                                  AS currency,
    SALESSTATUS                                                           AS sales_status,
    ENTITY_NAME                                                           AS company,
    COALESCE(INVENTSITEID, ENTITY_NAME)                                   AS site,
    CUSTNAME                                                              AS customer_name,
    CASE
        WHEN TURNEDINDATE IS NOT NULL AND CONFIRMEDSHIPPINGDATE IS NOT NULL
             AND CAST(TURNEDINDATE AS DATE) <= CAST(CONFIRMEDSHIPPINGDATE AS DATE) THEN 'DIFOT'
        WHEN TURNEDINDATE IS NOT NULL AND CONFIRMEDSHIPPINGDATE IS NOT NULL
             AND CAST(TURNEDINDATE AS DATE) >  CAST(CONFIRMEDSHIPPINGDATE AS DATE) THEN 'Late'
        WHEN TURNEDINDATE IS NOT NULL                                               THEN 'Partial'
        WHEN CONFIRMEDSHIPPINGDATE IS NOT NULL
             AND CAST(CONFIRMEDSHIPPINGDATE AS DATE) < CURRENT_DATE()              THEN 'Past Due'
        ELSE 'Open'
    END                                                                   AS difot_status,
    DATEDIFF(day, CAST(CREATEDDATE AS DATE), CAST(TURNEDINDATE AS DATE))  AS lead_time_days,
    SOURCE                                                                AS erp_source,
    CASE
        WHEN COALESCE(MININVENTONHAND, 0) > 0 OR COALESCE(MAXINVENTONHAND, 0) > 0
        THEN 'Stock' ELSE 'Non Stock'
    END                                                                   AS stock_non_stock,
    EXECUTIONTYPE                                                         AS execution_type,
    REFERENCE                                                             AS reference,
    BUSINESS_LINE                                                         AS business_line,
    OFFERINGTYPEVALUE                                                     AS offering_type,
    PRODUCT_LINE_NAME                                                     AS product_line_name
FROM FINAL_WITH_DUPS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SOURCE, DATAAREAID, ORDER_NUMBER, LINE_ID, LINE_NUMBER, ORDERED_ITEM
    ORDER BY
        CASE WHEN BUSINESS_LINE     IS NOT NULL THEN 0 ELSE 1 END,
        CASE WHEN PRODUCT_LINE_NAME IS NOT NULL THEN 0 ELSE 1 END,
        CREATEDDATETIME DESC NULLS LAST
) = 1
LIMIT 15000
"""

CROSS_SITE_SQL = """
WITH
ranked_load_line AS (
    SELECT LD.INVENTTRANSID, LD.DATAAREAID,
        CASE
            WHEN LT.LOADSTATUS IN (0,1,2,3) THEN CAST(LT.LOADSCHEDSHIPUTCDATETIME AS DATE)
            WHEN LT.LOADSTATUS IN (5,6,9) THEN
                CASE WHEN LT.LOADSCHEDSHIPUTCDATETIME < LT.LOADSHIPCONFIRMUTCDATETIME
                      AND LT.LOADSCHEDSHIPUTCDATETIME <> '1900-01-01'
                     THEN CAST(LT.LOADSCHEDSHIPUTCDATETIME AS DATE)
                     ELSE CAST(LT.LOADSHIPCONFIRMUTCDATETIME AS DATE) END
            ELSE CAST('9999-01-01' AS DATE)
        END AS LOADDATESUPPORT
    FROM FLS_PROD_DB.MART_DYN_FO.LOAD_DETAILS LD
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.LOADS LT
        ON LD.LOADID = LT.LOADID AND LD.DATAAREAID = LT.DATAAREAID
    QUALIFY ROW_NUMBER() OVER (PARTITION BY LD.INVENTTRANSID, LD.DATAAREAID ORDER BY LD.MODIFIEDDATETIME DESC) = 1
),
mhs_deduped AS (
    SELECT COMPANY, LOCATION, COMPANY_NAME, HFM_LOCATION
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_HFM_MASTER_SALEORDER
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COMPANY ORDER BY COMPANY) = 1
),
fx_rates AS (
    SELECT xc.custom1 AS from_currency, xc.RATE AS rate_to_dkk
    FROM FLS_PROD_DB.EDW.HFM_EXCHANGE_RATE_STG xc
    WHERE xc.custom2 = 'DKK'
      AND xc.year = YEAR(ADD_MONTHS(SYSDATE(), -1))
      AND xc.period = TO_VARCHAR(ADD_MONTHS(SYSDATE(), -1), 'Mon')
      AND xc.account = 'ENDRATE' AND xc.scenario = 'ACT'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY xc.custom1 ORDER BY xc.custom1) = 1
),
inventory AS (
    SELECT UPPER(TRIM(inv.PARTNUM)) AS item_id, inv.COMPANY AS inv_company,
        inv."Entity" AS entity, inv.plant AS warehouse,
        inv."Country" AS country, inv.onhandqty AS onhand_qty,
        inv.PRODUCTFAMILY AS product_family,
        CASE WHEN inv.ID LIKE '%DYNAMIC%' OR inv.ID LIKE '%D365%' THEN 'D365'
             WHEN inv.ID = 'ST01P' THEN 'Epicor'
             WHEN inv.ID = 'SDBP11' THEN 'Microsiga'
             ELSE 'Oracle/Other' END AS stock_erp
    FROM EDW.INVENTORY_VW_MYINVENTORY inv WHERE inv.onhandqty > 0
),
d365_demand AS (
    SELECT UPPER(TRIM(OL.ITEMID)) AS item_id,
        COALESCE(ID.INVENTSITEID, OL.DATAAREAID) AS demand_site, OL.DATAAREAID,
        CAST(OL.QTYORDERED AS FLOAT) AS ordered_qty,
        CAST(OL.ShippingDateConfirmed AS DATE) AS confirmed_ship_date,
        ROUND(OL.SALESPRICE * OL.QTYORDERED, 2) AS line_value,
        CAST(OL.SALESID AS VARCHAR) AS order_number,
        'D365' AS demand_erp, COALESCE(SO.CURRENCYCODE, 'USD') AS currency,
        CASE WHEN CAST(OL.ShippingDateConfirmed AS DATE) < CURRENT_DATE() THEN 'Past Due' ELSE 'Open' END AS demand_status
    FROM FLS_PROD_DB.MART_DYN_FO.ORDER_LINES OL
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.SALES_ORDERS SO ON SO.SALESID = OL.SALESID AND SO.DATAAREAID = OL.DATAAREAID
    LEFT JOIN FLS_PROD_DB.MART_DYN_FO.INVENTORY_DIMENSIONS ID ON ID.INVENTDIMID = OL.INVENTDIMID
    LEFT JOIN ranked_load_line RLL ON RLL.INVENTTRANSID = OL.INVENTTRANSID AND RLL.DATAAREAID = OL.DATAAREAID
    WHERE OL.CREATEDDATETIME >= '2024-01-01' AND OL.SALESSTATUS IN (0,1)
      AND (RLL.LOADDATESUPPORT IS NULL OR RLL.LOADDATESUPPORT = CAST('9999-01-01' AS DATE) OR RLL.LOADDATESUPPORT = CAST('1900-01-01' AS DATE))
      AND OL.ITEMID IS NOT NULL
      AND CAST(OL.ShippingDateConfirmed AS DATE) > '1901-01-01'
      AND CAST(OL.ShippingDateConfirmed AS DATE) < '9998-01-01'
),
oracle_demand AS (
    SELECT UPPER(TRIM(REGEXP_REPLACE(OOLA.ORDERED_ITEM, '-[A-Z]{2,4}[0-9]{0,2}$', ''))) AS item_id,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE) AS demand_site,
        COALESCE(MHS.COMPANY_NAME, OOD.ORGANIZATION_CODE) AS DATAAREAID,
        CAST(CASE WHEN OOLA.LINE_CATEGORY_CODE = 'RETURN' THEN OOLA.ORDERED_QUANTITY * -1 ELSE OOLA.ORDERED_QUANTITY END AS FLOAT) AS ordered_qty,
        CAST(OOLA.PROMISE_DATE AS DATE) AS confirmed_ship_date,
        ROUND(OOLA.UNIT_SELLING_PRICE * OOLA.ORDERED_QUANTITY, 2) AS line_value,
        CAST(OOHA.ORDER_NUMBER AS VARCHAR) AS order_number,
        'Oracle CEN01' AS demand_erp, COALESCE(OOHA.TRANSACTIONAL_CURR_CODE, 'USD') AS currency,
        CASE WHEN CAST(OOLA.PROMISE_DATE AS DATE) < CURRENT_DATE() THEN 'Past Due' ELSE 'Open' END AS demand_status
    FROM FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_LINES_ALL OOLA
    JOIN FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_HEADERS_ALL OOHA ON OOLA.HEADER_ID = OOHA.HEADER_ID
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD ON OOLA.SHIP_FROM_ORG_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped MHS ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    WHERE OOHA.BOOKED_DATE >= '2024-01-01' AND OOHA.CONTEXT = 'ORDER'
      AND OOHA.ORG_ID IN ('18102','3241','10682','91','19480','8962')
      AND OOHA.ORDER_TYPE_ID NOT IN ('1187','1541','1405','1245','1308','1592','1201','1121')
      AND OOLA.LINE_TYPE_ID NOT IN ('1184','1182','1181','1006')
      AND OOLA.FLOW_STATUS_CODE IN ('AWAITING_SHIPPING','BOOKED','AWAITING_RECEIPT','AWAITING_RETURN')
      AND OOLA.ACTUAL_SHIPMENT_DATE IS NULL AND OOLA.ORDERED_ITEM IS NOT NULL
),
epicor_demand AS (
    SELECT UPPER(TRIM(SO_V.SO_ITEM_NUMBER)) AS item_id,
        SO_V.SO_PLANT AS demand_site, SO_V.SO_COMPANY AS DATAAREAID,
        CAST(SO_V.SO_ORDER_QTY AS FLOAT) AS ordered_qty,
        CAST(SO_V.SO_PROMISE_DATE AS DATE) AS confirmed_ship_date,
        ROUND(SO_V.SO_UNIT_PRICE * SO_V.SO_ORDER_QTY, 2) AS line_value,
        CAST(SO_V.SALEORDER_NUMBER AS VARCHAR) AS order_number,
        'Epicor' AS demand_erp, COALESCE(SO_V.SO_CURRENCY, 'USD') AS currency,
        CASE WHEN CAST(SO_V.SO_PROMISE_DATE AS DATE) < CURRENT_DATE() THEN 'Past Due' ELSE 'Open' END AS demand_status
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.VW_EPICOR_SALE_ORDER SO_V
    WHERE SO_V.SALEORDER_ORDERDATE >= '2024-01-01' AND SO_V.POFULFILLMENT_DATE IS NULL
      AND SO_V.SO_LINE_STATUS NOT IN ('Complete','Cancelled','Closed') AND SO_V.SO_ITEM_NUMBER IS NOT NULL
),
all_demand AS (SELECT * FROM d365_demand UNION ALL SELECT * FROM oracle_demand UNION ALL SELECT * FROM epicor_demand),
cross_site_match AS (
    SELECT d.item_id, d.demand_site, d.demand_erp, d.order_number,
        d.ordered_qty, d.confirmed_ship_date, d.line_value, d.currency, d.demand_status,
        DATEDIFF(day, d.confirmed_ship_date, CURRENT_DATE()) AS days_overdue,
        ROUND(d.line_value * COALESCE(fx.rate_to_dkk, CASE WHEN d.currency = 'DKK' THEN 1.0 ELSE NULL END, 1.0), 2) AS line_value_dkk,
        i.inv_company AS stock_site, i.entity AS stock_entity, i.warehouse,
        i.country AS stock_country, i.stock_erp, i.onhand_qty, i.product_family,
        CASE WHEN i.onhand_qty >= d.ordered_qty THEN 'Full Cover'
             WHEN i.onhand_qty > 0 THEN 'Partial Cover' ELSE 'No Cover' END AS coverage_type,
        ROUND(i.onhand_qty - d.ordered_qty, 2) AS qty_surplus
    FROM all_demand d
    JOIN inventory i ON i.item_id = d.item_id AND UPPER(TRIM(i.inv_company)) <> UPPER(TRIM(d.demand_site))
    LEFT JOIN fx_rates fx ON fx.from_currency = d.currency
    WHERE d.ordered_qty > 0
)
SELECT
    item_id, COALESCE(product_family, '') AS product_family,
    demand_erp, demand_site, order_number,
    ordered_qty, CAST(confirmed_ship_date AS VARCHAR) AS confirmed_ship_date,
    days_overdue, ROUND(line_value_dkk / 1000, 1) AS value_dkk_k,
    coverage_type, demand_status,
    stock_site, stock_entity, stock_country, stock_erp, warehouse,
    onhand_qty, qty_surplus
FROM cross_site_match
WHERE coverage_type IN ('Full Cover','Partial Cover')
ORDER BY
    CASE WHEN demand_status = 'Past Due' AND coverage_type = 'Full Cover' THEN 0
         WHEN demand_status = 'Past Due' AND coverage_type = 'Partial Cover' THEN 1
         WHEN coverage_type = 'Full Cover' THEN 2 ELSE 3 END,
    line_value_dkk DESC NULLS LAST
LIMIT 15000
"""

_pnu_path = os.path.join(os.path.dirname(__file__), "purchased_not_used_query.sql")
with open(_pnu_path, "r", encoding="utf-8") as _f:
    PURCHASED_NOT_USED_SQL = _f.read()

print("Running unified D365 + Oracle CEN01 + Epicor query (from speed_up_query.sql)...")
rows = run_query("Unified DIFOT + Business Line", UNIFIED_SQL)

print("Running cross-site inventory opportunity query...")
cs_rows = run_query("Cross-Site Opportunity", CROSS_SITE_SQL)

print("Running purchased-but-not-used query...")
pnu_rows = run_query("Purchased Not Used", PURCHASED_NOT_USED_SQL)

conn.close()
print()

sources = {}
for r in rows:
    src = r.get("erp_source", "Unknown")
    sources[src] = sources.get(src, 0) + 1

print(f"Total rows: {len(rows)}")
for src, cnt in sorted(sources.items()):
    print(f"  {src}: {cnt}")

# Strip columns the frontend never reads (reduces file size significantly)
DROP_COLS = {"line_num", "shipped_qty", "requested_ship_date", "site", "line_value"}
rows = [{k: v for k, v in r.items() if k not in DROP_COLS} for r in rows]

out_path = os.path.join(OUTPUT, "speed_up.json")
with open(out_path, "w", encoding="utf-8") as f:
    json.dump(rows, f, separators=(',', ':'), default=str)

size_mb = os.path.getsize(out_path) / 1e6
print(f"\nSaved to frontend/public/data/speed_up.json ({size_mb:.0f} MB)")

cs_path = os.path.join(OUTPUT, "cross_site.json")
with open(cs_path, "w", encoding="utf-8") as f:
    json.dump(cs_rows, f, separators=(',', ':'), default=str)
size_cs = os.path.getsize(cs_path) / 1e6
print(f"Saved to frontend/public/data/cross_site.json ({size_cs:.0f} MB)")

pnu_path = os.path.join(OUTPUT, "purchased_not_used.json")
with open(pnu_path, "w", encoding="utf-8") as f:
    json.dump(pnu_rows, f, separators=(',', ':'), default=str)
size_pnu = os.path.getsize(pnu_path) / 1e6
print(f"Saved to frontend/public/data/purchased_not_used.json ({size_pnu:.1f} MB)")
print("Next: cd ../frontend && npm run build && npx vercel --prod --yes")
