

/* ══════════════════════════════════════════════════════════════
   SHARED CTEs  —  all dedup'd to 1 row per join key
   ══════════════════════════════════════════════════════════════ */

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
    SELECT
        segment1,
        inventory_item_id,
        organization_id,
        min_minmax_quantity,
        max_minmax_quantity
    FROM FLS_PROD_DB.RAW_CEN01.APPS_MTL_SYSTEM_ITEMS_B
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY inventory_item_id, organization_id
        ORDER BY last_update_date DESC, last_updated_by DESC
    ) = 1
),

/* ship_to: dedup to 1 ship-to name per header_id */
ship_to AS (
    SELECT header_id, ship_to_name
    FROM (
        SELECT
            ooha.header_id,
            hps_st.party_name AS ship_to_name,
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
    SELECT
        e.OBJECT_NAME,
        d.PRODUCT_LINE_NAME,
        d.PRODUCT_TYPOLOGY_NAME
    FROM FLS_PROD_DB.MART_ENOVIA.EDW_ENOVIA_VW_PARTS_CURR       e
    LEFT JOIN FLS_DEV_DB.MART_DATA_QUALITY.DIM_Product_Typology d
      ON d.PRODUCT_TYPOLOGY_NAME = e.PRODUCT_CODE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY e.OBJECT_NAME
        ORDER BY e.modification_info_datetime DESC
    ) = 1
),

/* HFM master dedup — join key is COMPANY code; keep latest */
mhs_deduped AS (
    SELECT COMPANY, LOCATION, COMPANY_NAME, HFM_LOCATION
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.TBL_HFM_MASTER_SALEORDER
    QUALIFY ROW_NUMBER() OVER (PARTITION BY COMPANY ORDER BY COMPANY) = 1
),

/* Order-type typology dedup */
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

/* PO chain — dedup to 1 PO per order line */
po_chain AS (
    SELECT
        PRDA.ATTRIBUTE13        AS OOLA_LINE_ID_TXT,
        PHA.SEGMENT1            AS PO_NUMBER,
        PLA.LINE_NUM            AS PO_LINE_NUM,
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

/* Expediting — dedup per PO/line */
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

/* People — dedup per person_id */
per_deduped AS (
    SELECT PERSON_ID, FULL_NAME
    FROM FLS_PROD_DB.RAW_CEN01.PER_ALL_PEOPLE_F
    WHERE CURRENT_DATE BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
    QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY EFFECTIVE_START_DATE DESC) = 1
),

/* ══════════════════════════════════════════════════════════════
   D365 LOAD-LINE LOGIC (Eagle-style LoadDateSupport)
   Combines load line (LOAD_DETAILS) with load header (LOADS) and
   derives the shipping-date-equivalent used as TurnedInDate for D365.
   Deduped to 1 row per (INVENTTRANSID, DATAAREAID).
   ══════════════════════════════════════════════════════════════ */
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

/* ══════════════════════════════════════════════════════════════
   BL PROFILE CTEs
   ══════════════════════════════════════════════════════════════ */

item_bl_profile AS (
    SELECT
        OBL.ORDERED_ITEM,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'PCV'      THEN 1 ELSE 0 END) AS pcv_n,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'Service'  THEN 1 ELSE 0 END) AS service_n,
        SUM(CASE WHEN OBL.assigned_business_line_name = 'Products' THEN 1 ELSE 0 END) AS products_n,
        COUNT(*)                                                                       AS total_n
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
        COUNT(*)                                                                       AS total_n
    FROM FLS_DEV_DB.MART_MY_ORDERS.ORDER_WITH_BUSINESS_LINE OBL
    JOIN enovia_typology ENO
      ON ENO.OBJECT_NAME = OBL.ORDERED_ITEM
    WHERE OBL.assigned_business_line_name IN ('PCV','Service','Products')
      AND ENO.PRODUCT_LINE_NAME IS NOT NULL
    GROUP BY ENO.PRODUCT_LINE_NAME
),

inv_company_mapping AS (
    SELECT ORDER_DATAAREAID, INV_COMPANY
    FROM (
        VALUES
            ('FLS158',   'FLS158'),
            ('FLS309',   'FLS309'),
            ('FLS440',   'FLS440'),
            ('FLS936A',  'FLS936A'),
            ('103 FLSmidth AS',                        '103 FLSmidth AS'),
            ('114 FLSmidth Germany GmbH',              '114 FLSmidth Germany GmbH'),
            ('287 FLSmidth A/S',                       '287 FLSmidth A/S'),
            ('311 FLSmidth Ltd.',                      '311 FLSmidth Ltd.'),
            ('451 FLSmidth Cemento Mexico S.A. de C.V.','451 FLSmidth Cemento Mexico S.A. de C.V.'),
            ('453 FLSmidth Cement India LLP',          '453 FLSmidth Cement India LLP'),
            ('525 FLS USA Inc.',                       '525 FLS USA Inc.'),
            ('532 FLSmidth S.A. de C.V.',              '532 FLSmidth S.A. de C.V.'),
            ('544 FLSmidth Private Limited',           '544 FLSmidth Private Limited'),
            ('923 FLSmidth Inc.',                      '923 FLSmidth Inc.'),
            ('923 FLSmidth Inc. Minerals',             '923 FLSmidth Inc. Minerals'),
            ('us2',   'FLSmidth Inc.'),
            ('in2',   'FLSmidth Private Ltd.'),
            ('mx1',   'FLSmidth S.A. de C.V.'),
            ('sa1',   'Saudi FLSmidth Co'),
            ('za4',   'FLSmidth (Pty) Ltd - South Africa'),
            ('za3',   'FLSmidth (Pty) Ltd.'),
            ('pcpl',  NULL),
            ('gh1',   NULL)
    ) AS m(ORDER_DATAAREAID, INV_COMPANY)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(ORDER_DATAAREAID) ORDER BY INV_COMPANY) = 1
),

inv_enriched AS (
    SELECT
        inv.COMPANY                         AS INV_COMPANY,
        inv.PARTNUM                         AS INV_PARTNUM,
        inv."Country"                       AS INV_COUNTRY,
        inv."Region"                        AS INV_REGION,
        inv.plant                           AS INV_PLANT,
        inv.PRODUCTFAMILY                   AS INV_PRODUCT_FAMILY,
        inv."Entity"                        AS INV_ENTITY,
        inv.hfm                             AS INV_HFM,
        COALESCE(
            prod1.business_line,
            prod2.business_line,
            prod3.business_line,
            prod4.business_line,
            CASE
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Pumps, Cyclones & Valves'        THEN 'PCV'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Crushing & Screening'             THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Common Parts'                      THEN 'Service'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Milling & Grinding'                THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Separation'                        THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Precious Metals Recovery'          THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Thickening & Filtration'           THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Pneumatic Transport'               THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Mine Shaft Systems (PL)'           THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Sampling Preparation & Analysis (Mining)' THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Sampling Preparation & Analysis'   THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Pyro'                              THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Pyromet'                           THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Grinding'                          THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Feeding & Dosing (Pfister)'        THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Process Control & Optimisation'    THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Material Handling'                 THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Gas Analysis & Emission Monitoring' THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Bulk Materials Logistics'          THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Packing (Ventomatic)'              THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Gears'                             THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Integration Scope'                 THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Bags and Cages (AFT)'              THEN 'Service'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Conveyors (PL)'                    THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Air Pollution Control'             THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Continuous Surface Mining'         THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Auxiliary'                         THEN 'Products'
                WHEN SPLIT_PART(inv.PRODUCTFAMILY, '.', 1) = 'Mill & Crusher Liners'             THEN 'Products'
            END,
            CASE MIC.NOME_SEGME
                WHEN 'PUMPS'                           THEN 'PCV'
                WHEN 'CYCLONES'                        THEN 'PCV'
                WHEN 'VALVES'                          THEN 'PCV'
                WHEN 'VALVES (ENGINEERING)'            THEN 'PCV'
                WHEN 'MANIFOLDS'                       THEN 'PCV'
                WHEN 'MILING'                          THEN 'Products'
                WHEN 'FILTRATION'                      THEN 'Products'
                WHEN 'ROLLER PRESS'                    THEN 'Products'
                WHEN 'PRECIOUS METALS RECOVERY'        THEN 'Products'
                WHEN 'FLOTATION'                       THEN 'Products'
                WHEN 'THICKENING'                      THEN 'Products'
                WHEN 'SIZERS'                          THEN 'Products'
                WHEN 'CRUSHING'                        THEN 'Products'
                WHEN 'PT SYSTEM'                       THEN 'Products'
                WHEN 'SAMPLING PREPARATION & ANALYSIS' THEN 'Products'
                WHEN 'AIRTECH'                         THEN 'Products'
                WHEN 'PYRO PROCESSING'                 THEN 'Products'
                WHEN 'PYROMET'                         THEN 'Products'
                WHEN 'FEEDER'                          THEN 'Products'
                WHEN 'GAS ANALYSIS & REPORTING'        THEN 'Products'
                WHEN 'CLASSIFICATION'                  THEN 'Products'
                WHEN 'GRINDING AND GEARS'              THEN 'Products'
                WHEN 'CEMENT'                          THEN 'Products'
                WHEN 'ESSA'                            THEN 'Products'
                WHEN 'PROCESS CONTROL AND OPTIMISATION' THEN 'Products'
                WHEN 'MAAG GEAR'                       THEN 'Products'
                WHEN 'WEAR & CONSUMABLES'              THEN 'Service'
                WHEN 'LINERS & PERFORMANCE PARTS'      THEN 'Service'
            END,
            CASE
                WHEN UPPER(LEFT(inv.PARTNUM,3)) = 'MMA' AND UPPER(inv.PARTDESCRIPTION) LIKE '%PUMP%'   THEN 'PCV'
                WHEN UPPER(LEFT(inv.PARTNUM,3)) = 'MMB' AND UPPER(inv.PARTDESCRIPTION) LIKE '%PUMP%'   THEN 'PCV'
                WHEN UPPER(LEFT(inv.PARTNUM,3)) = 'MMA' AND UPPER(inv.PARTDESCRIPTION) LIKE '%CYCLON%' THEN 'PCV'
                WHEN UPPER(LEFT(inv.PARTNUM,3)) = 'MMB' AND UPPER(inv.PARTDESCRIPTION) LIKE '%CYCLON%' THEN 'PCV'
                WHEN UPPER(LEFT(inv.PARTNUM,3)) = 'MMA' AND UPPER(inv.PARTDESCRIPTION) LIKE '%VALVE%'  THEN 'PCV'
                WHEN UPPER(LEFT(inv.PARTNUM,3)) = 'MMB' AND UPPER(inv.PARTDESCRIPTION) LIKE '%VALVE%'  THEN 'PCV'
                WHEN UPPER(LEFT(inv.PARTNUM,2)) IN ('UM','GM','HM','SM','RM','EM','VM','TG',
                                                    'G7','G10','G15','G20','G26','G33','G61') THEN 'PCV'
                ELSE NULL
            END,
            CASE
                WHEN inv.ID = 'DYNAMIC | D365' AND inv."Country" = 'India' AND inv.plant = 'Arakkonam' THEN 'PCV'
                WHEN inv.ID = 'DYNAMIC | D365' AND inv."Country" = 'India' AND inv.plant = 'Bawal'     THEN 'Products'
                WHEN inv.ID = 'ST01P' THEN 'Service'
            END
        )                                   AS INV_BUSINESS_LINE
    FROM EDW.INVENTORY_VW_MYINVENTORY inv
    LEFT JOIN (
        SELECT * FROM FLS_PROD_DB.MART_ENOVIA.EDW_ENOVIA_VW_PARTS_CURR
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY OBJECT_NAME
            ORDER BY MODIFICATION_INFO_DATETIME DESC
        ) = 1
    ) ENO
      ON ENO.OBJECT_NAME = inv.PARTNUM
    LEFT JOIN (
        SELECT C7_PRODUTO, MAIN_PROD, NOME_SEGME, 'SDBP11' AS id
        FROM FLS_PROD_DB.RAW_MICROSIGA.DBO_VW_SIEVO_PO_EXTRACTION
        WHERE MAIN_PROD IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY C7_PRODUTO ORDER BY MAIN_PROD) = 1
    ) MIC
      ON MIC.C7_PRODUTO = inv.partnum AND MIC.ID = inv.id
    LEFT JOIN (
        SELECT VENDOR_ITEM_NO_, ITEM_CATEGORY_CODE, 'NAVISION' AS ID
        FROM FLS_PROD_DB.RAW_NAVISION_MNG.DBO_FLSMIDTH_LIVE_ITEM
        QUALIFY ROW_NUMBER() OVER (PARTITION BY VENDOR_ITEM_NO_ ORDER BY ITEM_CATEGORY_CODE) = 1
    ) NAV
      ON inv.PARTNUM = NAV.VENDOR_ITEM_NO_ AND NAV.ID = inv.id
    LEFT JOIN (
        SELECT product_code, business_line
        FROM FLS_PROD_DB.RAW_SHAREPOINT.PRODUCT_TYPOLOGY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_code) ORDER BY business_line) = 1
    ) prod1
      ON UPPER(ENO.PRODUCT_CODE) = UPPER(prod1.product_code)
    LEFT JOIN (
        SELECT product_code, business_line
        FROM FLS_PROD_DB.RAW_SHAREPOINT.PRODUCT_TYPOLOGY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_code) ORDER BY business_line) = 1
    ) prod2
      ON prod1.product_code IS NULL
     AND UPPER(inv.product_typology) = UPPER(prod2.product_code)
    LEFT JOIN (
        SELECT product_code, business_line
        FROM FLS_PROD_DB.RAW_SHAREPOINT.PRODUCT_TYPOLOGY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_code) ORDER BY business_line) = 1
    ) prod3
      ON prod1.product_code IS NULL
     AND prod2.product_code IS NULL
     AND UPPER(NAV.ITEM_CATEGORY_CODE) = UPPER(prod3.product_code)
    LEFT JOIN (
        SELECT product_code, business_line
        FROM FLS_PROD_DB.RAW_SHAREPOINT.PRODUCT_TYPOLOGY
        QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(product_code) ORDER BY business_line) = 1
    ) prod4
      ON prod1.product_code IS NULL
     AND prod2.product_code IS NULL
     AND prod3.product_code IS NULL
     AND UPPER(MIC.MAIN_PROD) = UPPER(prod4.product_code)
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

/* OBL (ORDER_WITH_BUSINESS_LINE) dedup — CRITICAL to prevent row fan-out
   on the outer join. Keep 1 row per (COMPANY, ORDER_NUMBER, LINE_ID,
   LINE_NUMBER, ORDERED_ITEM), preferring rows with non-null BL. */
obl_deduped AS (
    SELECT
        COMPANY,
        ORDER_NUMBER,
        LINE_ID,
        LINE_NUMBER,
        ORDERED_ITEM,
        assigned_business_line_name
    FROM FLS_DEV_DB.MART_MY_ORDERS.ORDER_WITH_BUSINESS_LINE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY COMPANY, ORDER_NUMBER, LINE_ID, LINE_NUMBER, ORDERED_ITEM
        ORDER BY CASE WHEN assigned_business_line_name IS NOT NULL THEN 0 ELSE 1 END
    ) = 1
),

/* ══════════════════════════════════════════════════════════════
   ORACLE CEN01 CTE
   ══════════════════════════════════════════════════════════════ */

ORACLE_ERP_CTE AS (
    SELECT
        OOLA.LINE_ID,
        OOHA.ORDER_NUMBER,
        OOLA.LINE_NUMBER,
        OOLA.ORDERED_ITEM,
        OOHA.BOOKED_DATE                                            AS CREATEDDATETIME,
        MM.MAX_MINMAX_QUANTITY                                      AS MAXINVENTONHAND,
        MM.MIN_MINMAX_QUANTITY                                      AS MININVENTONHAND,
        SUBQUERYPAF2.FULL_NAME                                      AS MODIFIEDBY,
        NULL                                                        AS MODIFIEDDATETIME,
        CASE
            WHEN OOLA.FLOW_STATUS_CODE IN ('AWAITING_RETURN','AWAITING_RECEIPT',
                                           'AWAITING_SHIPPING','BOOKED')
              AND DDO.DIFOT_DATE IS NULL     THEN 'Open'
            WHEN OOLA.FLOW_STATUS_CODE IN ('AWAITING_RETURN','AWAITING_RECEIPT',
                                           'AWAITING_SHIPPING','BOOKED')
              AND DDO.DIFOT_DATE IS NOT NULL THEN 'Closed'
            WHEN OOLA.FLOW_STATUS_CODE IN ('CLOSED','FULFILLED')   THEN 'Closed'
            WHEN OOLA.FLOW_STATUS_CODE = 'CANCELLED'               THEN 'Cancelled'
            ELSE NULL
        END                                                         AS SALESSTATUS,
        NULL                                                        AS DELIVERYTYPE,
        MOT."ORDER OFFERING (LEVEL 2)"                              AS OFFERINGTYPEVALUE,
        OOLA.UNIT_SELLING_PRICE                                     AS SALESPRICE,
        ROUND(
            CASE WHEN OOLA.LINE_CATEGORY_CODE = 'RETURN'
                THEN OOLA.ORDERED_QUANTITY * -1
                ELSE OOLA.ORDERED_QUANTITY
            END, 2
        )                                                           AS ORDERED_QUANTITY,
        CAST(OOHA.BOOKED_DATE AS DATE)                              AS CreatedDate,
        CAST(OOLA.REQUEST_DATE AS DATE)                             AS RequestShippingDate,
        CAST(OOLA.PROMISE_DATE AS DATE)                             AS ConfirmedShippingDate,
        NULL                                                        AS RequestedReceiptDate,
        CAST(OOLA.ACTUAL_SHIPMENT_DATE AS DATE)                     AS RequestedConfirmedDate,
        COALESCE(MHS.LOCATION, OOD.ORGANIZATION_CODE)               AS INVENTSITEID,
        NULL                                                        AS INVENTLOCATIONID,
        COALESCE(MHS.COMPANY_NAME, OOD.ORGANIZATION_CODE)           AS DATAAREAID,
        HCA_PARTY.PARTY_NAME                                        AS CustName,
        OOHA.CUST_PO_NUMBER,
        SHIP_TO.SHIP_TO_NAME                                        AS DELIVERYNAME,
        OOHA.FOB_POINT_CODE                                         AS DLVTERMID,
        NULL                                                        AS LOADID,
        CAST(OOHA.BOOKED_DATE AS DATE)                              AS ShipmentCreatedDate,
        NULL                                                        AS SHIPMENTID,
        CASE WHEN POC.PO_NUMBER IS NOT NULL THEN 'PO' ELSE 'Stock' END AS REFERENCE,
        CASE WHEN POC.PO_NUMBER IS NOT NULL THEN 'Purchase' ELSE 'Inventory' END AS ExecutionType,
        COALESCE(
            SIFOT.ACTUAL_RECEIPT_DATE,
            EWBEXP."PACKSLIP CREATION DATE",
            LEAST(
                TO_DATE(LEFT(SHP.SUPPLIER_SHIP_DATE, 10)),
                TO_DATE(LEFT(RCT.ACTUAL_RECEIPT_DATE, 10))
            )
        )                                                           AS TurnedInDate,
        COALESCE(MHS.COMPANY_NAME, OOD.ORGANIZATION_CODE)           AS ENTITY_NAME,
        ENO_O.PRODUCT_LINE_NAME                                     AS PRODUCT_LINE_NAME,
        COALESCE(OOHA.TRANSACTIONAL_CURR_CODE, 'USD')               AS CURRENCY

    FROM FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_LINES_ALL               OOLA
    JOIN FLS_PROD_DB.RAW_CEN01.ONT_OE_ORDER_HEADERS_ALL             OOHA
      ON OOLA.HEADER_ID = OOHA.HEADER_ID
    LEFT JOIN FLS_PROD_DB.RAW_CEN01.APPS_ORG_ORGANIZATION_DEFINITIONS OOD
      ON OOLA.SHIP_FROM_ORG_ID = OOD.ORGANIZATION_ID
    LEFT JOIN mhs_deduped MHS
      ON OOD.ORGANIZATION_CODE = MHS.COMPANY
    LEFT JOIN xott_deduped XOTT
      ON COALESCE(OOLA.ATTRIBUTE8, OOHA.ATTRIBUTE15) = XOTT.ID
    LEFT JOIN mot_deduped MOT
      ON XOTT.CONCATENATED_VALUE = MOT.CODE
    LEFT JOIN mtl_deduped MTL
      ON TRIM(OOLA.ATTRIBUTE20) = MTL.CATEGORY_ID
    LEFT JOIN ptm_deduped PTM
      ON MTL.DESCRIPTION = PTM."PRODUCT TYPOLOGY"
    LEFT JOIN hca_party HCA_PARTY
      ON OOHA.SOLD_TO_ORG_ID = HCA_PARTY.CUST_ACCOUNT_ID
    LEFT JOIN ship_to SHIP_TO
      ON OOHA.HEADER_ID = SHIP_TO.HEADER_ID
    LEFT JOIN minmax MM
      ON MM.SEGMENT1        = OOLA.ORDERED_ITEM
     AND MM.ORGANIZATION_ID = OOD.ORGANIZATION_ID
    LEFT JOIN per_deduped SUBQUERYPAF2
      ON OOHA.ATTRIBUTE16 = SUBQUERYPAF2.PERSON_ID
    LEFT JOIN difot_deduped DDO
      ON TO_CHAR(OOHA.ORDER_NUMBER) || TO_CHAR(OOLA.LINE_NUMBER) = DDO.ORDER_CONCAT
    /* PO chain — deduped, one PO per OOLA line */
    LEFT JOIN po_chain POC
      ON OOLA.LINE_ID = TO_NUMBER(POC.OOLA_LINE_ID_TXT)
    LEFT JOIN ewbexp_deduped EWBEXP
      ON POC.PO_NUMBER = EWBEXP."PO NUMBER"
     AND POC.PO_LINE_NUM = EWBEXP."PO LINE NUMBER"
    LEFT JOIN sifot_deduped SIFOT
      ON POC.PO_NUMBER = SIFOT."PO_NUMBER"
     AND POC.PO_LINE_NUM = SIFOT."PO_LINE_NUMBER"
    LEFT JOIN (
        SELECT RSL.PO_LINE_ID, MAX(RSH.SHIPPED_DATE) AS SUPPLIER_SHIP_DATE
        FROM FLS_PROD_DB.RAW_CEN01.RCV_SHIPMENT_LINES   RSL
        JOIN FLS_PROD_DB.RAW_CEN01.RCV_SHIPMENT_HEADERS RSH
          ON RSL.SHIPMENT_HEADER_ID = RSH.SHIPMENT_HEADER_ID
        WHERE RSH.SHIPPED_DATE IS NOT NULL
        GROUP BY RSL.PO_LINE_ID
    ) SHP
      ON POC.PO_LINE_ID = SHP.PO_LINE_ID
    LEFT JOIN (
        SELECT RT.PO_LINE_ID, MAX(RT.TRANSACTION_DATE) AS ACTUAL_RECEIPT_DATE
        FROM FLS_PROD_DB.RAW_CEN01.RCV_TRANSACTIONS RT
        WHERE RT.TRANSACTION_TYPE = 'RECEIVE'
        GROUP BY RT.PO_LINE_ID
    ) RCT
      ON POC.PO_LINE_ID = RCT.PO_LINE_ID
    LEFT JOIN enovia_typology ENO_O
      ON ENO_O.OBJECT_NAME = REGEXP_REPLACE(OOLA.ORDERED_ITEM, '-[A-Z]{2,4}[0-9]{0,2}$', '')

    WHERE OOHA.BOOKED_DATE   >= '2025-01-01'
      AND OOHA.CONTEXT        = 'ORDER'
      AND OOHA.ORG_ID        IN ('18102','3241','10682','91','19480','8962')
      AND OOHA.ORDER_TYPE_ID NOT IN ('1187','1541','1405','1245','1308','1592','1201','1121')
      AND OOLA.LINE_TYPE_ID  NOT IN ('1184','1182','1181','1006')
      AND OOLA.LINE_ID        IS NOT NULL
),

/* ══════════════════════════════════════════════════════════════
   D365 CTE
   TurnedInDate changed: was CAST(W.WORKCLOSEDUTCDATETIME AS DATE),
   now derived from ranked_load_line.LOADDATESUPPORT (Eagle-style).
   WORK join removed — no longer needed.
   ══════════════════════════════════════════════════════════════ */

D365_CTE AS (
    SELECT
        CAST(OL.SALESID AS STRING)                  AS LINE_ID,
        SO.SALESID                                  AS ORDER_NUMBER,
        OL.LINENUM                                  AS LINE_NUMBER,
        OL.ITEMID                                   AS ORDERED_ITEM,
        SO.CREATEDDATETIME,
        IC.MAXINVENTONHAND,
        IC.MININVENTONHAND,
        IC.MODIFIEDBY,
        IC.MODIFIEDDATETIME,
        CASE
            WHEN OL.SALESSTATUS = 3 THEN 'Invoiced'
            WHEN OL.SALESSTATUS = 1 THEN 'Open Order'
            WHEN OL.SALESSTATUS = 4 THEN 'Canceled'
            WHEN OL.SALESSTATUS = 0 THEN 'NULL'
            WHEN OL.SALESSTATUS = 2 THEN 'Delivered'
        END                                         AS SALESSTATUS,
        CASE
            WHEN OL.DELIVERYTYPE = 0 THEN 'None'
            WHEN OL.DELIVERYTYPE = 1 THEN 'DropShip'
        END                                         AS DELIVERYTYPE,
        COALESCE(
            DCS_LINE.ORDERTYPOLOGYVALUE,
            DCS_LINE.OFFERINGTYPEVALUE
        )                                           AS OFFERINGTYPEVALUE,
        OL.SALESPRICE,
        OL.QTYORDERED                               AS ORDERED_QUANTITY,
        CAST(OL.CREATEDDATETIME AS DATE)            AS CreatedDate,
        CAST(OL.SHIPPINGDATEREQUESTED AS DATE)      AS RequestShippingDate,
        CAST(OL.ShippingDateConfirmed AS DATE)      AS ConfirmedShippingDate,
        CAST(OL.ReceiptDateRequested AS DATE)       AS RequestedReceiptDate,
        CAST(OL.receiptdateconfirmed AS DATE)       AS RequestedConfirmedDate,
        ID.INVENTSITEID,
        ID.INVENTLOCATIONID,
        OL.DATAAREAID,
        dpt.name                                    AS CustName,
        C.ACCOUNTNUM                                AS CUST_PO_NUMBER,
        S.deliveryname,
        S.DLVTERMID,
        S.LOADID,
        CAST(S.CREATEDDATETIME AS DATE)             AS ShipmentCreatedDate,
        S.SHIPMENTID,
        CASE OL.inventreftype
            WHEN 0 THEN NULL             WHEN 1 THEN 'Sales order'
            WHEN 2 THEN 'Purchase order' WHEN 3 THEN 'Production'
            WHEN 4 THEN 'Production line' WHEN 5 THEN 'Inventory journal'
            WHEN 6 THEN 'Sales quotation' WHEN 7 THEN 'Transfer order'
            WHEN 8 THEN 'Fixed asset'    ELSE 'Unknown'
        END                                         AS REFERENCE,
        CASE
            WHEN OL.INVENTREFTYPE = 1 THEN 'Sales order-driven'
            WHEN OL.INVENTREFTYPE = 3 THEN 'Production-driven'
            WHEN OL.INVENTREFTYPE = 0 THEN 'Standard / No reference'
        END                                         AS ExecutionType,
        /* ▼▼▼ TurnedInDate: now derived from LoadDateSupport (Eagle-style) ▼▼▼ */
        CASE
            WHEN RLL.LOADDATESUPPORT IS NULL
              OR RLL.LOADDATESUPPORT = '1900-01-01'
              OR RLL.LOADDATESUPPORT = '9999-01-01'
            THEN NULL
            ELSE RLL.LOADDATESUPPORT
        END                                         AS TurnedInDate,
        /* ▲▲▲ end change ▲▲▲ */
        COALESCE(ID.INVENTSITEID, OL.DATAAREAID)    AS ENTITY_NAME,
        ENO_D.PRODUCT_LINE_NAME                     AS PRODUCT_LINE_NAME,
        COALESCE(SO.CURRENCYCODE, 'USD')            AS CURRENCY
    FROM "FLS_PROD_DB"."MART_DYN_FO".ORDER_LINES               OL
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".SALES_ORDERS         SO
      ON SO.SALESID    = OL.SALESID    AND SO.DATAAREAID  = OL.DATAAREAID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".CUSTOMERS            C
      ON SO.INVOICEACCOUNT = C.ACCOUNTNUM AND SO.DATAAREAID = C.DATAAREAID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".GLOBAL_ADDRESS_BOOK  dpt
      ON dpt.RECID = C.PARTY
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".DIMENSION_CODE_SET   DCS_LINE
      ON OL.DEFAULTDIMENSION = DCS_LINE.RECID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".INVENTORY_DIMENSIONS ID
      ON ID.INVENTDIMID = OL.INVENTDIMID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".LOAD_DETAILS         LD
      ON LD.INVENTTRANSID = OL.INVENTTRANSID AND LD.DATAAREAID = OL.DATAAREAID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".SHIPMENTS            S
      ON S.SHIPMENTID = LD.SHIPMENTID AND S.DATAAREAID = LD.DATAAREAID
    /* WORK join removed — TurnedInDate no longer uses WORKCLOSEDUTCDATETIME */
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".ITEM_COVERAGE        IC
      ON IC.ITEMID = OL.ITEMID AND IC.DATAAREAID = OL.DATAAREAID
    LEFT JOIN "FLS_PROD_DB"."MART_DYN_FO".FLS_LEGAL_ENTITY     LE
      ON LE.DATAAREAID = OL.DATAAREAID
    LEFT JOIN enovia_typology ENO_D
      ON ENO_D.OBJECT_NAME = OL.ITEMID
    /* New join: Eagle-style LoadDateSupport → TurnedInDate */
    LEFT JOIN ranked_load_line RLL
      ON RLL.INVENTTRANSID = OL.INVENTTRANSID
     AND RLL.DATAAREAID    = OL.DATAAREAID
    WHERE OL.CREATEDDATETIME >= '2025-01-01'
),

/* ══════════════════════════════════════════════════════════════
   EPICOR CTE
   ══════════════════════════════════════════════════════════════ */

EPICOR_CTE AS (
    SELECT
        CAST(SALEORDER_NUMBER AS STRING)
            || '-' || CAST(SALEORDER_LINE AS STRING)            AS LINE_ID,
        CAST(SALEORDER_NUMBER AS STRING)                        AS ORDER_NUMBER,
        SALEORDER_LINE                                          AS LINE_NUMBER,
        SO_ITEM_NUMBER                                          AS ORDERED_ITEM,
        CAST(SALEORDER_ORDERDATE AS TIMESTAMP)                  AS CREATEDDATETIME,
        COALESCE(NULLIF(PP.MAXIMUMQTY, 0), NULLIF(PW.MAXIMUMQTY, 0)) AS MAXINVENTONHAND,
        COALESCE(NULLIF(PP.MINIMUMQTY, 0), NULLIF(PW.MINIMUMQTY, 0)) AS MININVENTONHAND,
        ORDER_HANDLER                                           AS MODIFIEDBY,
        NULL                                                    AS MODIFIEDDATETIME,
        SO_LINE_STATUS                                          AS SALESSTATUS,
        CASE WHEN DROPSHIP = 'true' THEN 'DropShip' ELSE 'None' END AS DELIVERYTYPE,
        SO_ORDER_OFFERING                                       AS OFFERINGTYPEVALUE,
        SO_UNIT_PRICE                                           AS SALESPRICE,
        SO_ORDER_QTY                                            AS ORDERED_QUANTITY,
        CAST(SALEORDER_ORDERDATE AS DATE)                       AS CreatedDate,
        CAST(SO_NEEDBY_DATE AS DATE)                            AS RequestShippingDate,
        CAST(SO_PROMISE_DATE AS DATE)                           AS ConfirmedShippingDate,
        NULL                                                    AS RequestedReceiptDate,
        CAST(ACTUAL_SHIP_DATE AS DATE)                          AS RequestedConfirmedDate,
        SO_PLANT                                                AS INVENTSITEID,
        NULL                                                    AS INVENTLOCATIONID,
        SO_COMPANY                                              AS DATAAREAID,
        SO_CUSTOMER_NAME                                        AS CustName,
        CUSTOMER_PO_NUMBER                                      AS CUST_PO_NUMBER,
        NULL                                                    AS DELIVERYNAME,
        INCOTERM                                                AS DLVTERMID,
        NULL                                                    AS LOADID,
        CAST(ACTUAL_SHIP_DATE AS DATE)                          AS ShipmentCreatedDate,
        NULL                                                    AS SHIPMENTID,
        SO_LINE_SOURCE                                          AS REFERENCE,
        CASE SO_LINE_SOURCE
            WHEN 'Make'  THEN 'Manufacturing'
            WHEN 'PO'    THEN 'Purchase'
            WHEN 'Stock' THEN 'Inventory'
            ELSE SO_LINE_SOURCE
        END                                                     AS ExecutionType,
        CAST(POFULFILLMENT_DATE AS DATE)                        AS TurnedInDate,
        CASE SO_COMPANY
            WHEN 'FLS309'  THEN 'AU, Welshpool & Pinkenba & Beresfield'
            WHEN 'FLS158'  THEN 'CL, La Negra & Renca'
            WHEN 'FLS936A' THEN 'PE, Lima'
            WHEN 'FLS440'  THEN 'ID, Surabaya'
            ELSE SO_COMPANY
        END                                                     AS ENTITY_NAME,
        ENO_E.PRODUCT_LINE_NAME                                 AS PRODUCT_LINE_NAME,
        COALESCE(SO_V.SO_CURRENCY, 'USD')                       AS CURRENCY
    FROM FLS_SELFSERVICE_PROD_DB.PROCUREMENT_DATA.VW_EPICOR_SALE_ORDER SO_V
    LEFT JOIN epicor_partplant PP
      ON  PP.COMPANY = SO_V.SO_COMPANY
      AND PP.PARTNUM = SO_V.SO_ITEM_NUMBER
      AND UPPER(PP.PLANT) = UPPER(SO_V.SO_PLANT)
    LEFT JOIN epicor_partwhse PW
      ON  PW.COMPANY = SO_V.SO_COMPANY
      AND PW.PARTNUM = SO_V.SO_ITEM_NUMBER
      AND UPPER(PW.WAREHOUSECODE) = UPPER(SO_V.SO_PLANT)
    LEFT JOIN enovia_typology ENO_E
      ON ENO_E.OBJECT_NAME = SO_V.SO_ITEM_NUMBER
    WHERE SALEORDER_ORDERDATE >= '2025-01-01'
),

/* ══════════════════════════════════════════════════════════════
   UNION + FINAL DEDUP SAFETY NET
   ══════════════════════════════════════════════════════════════ */

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
        COUNT(*)                                                                       AS total_n
    FROM UNIONED SV
    JOIN obl_deduped OBL
      ON OBL.COMPANY       = SV.DATAAREAID
     AND OBL.ORDER_NUMBER  = SV.ORDER_NUMBER
     AND OBL.LINE_ID       = SV.LINE_ID
     AND OBL.LINE_NUMBER   = SV.LINE_NUMBER
     AND OBL.ORDERED_ITEM  = SV.ORDERED_ITEM
    WHERE OBL.assigned_business_line_name IN ('PCV','Service','Products')
      AND SV.INVENTSITEID IS NOT NULL
    GROUP BY SV.INVENTSITEID
),

/* ── Exchange rates → DKK (last month-end actual rate) ──────────────── */
fx_rates AS (
    SELECT
        xc.custom1  AS from_currency,
        xc.RATE     AS rate_to_dkk
    FROM FLS_PROD_DB.EDW.HFM_EXCHANGE_RATE_STG xc
    WHERE xc.custom2  = 'DKK'
      AND xc.year     = YEAR(ADD_MONTHS(SYSDATE(), -1))
      AND xc.period   = TO_VARCHAR(ADD_MONTHS(SYSDATE(), -1), 'Mon')
      AND xc.account  = 'ENDRATE'
      AND xc.scenario = 'ACT'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY xc.custom1 ORDER BY xc.custom1) = 1
),

FINAL_WITH_DUPS AS (
    SELECT
        SV.LINE_ID,
        SV.ORDER_NUMBER,
        SV.LINE_NUMBER,
        SV.ORDERED_ITEM,
        SV.CREATEDDATETIME,
        SV.MAXINVENTONHAND,
        SV.MININVENTONHAND,
        SV.MODIFIEDBY,
        SV.MODIFIEDDATETIME,
        SV.SALESSTATUS,
        SV.DELIVERYTYPE,
        SV.OFFERINGTYPEVALUE,
        SV.SALESPRICE,
        SV.ORDERED_QUANTITY,
        SV.CREATEDDATE,
        SV.REQUESTSHIPPINGDATE,
        SV.CONFIRMEDSHIPPINGDATE,
        SV.REQUESTEDRECEIPTDATE,
        SV.REQUESTEDCONFIRMEDDATE,
        SV.INVENTSITEID,
        SV.INVENTLOCATIONID,
        SV.DATAAREAID,
        SV.CUSTNAME,
        SV.CUST_PO_NUMBER,
        SV.DELIVERYNAME,
        SV.DLVTERMID,
        SV.LOADID,
        SV.SHIPMENTCREATEDDATE,
        SV.SHIPMENTID,
        SV.REFERENCE,
        SV.EXECUTIONTYPE,
        SV.TURNEDINDATE,
        SV.ENTITY_NAME,

        COALESCE(
            OBL.assigned_business_line_name,
            INV.INV_BUSINESS_LINE,
            CASE
                WHEN IBL.total_n > 0
                 AND GREATEST(IBL.pcv_n, IBL.service_n, IBL.products_n) * 1.0 / IBL.total_n >= 0.95
                THEN CASE
                        WHEN IBL.pcv_n      >= IBL.service_n AND IBL.pcv_n      >= IBL.products_n THEN 'PCV'
                        WHEN IBL.service_n  >= IBL.pcv_n     AND IBL.service_n  >= IBL.products_n THEN 'Service'
                        ELSE 'Products'
                     END
            END,
            CASE
                WHEN PBL.total_n >= 50
                 AND GREATEST(PBL.pcv_n, PBL.service_n, PBL.products_n) * 1.0 / PBL.total_n >= 0.95
                THEN CASE
                        WHEN PBL.pcv_n      >= PBL.service_n AND PBL.pcv_n      >= PBL.products_n THEN 'PCV'
                        WHEN PBL.service_n  >= PBL.pcv_n     AND PBL.service_n  >= PBL.products_n THEN 'Service'
                        ELSE 'Products'
                     END
            END,
            CASE
                WHEN SBL.total_n >= 100
                 AND GREATEST(SBL.pcv_n, SBL.service_n, SBL.products_n) * 1.0 / SBL.total_n >= 0.95
                THEN CASE
                        WHEN SBL.pcv_n      >= SBL.service_n AND SBL.pcv_n      >= SBL.products_n THEN 'PCV'
                        WHEN SBL.service_n  >= SBL.pcv_n     AND SBL.service_n  >= SBL.products_n THEN 'Service'
                        ELSE 'Products'
                     END
            END,
            CASE
                WHEN IBL.total_n > 0
                 AND GREATEST(IBL.pcv_n, IBL.service_n, IBL.products_n) * 1.0 / IBL.total_n >= 0.70
                THEN CASE
                        WHEN IBL.pcv_n      >= IBL.service_n AND IBL.pcv_n      >= IBL.products_n THEN 'PCV'
                        WHEN IBL.service_n  >= IBL.pcv_n     AND IBL.service_n  >= IBL.products_n THEN 'Service'
                        ELSE 'Products'
                     END
            END,
            CASE
                WHEN SBL.total_n >= 100
                 AND GREATEST(SBL.pcv_n, SBL.service_n, SBL.products_n) * 1.0 / SBL.total_n >= 0.70
                THEN CASE
                        WHEN SBL.pcv_n      >= SBL.service_n AND SBL.pcv_n      >= SBL.products_n THEN 'PCV'
                        WHEN SBL.service_n  >= SBL.pcv_n     AND SBL.service_n  >= SBL.products_n THEN 'Service'
                        ELSE 'Products'
                     END
            END
        )                                                                AS BUSINESS_LINE,

        SV.PRODUCT_LINE_NAME,
        SV.SOURCE,
        SV.CURRENCY,
        ROUND(SV.SALESPRICE * SV.ORDERED_QUANTITY * COALESCE(
            FX.rate_to_dkk,
            CASE WHEN SV.CURRENCY = 'DKK' THEN 1.0 ELSE NULL END,
            1.0
        ), 2)                                                                AS LINE_VALUE_DKK

    FROM UNIONED SV
    LEFT JOIN fx_rates FX
      ON FX.from_currency = SV.CURRENCY
    LEFT JOIN obl_deduped OBL
      ON OBL.COMPANY       = SV.DATAAREAID
     AND OBL.ORDER_NUMBER  = SV.ORDER_NUMBER
     AND OBL.LINE_ID       = SV.LINE_ID
     AND OBL.LINE_NUMBER   = SV.LINE_NUMBER
     AND OBL.ORDERED_ITEM  = SV.ORDERED_ITEM
    LEFT JOIN inv_company_mapping MAP
      ON UPPER(MAP.ORDER_DATAAREAID) = UPPER(SV.DATAAREAID)
    LEFT JOIN inv_enriched INV
      ON UPPER(INV.INV_COMPANY) = UPPER(MAP.INV_COMPANY)
     AND INV.INV_PARTNUM = SV.ORDERED_ITEM
    LEFT JOIN item_bl_profile        IBL ON IBL.ORDERED_ITEM      = SV.ORDERED_ITEM
    LEFT JOIN productline_bl_profile PBL ON PBL.PRODUCT_LINE_NAME = SV.PRODUCT_LINE_NAME
    LEFT JOIN site_bl_profile        SBL ON SBL.INVENTSITEID      = SV.INVENTSITEID
)

SELECT
    CAST(LINE_ID AS VARCHAR)                                              AS line_id,
    CAST(ORDER_NUMBER AS VARCHAR)                                         AS sales_order_id,
    CAST(LINE_NUMBER AS VARCHAR)                                          AS line_num,
    CAST(ORDERED_ITEM AS VARCHAR)                                         AS item_id,
    CAST(CREATEDDATE AS VARCHAR)                                          AS created_date,
    CAST(REQUESTSHIPPINGDATE AS VARCHAR)                                  AS requested_ship_date,
    CAST(CONFIRMEDSHIPPINGDATE AS VARCHAR)                                AS confirmed_ship_date,
    CAST(TURNEDINDATE AS VARCHAR)                                         AS goods_issue_date,
    CAST(ORDERED_QUANTITY AS VARCHAR)                                     AS ordered_qty,
    NULL                                                                  AS shipped_qty,
    CAST(ROUND(SALESPRICE * ORDERED_QUANTITY, 2) AS VARCHAR)              AS line_value,
    CAST(CURRENCY AS VARCHAR)                                             AS currency,
    CAST(LINE_VALUE_DKK AS VARCHAR)                                       AS line_value_dkk,
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
    PRODUCT_LINE_NAME                                                     AS product_line_name,
    -- OTS value bucket (USD equivalent via DKK rate 6.388, 06-May-2026)
    CASE
        WHEN LINE_VALUE_DKK / 6.388 <   1000 THEN '< $1K'
        WHEN LINE_VALUE_DKK / 6.388 <  10000 THEN '$1K – $10K'
        WHEN LINE_VALUE_DKK / 6.388 <  50000 THEN '$10K – $50K'
        WHEN LINE_VALUE_DKK / 6.388 < 100000 THEN '$50K – $100K'
        ELSE                                       '> $100K'
    END                                                                   AS order_value_bucket
FROM FINAL_WITH_DUPS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SOURCE, DATAAREAID, ORDER_NUMBER, LINE_ID, LINE_NUMBER, ORDERED_ITEM
    ORDER BY
        CASE WHEN BUSINESS_LINE IS NOT NULL THEN 0 ELSE 1 END,
        CASE WHEN PRODUCT_LINE_NAME IS NOT NULL THEN 0 ELSE 1 END,
        CREATEDDATETIME DESC NULLS LAST
) = 1
ORDER BY
    CASE WHEN goods_issue_date IS NULL THEN 0 ELSE 1 END,
    created_date DESC NULLS LAST
LIMIT 150000
