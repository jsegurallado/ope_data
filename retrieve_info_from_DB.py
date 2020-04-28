"""
Module with functions for retrieving info from DB.
"""

import pandas as pd

def retrieve_calls_info(conn):

    calls = pd.read_sql("""
    SELECT date(calldate) as date , call_type, count(distinct puid) as n_calls, 
    count(distinct source) as n_callers FROM MVC_ATC_DETAILS_FT A
    left join MVC_ATC_SERVICE_FT B ON A.service = B.service 
    where level_01 = '[Grupo de agentes]:Ventas'
    and disposition = 'ANSWERED'
    and duration > 60
    and date(calldate) > (SELECT MAX(date) from OPE_CALLS)
    group by date(calldate), call_type
        """, conn)

    return calls


def retrieve_sales_info(conn):

    electr_sales = pd.read_sql("""
        SELECT con.contract_id, 
        con.cups, 
        tar.tariff_name as tariff, 
        con.product_ekon_id as product, 
        con.sales_company_id, 
        sls.sales_company_name, 
        sls.channel_group, 
        'LIGHT' as business_type, 
        date(co_start_date) as contract_start_date, 
        esup.sp_zipcode as zipcode, 
        prov.province  
        FROM CON_ECONTRACT_DIM con
        left JOIN CON_TARIFF_DIM tar on con.tariff_ekon_id = tar.tariff_ekon_id 
        left join SLS_COMPANY_DIM sls on con.sales_company_id = sls.sales_company_id 
        left join CON_STATUS_DIM sta on con.co_status_id = sta.status_pk 
        left join CON_ESUPPLY_POINT_DIM esup on esup.contract_id = con.contract_id
        left join GEN_PROVINCE_DIM prov on left(esup.sp_zipcode, 2) = prov.province_nk
        /*left join OPE_SALES ope on con.contract_id = ope.contract_id*/
        WHERE registration_type IN('Cambio', 'Nueva Alta')
        /*and ope.contract_id is null*/
        /*and esup.sp_zipcode is not null*/
        """, conn)

    gas_sales = pd.read_sql("""
        SELECT con.contract_id, 
        con.cups, 
        tar.tariff_name as tariff, 
        tar.tariff_name as product, 
        COALESCE(con.sales_company_id, 9997) AS sales_company_id, 
        sls.sales_company_name, 
        sls.channel_group, 
        'GAS' as business_type, 
        date(co_start_date) as contract_start_date, 
        esup.sp_zipcode as zipcode, 
        prov.province  
        FROM CON_GCONTRACT_DIM con
        left JOIN CON_TARIFF_DIM tar on con.tariff_id = tar.tariff_pk
        left join SLS_COMPANY_DIM sls on COALESCE(con.sales_company_id, 9997) = sls.sales_company_id 
        left join CON_STATUS_DIM sta on con.co_status_id = sta.status_pk 
        left join CON_GSUPPLY_POINT_DIM esup on esup.contract_id = con.contract_id
        left join GEN_PROVINCE_DIM prov on left(esup.sp_zipcode, 2) = prov.province_nk
       /*LEFT JOIN OPE_SALES ope on ope.contract_id = con.contract_id*/
        WHERE new_client_flag = 1
        /*and ope.contract_id is null*/
        /*and esup.sp_zipcode is not null*/

        """, conn)

    self_consumption_sales = pd.read_sql("""
        select potential_id as contract_id,
        A.cups, 
        B.tariff_ekon_id as tariff, 
        A.product as product, 
        B.sales_company_id, 
        C.sales_company_name,
        C.channel_group, 
        'SELF-CONSUMPTION' AS business_type, 
        A.co_start_date as contract_start_date, 
        sp_zipcode as zipcode, 
        province_ekon_name as province 
        from SLS_SELF_CONSUMPTION A
        LEFT JOIN
        (WITH CONTRACTS AS( 
        SELECT contract_id, cups, date(co_start_date) as co_start_date, registration_type, tariff_ekon_id, sales_company_id,
        RANK() OVER(
        PARTITION BY(cups)
        order by co_start_date desc, contract_id desc
        ) as ranking
        FROM CON_ECONTRACT_DIM 
        WHERE registration_type IN('Cambio','Nueva Alta'))
        SELECT * FROM CONTRACTS
        WHERE ranking = 1) B ON A.cups = B.cups 
        left JOIN SLS_COMPANY_DIM C on B.sales_company_id = C.sales_company_id 
        left join CON_ESUPPLY_POINT_DIM D ON B.contract_id = D.contract_id
        left JOIN GEN_PROVINCE_DIM E ON left(sp_zipcode, 2) = province_nk""", conn)

    sales = electr_sales.append(gas_sales).append(self_consumption_sales)

    return sales


def retrieve_drops_info(conn):

    electr_drops = pd.read_sql("""
        SELECT con.contract_id, con.cups, tar.tariff_name as tariff, product_ekon_id as product, con.sales_company_id, 
        sls.sales_company_name, sls.channel_group, 'LIGHT' as business_type, 
        date(con.supply_end_date) as supply_end_date, esup.sp_zipcode as zipcode, prov.province, 
        lea.description  FROM CON_ECONTRACT_DIM con
        left JOIN CON_TARIFF_DIM tar on con.tariff_ekon_id = tar.tariff_ekon_id 
        left join SLS_COMPANY_DIM sls on con.sales_company_id = sls.sales_company_id 
        left join CON_ESUPPLY_POINT_DIM esup on esup.contract_id = con.contract_id
        left join GEN_PROVINCE_DIM prov on left(esup.sp_zipcode, 2) = prov.province_nk 
        left join CON_LEAVING_REASON_DIM lea on con.leaving_reason_id = lea.leaving_reason_id
        LEFT JOIN OPE_DROPS ope on ope.contract_id = con.contract_id
        where CUPS_out_of_Holaluz_flag = 1
        and con.co_status_id IN(9,10)
        and ope.contract_id is null
        and esup.sp_zipcode is not null
        """, conn)

    gas_drops = pd.read_sql("""
        SELECT con.contract_id, con.cups, tariff_name as tariff, tariff_name as product, 
        COALESCE(con.sales_company_id, 9997) AS sales_company_id, sls.sales_company_name, 
        sls.channel_group, 'GAS' as business_type, date(con.supply_end_date) as supply_end_date, 
        esup.sp_zipcode as zipcode, prov.province, co_state_detail as description FROM CON_GCONTRACT_DIM con
        left JOIN CON_TARIFF_DIM tar on con.tariff_id = tar.tariff_pk
        left join SLS_COMPANY_DIM sls on COALESCE(con.sales_company_id, 9997) = sls.sales_company_id
        left join CON_STATUS_DIM sta on con.co_status_id = sta.status_pk
        left join CON_GSUPPLY_POINT_DIM esup on esup.contract_id = con.contract_id
        left join GEN_PROVINCE_DIM prov on left(esup.sp_zipcode, 2) = prov.province_nk
        LEFT JOIN OPE_DROPS ope on ope.contract_id = con.contract_id
        where co_state_detail IN('terminated_new_sales_company', 'terminated_definitive', 'terminated_unpaid', 
        'terminated_withdrawal')
        and ope.contract_id is null
        and esup.sp_zipcode is not null

        """, conn)

    drops = electr_drops.append(gas_drops)

    return drops


def retrieve_leads_info(from_date, conn):

    leads_luz = pd.read_sql(
        f"""
        select day, product, source, count(*) as num_leads from
        (select cups, product, day, source from
        (select cups, product, lead_id, date_trunc('day', created_at) as day
        from core.extended_leads where created_at > '{from_date}') as a
        left join
        (select lead_id, source from lead.journey 
        where business_unit = 'electricity' and created_at > '{from_date}') as b
        on a.lead_id = b.lead_id) as c
        group by day, product, source
        order by day, product, source
        """, conn)

    leads_gas = pd.read_sql(
        f"""
        select day, product, source, count(*) as num_leads from
        (select business_unit as product, source, date_trunc('day', created_at) as day from lead.journey 
        where business_unit = 'gas' and created_at > '{from_date}') as a
        group by day, product, source
        order by day, product, source
        """, conn)

    leads = leads_luz.append(leads_gas)

    return leads