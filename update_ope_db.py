# -*- coding: utf-8 -*-
"""
Script that updates data_warehouse OPE tables with information from Google Analytics, Core databases, Netquest files
and data_warehouse tables (not the OPE ones)
"""

########################################################################################################################

# Import modules.

import yaml, sys, os
import numpy as np
import pandas as pd
import datetime
from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine

sys.path.append(os.path.realpath(os.path.dirname(__file__)))
import retrieve_info_from_GA as retrieveGA
import retrieve_info_from_DB as retrieveDB


########################################################################################################################

# Open needed connections.

with open("config.yml", 'r') as conf:
    config = yaml.load(conf) # Add 'Loader=yaml.FullLoader' to yaml.load() to skip Warning

db_bi = config['db_bi']
conn_bi = create_engine(URL('mysql', **db_bi))
db_core = config['db_core']
conn_core = create_engine(URL('postgresql', **db_core))


########################################################################################################################

# Functions needed.

def insert_new_data_to_DB(df, table, conn):
    """Function to insert new data to database"""

    # Find info already in DB
    already_in = pd.read_sql(
        f"""
        SELECT * FROM {table}
        """, conn)

    # Filter new info not already in DB and insert in database
    df_notInDB = df.append(already_in).drop_duplicates(keep=False)
    if len(df_notInDB) > 0:
        df_notInDB.to_sql(name=table, con=conn, if_exists="append", index=False)

    return


def find_updating_date(table, conn):
    """Function to find the day for updating start.
    We check last few days in DB just in case some new info has been uploaded"""

    checking_num_days = 7

    info_in_bd = pd.read_sql(
        f"""
        SELECT max(day) as max_day FROM {table}
        """, conn)

    update_date = (info_in_bd['max_day'][0] - datetime.timedelta(days=checking_num_days)).date()
    return update_date

########################################################################################################################


# Update OPE_GA table

# Find last date in DB and start querying one week before just in case new info has been added retrospectively
info_in_bd = pd.read_sql(
        f"""
        SELECT max(day) as max_day FROM OPE_GA
        """, conn_bi)

first_date = (datetime.datetime.strptime(info_in_bd['max_day'][0], "%Y-%m-%d") - datetime.timedelta(days=7)).date()
last_date = datetime.date.today()

# Retrieve new info from API GA
GA_data = retrieveGA.retrieve_GA_data(first_date, last_date)

GA_data['day'] = GA_data['datetime'].apply(lambda x: str(x)[:4] + '-' + str(x)[4:6] + '-' + str(x)[6:8])
GA_data['CMB_CMN'] = GA_data['CMB_CMN'].apply(pd.to_numeric)
GA_data['tarification'] = GA_data['tarification'].apply(pd.to_numeric)
GA_data['type'] = np.where(GA_data['CMB_CMN'] > 0, 'CMB_CMN',
                           np.where(GA_data['tarification'] > 0, 'tarification', 'miss'))
new_OPE_GA = GA_data.groupby(['day', 'type']).agg({'cookie':'count'}).reset_index()
new_OPE_GA = new_OPE_GA.rename(columns={'cookie': 'num_users'}).sort_values(['day', 'type'])

insert_new_data_to_DB(new_OPE_GA, 'OPE_GA', conn_bi)


# Update OPE_CALLS table
calls = retrieveDB.retrieve_calls_info(conn_bi)
insert_new_data_to_DB(calls, 'OPE_CALLS', conn_bi)


# Update OPE_SALES table
sales = retrieveDB.retrieve_sales_info(conn_bi)
insert_new_data_to_DB(sales, 'OPE_SALES', conn_bi)


# Update OPE_DROPS table
drops = retrieveDB.retrieve_drops_info(conn_bi)
insert_new_data_to_DB(drops, 'OPE_DROPS', conn_bi)


# Update OPE_LEADS table
update_date = find_updating_date('OPE_LEADS', conn_bi)
leads = retrieveDB.retrieve_leads_info(from_date=update_date, conn=conn_core)
insert_new_data_to_DB(leads, 'OPE_LEADS', conn_bi)