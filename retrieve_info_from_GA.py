"""
Module with functions for retrieving info from Google Analytics.
"""


import socket, datetime
import pandas as pd
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials


scope = 'https://www.googleapis.com/auth/analytics.readonly'
key_file_location = 'creds.json'
default_timeout = 1200  # set timeout to 20 minutes
max_days4query = 1



def get_service(api_name, api_version, scopes, key_file_location):
    """Get a service that communicates to a Google API.
    Args:
        api_name: The name of the api to connect to.
        api_version: The api version to connect to.
        scopes: A list auth scopes to authorize for the application.
        key_file_location: The path to a valid service account JSON key file.
    Returns:
        A service that is connected to the specified API.
    """

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
            key_file_location, scopes=scopes)

    # Build the service object.
    service = build(api_name, api_version, credentials=credentials)

    return service


def get_first_profile_id(service):
    # Use the Analytics service object to get the first profile id.

    # Get a list of all Google Analytics accounts for this user
    accounts = service.management().accounts().list().execute()

    if accounts.get('items'):
        # Get the first Google Analytics account.
        account = accounts.get('items')[0].get('id')

        # Get a list of all the properties for the first account.
        properties = service.management().webproperties().list(
                accountId=account).execute()

        if properties.get('items'):
            # Get the first property id.
            property = properties.get('items')[0].get('id')

            # Get a list of all views (profiles) for the first property.
            profiles = service.management().profiles().list(
                    accountId=account,
                    webPropertyId=property).execute()

            if profiles.get('items'):
                # return the first view (profile) id.
                return profiles.get('items')[0].get('id')

    return None


def get_results(results):
    # results: query to be send to GA API

    if results:
        GA_data = results.get('rows')
        total_rows = results.get('totalResults')
        try:
            print('Sample percentage:', int(results.get('sampleSize'))/int(results.get('sampleSpace')),
                  ' from ', results.get('sampleSpace'))
        except:
            pass

    else:
        print('No results found')

    return (GA_data, total_rows)


def query_OPE_info(service, profile_id, start_date, end_date, start_index,
                   max_results4query=10000):
    """Queries needed info for cookie and timestamp to GA API:
    - Number of CMB/CMN
    - Number of tarifications"""

    return service.data().ga().get(
        ids='ga:' + profile_id,
        max_results=max_results4query,
        start_index=start_index,
        start_date=start_date,
        end_date=end_date,
        samplingLevel='HIGHER_PRECISION',
        # goal2Completions:CMB/CMN, goal19Completions: tarificacio
        metrics='ga:goal2Completions, ga:goal19Completions',
        # dimension13:cookie, dateHourMinute:timestamp
        dimensions='ga:dimension13, ga:dateHourMinute',
        # if ';' filter with AND, if ',' filter with OR
        filters='ga:goal2Completions>0,ga:goal19Completions>0',
        segment='users::condition::ga:country==Spain').execute()


def retrieve_GA_data(first_date, last_date):

    socket.setdefaulttimeout(default_timeout)  # set timeout to 20 minutes

    # Authenticate and construct service.
    service = get_service(
            api_name='analytics',
            api_version='v3',
            scopes=[scope],
            key_file_location=key_file_location)

    profile_id = get_first_profile_id(service)

    start_date = first_date
    start_index = 1
    GA_results = pd.DataFrame()

    while (start_date < last_date):
        end_date = start_date

        start_query = str(start_date.strftime('%Y-%m-%d'))
        end_query = str(end_date.strftime('%Y-%m-%d'))
        query_results = get_results(query_OPE_info(service, profile_id, start_query, end_query, start_index))

        total_rows = len(GA_results)
        GA_data = pd.DataFrame(query_results[0])
        GA_max_num_rows = query_results[1]

        GA_results = GA_results.append(GA_data)

        while ((total_rows + GA_max_num_rows) > len(GA_results)):
            start_index += len(GA_data)
            query_results = get_results(query_OPE_info(service, profile_id, start_query, end_query, start_index))
            GA_data = pd.DataFrame(query_results[0])
            GA_results = GA_results.append(GA_data)

        start_date = start_date + datetime.timedelta(days=max_days4query)
        start_index = 1

    GA_results.columns = ['cookie', 'datetime', 'CMB_CMN', 'tarification']
    return GA_results
