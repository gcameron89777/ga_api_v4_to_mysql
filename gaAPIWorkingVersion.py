#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 13 15:00:26 2018

@author: gcameron
"""

# cd /Users/gcameron/Documents/personal/gaapi/

"""Hello Analytics Reporting API V4."""

from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import os




# Globals
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'lifee3-209903-aae64c6e3245.json'
VIEW_ID = os.environ["le_view"]

print(VIEW_ID)



# Initialize
def initialize_analyticsreporting():
  """Initializes an Analytics Reporting API V4 service object.

  Returns:
    An authorized Analytics Reporting API V4 service object.
  """
  credentials = ServiceAccountCredentials.from_json_keyfile_name(
      KEY_FILE_LOCATION, SCOPES)

  # Build the service object.
  analytics = build('analyticsreporting', 'v4', credentials=credentials)

  return analytics


def get_report(analytics, query):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body = query
  ).execute()


def main(query):

    pageToken = query['reportRequests'][0]['pageToken']

    store_response = {}
    analytics = initialize_analyticsreporting()

    while pageToken != "" and pageToken != None:
        # debugging, was hoping to see print output on each iteration
        print(pageToken)
        response = get_report(analytics, query)
        store_response[pageToken] = response
        pageToken = response['reports'][0].get('nextPageToken') # update the pageToken
        query['reportRequests'][0]['pageToken'] = pageToken # update the query
    return(store_response)




# write querys to hit api with
## only 7 dimensions per call allowed, join on sessionId dimension1

## for batch
def generate_dims(dims):
    dims_ar = []
    for i in dims:
        d = {'name': i}
        dims_ar.append(d)
    return(dims_ar)


def generate_metrics(mets):
    mets_ar = []
    for i in mets:
        m = {'expression': i}
        mets_ar.append(m)
    return(mets_ar)


def query(pToken, dimensions, metrics, start, end, dim_filter = None):
    api_query = {
            'reportRequests': [
                    {'viewId': VIEW_ID,
                     'pageToken': pToken,          
                     'pageSize': 10000,
                     'samplingLevel': 'LARGE',
                     'dateRanges': [{'startDate': start, 'endDate': end}],
                     'dimensions': generate_dims(dimensions),
                     'metrics': generate_metrics(metrics)
                     }]
    }
    
    if(dim_filter == None):
        pass
    else:
        api_query['reportRequests'][0]['dimensionFilterClauses'] = dim_filter
                
    return(api_query)


## create sessions queries
# pageToken = "go" # can be any string to get started

## datetimes into GA api format, for rolling 24 hour period (1 day)
from datetime import datetime, timedelta
today = datetime.today()
yesterday = today - timedelta(days = 1)
start_date = yesterday.strftime('%Y-%m-%d')
end_date = start_date
print('start date (and end date) is :' + start_date)


## For Manual Updates Only
# start_date = '2018-07-27'; print('start_date is :' + start_date)
# end_date = '2018-07-27'; print('end_date is :' + end_date)


start = start_date # string e.g. '2018-07-01'
end = end_date
sessions1_qr = query(pToken = "go",
                     dimensions = ['ga:date', 'ga:dimension1',
                                   'ga:userType', 'ga:landingpagePath',
                                   'ga:deviceCategory'],
                     metrics = ['ga:sessions', 'ga:bounces', 'ga:goal1Completions', 'ga:sessionDuration'],
                     start = start,
                     end = end)


sessions2_qr = query(pToken = "go",
                     dimensions = ['ga:dimension1', 'ga:operatingSystem',
                                   'ga:source', 'ga:medium',
                                   'ga:campaign', 'ga:adContent', 'ga:keyword'],
                     metrics = ['ga:sessions'],
                     start = start,
                     end = end)


sessions3_qr = query(pToken = "go",
                     dimensions = ['ga:dimension1', 'ga:dimension2'],
                     metrics = ['ga:sessions'],
                     start = start,
                     end = end)

pageviews_qr = query(pToken = "go",
                     dimensions = ['ga:dimension1', 'ga:hostname', 'ga:pagePath',
                                   'ga:dimension7', 'ga:dimension4', 'ga:dimension3',
                                   'ga:dimension6'],
                     metrics = ['ga:pageviews', 'ga:uniquePageviews', 'ga:timeOnPage'],
                     start = start,
                     end = end)

events_qr = query(pToken = "go",
                  dimensions = ['ga:dimension1', 'ga:eventCategory', 'ga:eventAction',
                                'ga:eventLabel', 'ga:dimension5', 'ga:dimension6',
                                'ga:dimension3', 'ga:dimension4'],
                    metrics = ['ga:totalEvents', 'ga:uniqueEvents'],
                    start = start,
                    end = end,
                    dim_filter = [{"filters": [{'dimensionName': 'ga:eventCategory',
                                                'not': True,
                                                'operator': 'EXACT',
                                                'expressions': ['Article Engagement']}]
                }])

eng_events_qr = query(pToken = "go",
                  dimensions = ['ga:dimension1', 'ga:eventCategory', 'ga:eventAction',
                                'ga:eventLabel', 'ga:dimension5', 'ga:dimension6',
                                'ga:dimension3', 'ga:dimension4', 'ga:pagePath'],
                    metrics = ['ga:uniqueEvents'],
                    start = start,
                    end = end,
                    dim_filter = [{"filters": [{'dimensionName': 'ga:eventCategory',
                                                'not': False,
                                                'operator': 'EXACT',
                                                'expressions': ['Article Engagement']}]
                }])                



sessions1 = main(sessions1_qr)
sessions2 = main(sessions2_qr)
sessions3 = main(sessions3_qr)
pageviews = main(pageviews_qr)
events = main(events_qr)
eng_events = main(eng_events_qr)


# pandas and preprocessing
import pandas as pd
import numpy as np


def get_metric_names(result_set):
    # extract metric names from results
    metric_entries = result_set["go"]['reports'][0]['columnHeader']['metricHeader']['metricHeaderEntries']
    metrics = []
    for i in metric_entries:
        name = i['name']
        metrics.append(name)
    return(metrics)


## parse dimensions and metrics from API results
sessions1_dims = sessions1["go"]["reports"][0]['columnHeader']['dimensions']
sessions1_metrics = get_metric_names(sessions1)
sessions2_dims = sessions2["go"]["reports"][0]['columnHeader']['dimensions']
sessions2_metrics = get_metric_names(sessions2)
sessions3_dims = sessions3["go"]["reports"][0]['columnHeader']['dimensions']
sessions3_metrics = get_metric_names(sessions3)
pageviews_dims = pageviews["go"]["reports"][0]['columnHeader']['dimensions']
pageviews_metrics = get_metric_names(pageviews)
events_dims = events["go"]["reports"][0]['columnHeader']['dimensions']
events_metrics = get_metric_names(events)
eng_events_dims = eng_events["go"]["reports"][0]['columnHeader']['dimensions']
eng_events_metrics = get_metric_names(eng_events)


## build data frame from result set
def buildDF(result_set):
    #takes a ga result set and builds a pandas data frame
    master_dm = []
    for key, value in result_set.items():
        rows = value['reports'][0]['data']['rows']
        for r in rows:
            d = r['dimensions']
            m = r['metrics'][0]['values']
            dm = d + m
            master_dm.append(dm)
    master_dm = pd.DataFrame(master_dm)
    return(master_dm)


## create the DFs
sessions1DF = buildDF(sessions1)
sessions1DF.columns = sessions1_dims + sessions1_metrics
sessions2DF = buildDF(sessions2)
sessions2DF.columns = sessions2_dims + sessions2_metrics
sessions3DF = buildDF(sessions3)
sessions3DF.columns = sessions3_dims + sessions2_metrics
pageviewsDF = buildDF(pageviews)
pageviewsDF.columns = pageviews_dims + pageviews_metrics
eventsDF = buildDF(events)
eventsDF.columns = events_dims + events_metrics
eng_eventsDF = buildDF(eng_events)
eng_eventsDF.columns = eng_events_dims + eng_events_metrics


## get data types correct
sessions1DF['ga:sessions'] = sessions1DF['ga:sessions'].astype(int)
sessions1DF['ga:sessionDuration'] = sessions1DF['ga:sessionDuration'].astype(float)
sessions1DF['ga:goal1Completions'] = sessions1DF['ga:goal1Completions'].astype(int)
sessions1DF['ga:bounces'] = sessions1DF['ga:bounces'].astype(int)
sessions1DF['ga:date'] = pd.to_datetime(sessions1DF['ga:date'])
pageviewsDF['ga:pageviews'] = pageviewsDF['ga:pageviews'].astype(int)
pageviewsDF['ga:uniquePageviews'] = pageviewsDF['ga:uniquePageviews'].astype(int)
pageviewsDF['ga:timeOnPage'] = pageviewsDF['ga:timeOnPage'].astype(float)
eventsDF['ga:totalEvents'] = eventsDF['ga:totalEvents'].astype(int)
eventsDF['ga:uniqueEvents'] = eventsDF['ga:uniqueEvents'].astype(int)


## engagement DF, requires some minor pre processing
eng_eventsDF = eng_eventsDF[eng_eventsDF['ga:eventLabel'] != '(not set)']
eng_eventsDF['ga:uniqueEvents'] = eng_eventsDF['ga:uniqueEvents'].astype(int)
eng_eventsDF['ga:eventLabel'] = eng_eventsDF['ga:eventLabel'].astype(int)

## engagement events, group by all dimensions and then sum for engagement time
## and max for scroll %
del eng_eventsDF['ga:uniqueEvents']


## pivot
### add row num to hack pandas trying to aggregate
eng_eventsDF.insert(1, 'rownum', range(0, len(eng_eventsDF)))
eng_eventsDF = eng_eventsDF.pivot_table(index = ['ga:dimension1', 'ga:eventCategory',
                                                 'ga:eventAction', 'ga:dimension5', 
                                                 'ga:dimension6', 'ga:dimension3', 
                                                 'ga:dimension4', 'ga:pagePath', 
                                                 'rownum'],
                columns= 'ga:eventAction',
                values='ga:eventLabel',
                fill_value = 0).reset_index()


## drop rownum, eventCategory and action, redundant now
del eng_eventsDF['rownum']
del eng_eventsDF['ga:eventCategory']


## group and aggregate
## for scrolls max, for reading time sum
eng_eventsDF = eng_eventsDF.groupby(['ga:dimension1', 'ga:dimension5', 
                                     'ga:dimension6', 'ga:dimension3', 
                                     'ga:dimension4', 'ga:pagePath']).agg({'Reading Time': 'sum',
                                                                            'Scroll %': 'max'}).reset_index()
## done working on engagement events##



## get rid of duplicate columns that appear in both before joining
del sessions2DF['ga:sessions']
del sessions3DF['ga:sessions']


## create master sessionsDF
### note some sessions fail to collect client id, so left join
sessionsDF = sessions2DF.merge(sessions1DF, on = 'ga:dimension1', how ='inner')
sessionsDF = sessionsDF.merge(sessions3DF, on = 'ga:dimension1', how = "left")


## write out columns in desired order
cols = ['ga:dimension1', 'ga:dimension2', 'ga:date', 'ga:userType', 
        'ga:landingpagePath', 'ga:deviceCategory', 'ga:operatingSystem',
        'ga:source', 'ga:medium', 'ga:campaign', 'ga:adContent',
        'ga:keyword', 'ga:sessions', 'ga:bounces', 'ga:goal1Completions',
        'ga:sessionDuration']

cols_newname = ['session_id', 'client_id', 'ga_date', 'user_type', 
        'landing_page_path', 'device_category', 'operating_system',
        'source', 'medium', 'campaign', 'ad_content',
        'keyword', 'sessions', 'bounces', 'goal1_completions',
        'session_duraction']

pageviews_newname = ['session_id', 'hostname', 'page_path',
                     'article_title', 'author', 'disease_area', 'content_type',
                     'pageviews', 'unique_pageviews', 'time_on_page']

events_newname = ['session_id', 'event_category', 'event_action', 'event_label',
                  'format', 'content_type', 'disease_area', 'author',
                  'total_events', 'unique_events']

eng_events_newname = ['session_id', 'format', 'content_type', 'disease_area', 'author',
                      'page_path', 'reading_time', 'scroll_depth_percent']


## reorder
sessionsDF = sessionsDF[cols]
sessionsDF.columns = cols_newname
pageviewsDF.columns = pageviews_newname
eventsDF.columns = events_newname
eng_eventsDF.columns = eng_events_newname


# # MySQL Connection
# ## params
usr = os.environ["le_usr"]
hst = 'localhost'
pt = os.environ["le_port"]
pw = os.environ["le_pw"]
db = 'ga_web'

# import mysql.connector
# cnx = mysql.connector.connect(user = usr, password = pw,
#                                host = hst,
#                                database = db,
#                                port = pt)
# cnx.close()

# MySQL Connection
## connection
import mysql.connector
from sqlalchemy import create_engine
engine = create_engine('mysql+mysqlconnector://' + usr + ':' + pw + '@' + hst + ':' + pt + '/' + db, echo=False)


## add data to ga_web
sessionsDF.to_sql('ga_sessions', con = engine, if_exists = 'append', index = False)
pageviewsDF.to_sql('ga_pageviews', con = engine, if_exists = 'append', index = False)
eventsDF.to_sql('ga_events', con = engine, if_exists = 'append', index = False)
eng_eventsDF.to_sql('ga_engagement_events', con = engine, if_exists = 'append', index = False)


## close up
engine.dispose()