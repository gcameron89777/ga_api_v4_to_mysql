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




# Globals
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = 'lifee3-209903-aae64c6e3245.json'
VIEW_ID = VIEW_ID



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


def query(pToken, dimensions, metrics, start, end):
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
    return(api_query)


## create sessions queries
#pageToken = "go" # can be any string to get started
start = start_date # string e.g. '2018-07-01'
end = end_date
sessions1_qr = query(pToken = "go",
                     dimensions = ['ga:date', 'ga:dimension1',
                                   'ga:userType', 'ga:landingpagePath',
                                   'ga:deviceCategory'],
                     metrics = ['ga:sessions', 'ga:bounces', 'ga:goal1Completions'],
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


sessions1 = main(sessions1_qr)
sessions2 = main(sessions2_qr)
sessions3 = main(sessions3_qr)


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


sessions1_dims = sessions1["go"]["reports"][0]['columnHeader']['dimensions']
sessions1_metrics = get_metric_names(sessions1)
sessions2_dims = sessions2["go"]["reports"][0]['columnHeader']['dimensions']
sessions2_metrics = get_metric_names(sessions2)
sessions3_dims = sessions3["go"]["reports"][0]['columnHeader']['dimensions']
sessions3_metrics = get_metric_names(sessions3)


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


## create both DFs
sessions1DF = buildDF(sessions1)
sessions1DF.columns = sessions1_dims + sessions1_metrics
sessions2DF = buildDF(sessions2)
sessions2DF.columns = sessions2_dims + sessions2_metrics
sessions3DF = buildDF(sessions3)
sessions3DF.columns = sessions3_dims + sessions2_metrics


## get data types correct
sessions1DF['ga:sessions'] = sessions1DF['ga:sessions'].astype(int)
sessions1DF['ga:goal1Completions'] = sessions1DF['ga:goal1Completions'].astype(int)
sessions1DF['ga:bounces'] = sessions1DF['ga:bounces'].astype(int)
sessions1DF['ga:date'] = pd.to_datetime(sessions1DF['ga:date'])


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
        'ga:keyword', 'ga:sessions', 'ga:bounces', 'ga:goal1Completions']


## reorder
sessionsDF = sessionsDF[cols]


# MySQL Connection
## params
usr = USER
pw = PW
hst = HOST
pt = PORT

import mysql.connector
cnx = mysql.connector.connect(user = usr, password = pw,
                              host = hst,
                              database = db,
                              port = pt)
cnx.close()
