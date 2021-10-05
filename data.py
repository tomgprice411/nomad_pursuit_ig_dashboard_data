import requests
import json
from collections import defaultdict
import pandas as pd
import sys
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
import psycopg2.extras as extras
import os
import datetime
from pytz import timezone
from decouple import config

############################################################
#### Create the ISO format time to pass to API functions ###
############################################################

daily_start_datetime = int(datetime.datetime((pd.Timestamp.today(timezone('UTC')) - pd.Timedelta(1, unit="d")).date().year, 
        (pd.Timestamp.today(timezone('UTC')) - pd.Timedelta(1, unit="d")).date().month,
        (pd.Timestamp.today(timezone('UTC')) - pd.Timedelta(1, unit="d")).date().day,
        0,0).timestamp())
daily_end_datetime = int(datetime.datetime((pd.Timestamp.today(timezone('UTC'))).date().year, 
        (pd.Timestamp.today(timezone('UTC'))).date().month,
        (pd.Timestamp.today(timezone('UTC'))).date().day,
        0,0).timestamp())

time_utc = datetime.datetime(datetime.datetime.now(tz=timezone('UTC')).year, 
                datetime.datetime.now(tz=timezone('UTC')).month,
                datetime.datetime.now(tz=timezone('UTC')).day,
                datetime.datetime.now(tz=timezone('UTC')).hour,
                datetime.datetime.now(tz=timezone('UTC')).minute, 
                datetime.datetime.now(tz=timezone('UTC')).second, tzinfo=timezone('UTC'))

midday_start_datetime = datetime.datetime(datetime.datetime.now(tz=timezone('UTC')).year, 
                datetime.datetime.now(tz=timezone('UTC')).month,
                datetime.datetime.now(tz=timezone('UTC')).day,
                11, 30, 0, tzinfo=timezone('UTC'))

midday_end_datetime = datetime.datetime(datetime.datetime.now(tz=timezone('UTC')).year, 
                datetime.datetime.now(tz=timezone('UTC')).month,
                datetime.datetime.now(tz=timezone('UTC')).day,
                12, 30, 0, tzinfo=timezone('UTC'))





#######################################
####### Export to Postgresql DB #######
#######################################

def show_psycopg2_exception(err):
    # get details about the exception
    err_type, err_obj, traceback = sys.exc_info()
    # get the line number when exception occured
    line_n = traceback.tb_lineno
    # print the connect() error
    print("\npsycopg2 ERROR:", err, "on line number:", line_n)
    print("psycopg2 traceback:", traceback, "-- type:", err_type)
    # psycopg2 extensions.Diagnostics object attribute
    print("\nextensions.Diagnostics:", err.diag)
    # print the pgcode and pgerror exceptions
    print("pgerror:", err.pgerror)
    print("pgcode:", err.pgcode, "\n")



def connect(conn_params_dic):
    conn = None
    try:
        print('Connecting to the PostgreSQL...........')
        conn = psycopg2.connect(**conn_params_dic)
        #conn = psycopg2.connect(conn_params_dic)
        print("Connection successful..................")

    except OperationalError as err:
        # passing exception to function
        show_psycopg2_exception(err)
        # set the connection to 'None' in case of error
        conn = None

    return conn


# Define function using psycopg2.extras.execute_batch() to insert the dataframe
def execute_batch(conn, datafrm, table, cols, page_size=150):

    # Creating a list of tupples from the dataframe values
    tpls = [tuple(x) for x in datafrm.to_numpy()]

    # dataframe columns with Comma-separated
    colnames = ','.join(list(datafrm.columns))

    # SQL query to execute
    sql = "INSERT INTO {}({}) VALUES({})".format(
        table, colnames, ','.join(["%s"] * cols))
    cursor = conn.cursor()
    try:
        extras.execute_batch(cursor, sql, tpls, page_size)
        conn.commit()
        print("Data inserted using execute_batch() successfully...")
    except (Exception, psycopg2.DatabaseError) as err:
        # pass exception to function
        show_psycopg2_exception(err)
        cursor.close()




##########################################
### Connect to SQL Database and Upload ###
##########################################





######################################################
######################################################
##### SECTION 1 - CREATE FUNCTIONS FOR API CALLS #####
######################################################
######################################################

################################################################
#### Create the parameters dictionary to be used in API Call ###
################################################################

def getCreds():
    creds = dict()
    creds["access_token"] = ###### INSERT LONG LIVE ACCESS TOKEN

    creds["client_id"] = ###### INSERT APP ID FROM FACEBOOK GRAPH API EXPLORER
    creds["client_secret"] = ###### INSERT CLIENT SECRET FROM FACEBOOK GRAPH API EXPLORER

    creds["graph_domain"] = "https://graph.facebook.com/"
    creds["graph_version"] = "v11.0"
    creds["endpoint_base"] = creds["graph_domain"] + creds["graph_version"] + "/"
    creds["debug"] = "no"
    creds["page_id"] = "100141092294302"
    creds["instagram_account_id"] = "17841444944639753"
    creds["ig_username"] = "nomadpursuit"

    return creds



############################
#### Create the API Call ###
############################


def makeApiCall(url, endpointParams, debug="no"):
    data = requests.get(url, endpointParams)

    response = dict()
    response["url"] = url
    response["endpoint_params"] = endpointParams
    response["endpoint_params_pretty"] = json.dumps(endpointParams, indent=4)
    response["json_data"] = json.loads(data.content)
    response["json_data_pretty"] = json.dumps(response["json_data"], indent=4)

    if ("yes" == debug):
        displayApiCallData(response)

    return response


def displayApiCallData(response):
    print("\nURL: ")
    print(response["url"])
    print("\nEndpoint Params: ")
    print(response["endpoint_params_pretty"])
    print("\nResponse: ")
    print(response["json_data_pretty"])



#########################################
#### Create Get User Info API Call ###
#########################################

def getUserInfo(params, ig_username):
    """ Get info on a users account

    API Endpoint:
        https://graph.facebook.com{graph-api-version}/{ig-user-id}?fields=business_discovery.
        username({ig-username}){username,website,name,ig_id,id,profile_picture_url,biography,follows_count,
        followers_count,media_count}&access_token={access-token}

    Returns:
        object: data from the endpoint
    """

    endpointParams = dict()
    endpointParams["fields"] = "business_discovery.username(" + ig_username + \
        "){username,follows_count,followers_count,media_count}"

    endpointParams["access_token"] = params["access_token"]
    url = params["endpoint_base"] + params["instagram_account_id"]

    return makeApiCall(url, endpointParams, params["debug"])

##################################################
#### Create Get User Insights API Call - Daily ###
##################################################


def getUserInsightsDaily(params):
    """ Get insights for users account

    API Endpoint:
        https://graph.facebook.com{graph-api-version}/{ig-user-id}/insights?metric={metric}&since={since}&until={until}

    Returns:
        object: data from the endpoint
    """

    endpointParams = dict()
    endpointParams["metric"] = "follower_count,impressions,profile_views,reach,website_clicks"
    endpointParams["period"] = "day"
    endpointParams["since"] = daily_start_datetime
    endpointParams["until"] = daily_end_datetime
    endpointParams["access_token"] = params["access_token"]

    url = params["endpoint_base"] + \
        params["instagram_account_id"] + "/insights"

    return makeApiCall(url, endpointParams, params["debug"])

#####################################################
#### Create Get User Insights API Call - Lifetime ###
#####################################################


def getUserInsightsLifetime(params):
    """ Get insights for users account

    API Endpoint:
        https://graph.facebook.com{graph-api-version}/{ig-user-id}/insights?metric={metric}

    Returns:
        object: data from the endpoint
    """

    endpointParams = dict()
    endpointParams["metric"] = "audience_city,audience_country,audience_gender_age"
    endpointParams["period"] = "lifetime"
    endpointParams["since"] = daily_start_datetime
    endpointParams["until"] = daily_end_datetime
    endpointParams["access_token"] = params["access_token"]

    url = params["endpoint_base"] + \
        params["instagram_account_id"] + "/insights"

    return makeApiCall(url, endpointParams, params["debug"])


#######################################
#### Create Get Media Info API Call ###
#######################################

def getMediaInfo(params, pagingURL=""):
    """ Get info on a users account

    API Endpoint:
        https://graph.facebook.com{graph-api-version}/{ig-user-id}/media?fields={fields}
        &access_token={access-token}

    Returns:
        object: data from the endpoint
    """

    endpointParams = dict()
    endpointParams["fields"] = "id,caption,media_type,media_product_type,media_url,permalink,thumbnail_url,timestamp,username,comments_count,like_count"

    endpointParams["access_token"] = params["access_token"]

    if pagingURL == "":
        url = params["endpoint_base"] + \
            params["instagram_account_id"] + "/media"
    else:
        url = pagingURL

    return makeApiCall(url, endpointParams, params["debug"])

###########################################
#### Create Get Media Insights API Call ###
###########################################


def getMediaInsights(params, media_id):
    """ Get insights for latest post

    API Endpoint:
        https://graph.facebook.com{graph-api-version}/{ig-media-id}/insights?metric={metric}

    Returns:
        object: data from the endpoint
    """

    endpointParams = dict()
    endpointParams["metric"] = "engagement,impressions,reach,saved"
    endpointParams["access_token"] = params["access_token"]

    url = params["endpoint_base"] + media_id + "/insights"

    return makeApiCall(url, endpointParams, params["debug"])


######################################################
######################################################
##### SECTION 2 - SEND API CALLS & UPLOAD TO SQL #####
######################################################
######################################################

######################
#### Get User Info ###
######################

# create a nested dictionary to hold data on our account and competitor's account
def nested_dict():
    return defaultdict(nested_dict)


USER_INFO_DICT = nested_dict()

OUR_ACCOUNT = ["nomadpursuit"]


###########################################################################
### Below insert the names of accounts that you want to compare against ###
###########################################################################

COMPETITORS_ACCOUNTS = ["comp 1",
                        "comp 2",
                        "comp 3",
                        "comp 4",
                        "comp 5",
                        "comp 6",
                        "comp 7",
                        "comp 8"
                        ]

ALL_ACCOUNTS = OUR_ACCOUNT + COMPETITORS_ACCOUNTS

# Send API Call
params = getCreds()

for account in ALL_ACCOUNTS:
    response = getUserInfo(params, account)
    try:
        USER_INFO_DICT[account]["followers_count"] = response["json_data"]["business_discovery"]["followers_count"]
        USER_INFO_DICT[account]["follows_count"] = response["json_data"]["business_discovery"]["follows_count"]
        USER_INFO_DICT[account]["media_count"] = response["json_data"]["business_discovery"]["media_count"]
    except:
        pass

#print(json.dumps(response["json_data"], indent=4))

# convert nested dictionaries into dataframes
df_user_info_light = pd.DataFrame.from_dict(
    USER_INFO_DICT, orient='index').reset_index()

# rename index columns
df_user_info_light.rename(columns={"index": "user_account"}, inplace=True)

# add timestamp of when the data was pulled
df_user_info_light["created_date"] = pd.Timestamp.today(timezone('UTC'))

#create a category to show the size of the follower count
df_user_info_light["followers_count_group"] = "Under 1k"
df_user_info_light.loc[df_user_info_light["followers_count"].between(1000,4999), "followers_count_group"] = "1k-5k"
df_user_info_light.loc[df_user_info_light["followers_count"].between(5000,9999), "followers_count_group"] = "5k-10k"
df_user_info_light.loc[df_user_info_light["followers_count"] >= 10000, "followers_count_group"] = "Over 10k"


################################
#### Get User Insights Daily ###
################################

if time_utc > midday_start_datetime and time_utc < midday_end_datetime:

    # create a nested dictionary to hold data for insights on our account
    USER_INSIGHTS_DICT_DAILY = nested_dict()

    # Send API Call
    params = getCreds()
    response = getUserInsightsDaily(params)


    for insight in response["json_data"]["data"]:
        INSIGHT_NAME = insight["name"]
        USER_INSIGHTS_DICT_DAILY[INSIGHT_NAME]["period"] = insight["period"]
        USER_INSIGHTS_DICT_DAILY[INSIGHT_NAME]["end_timestamp"] = insight["values"][0]["end_time"]
        USER_INSIGHTS_DICT_DAILY[INSIGHT_NAME]["value"] = insight["values"][0]["value"]

    # convert nested dictionaries into dataframes
    df_user_insights_daily = pd.DataFrame.from_dict(
        USER_INSIGHTS_DICT_DAILY, orient='index').reset_index()

    # rename index columns
    df_user_insights_daily.rename(columns={"index": "metric"}, inplace=True)

    # add timestamp of when the data was pulled
    df_user_insights_daily["created_date"] = pd.Timestamp.today(timezone('UTC'))

    #rename period column ready for upload
    df_user_insights_daily.rename(columns = {"period": "time_period"}, inplace = True)


###################################
#### Get User Insights Lifetime ###
###################################

if time_utc > midday_start_datetime and time_utc < midday_end_datetime:

    # create a nested dictionary to hold data for insights on our account
    USER_INSIGHTS_DICT_LIFETIME = nested_dict()

    # Send API Call
    params = getCreds()
    response = getUserInsightsLifetime(params)

    for insight in response["json_data"]["data"]:
        INSIGHT_NAME = insight["name"]
        USER_INSIGHTS_DICT_LIFETIME[INSIGHT_NAME]["period"] = insight["period"]
        USER_INSIGHTS_DICT_LIFETIME[INSIGHT_NAME]["end_timestamp"] = insight["values"][0]["end_time"]
        USER_INSIGHTS_DICT_LIFETIME[INSIGHT_NAME]["value"] = insight["values"][0]["value"]


    # convert nested dictionaries into dataframes
    df_user_insights_lifetime = pd.DataFrame.from_dict(
        USER_INSIGHTS_DICT_LIFETIME, orient='index').reset_index()

    # rename index columns
    df_user_insights_lifetime.rename(columns={"index": "metric"}, inplace=True)

    #extract the audience dictionaries out of the value column in the insibghts lifetime dataframe
    df_user_insights_audience_city = pd.DataFrame.from_dict(df_user_insights_lifetime.loc[df_user_insights_lifetime["metric"] == "audience_city", "value"].values[0], orient='index').reset_index()
    df_user_insights_audience_country = pd.DataFrame.from_dict(df_user_insights_lifetime.loc[df_user_insights_lifetime["metric"] == "audience_country", "value"].values[0], orient='index').reset_index()
    df_user_insights_audience_gender_age = pd.DataFrame.from_dict(df_user_insights_lifetime.loc[df_user_insights_lifetime["metric"] == "audience_gender_age", "value"].values[0], orient='index').reset_index()

    
    # rename index columns in newly created dataframes
    df_user_insights_audience_city.rename(columns={"index": "city", 0: "follower_count"}, inplace=True)
    df_user_insights_audience_country.rename(columns={"index": "country", 0: "follower_count"}, inplace=True)
    df_user_insights_audience_gender_age.rename(columns={"index": "gender_age", 0: "follower_count"}, inplace=True)

    #split gender_age column into two columns
    df_user_insights_audience_gender_age["gender"] = df_user_insights_audience_gender_age["gender_age"].str.split(".", expand = True)[0]
    df_user_insights_audience_gender_age["age"] = df_user_insights_audience_gender_age["gender_age"].str.split(".", expand = True)[1]
    df_user_insights_audience_gender_age.drop(columns = ["gender_age"], inplace = True)

    # add timestamp of when the data was pulled
    df_user_insights_audience_city["created_date"] = pd.Timestamp.today(timezone('UTC'))
    df_user_insights_audience_country["created_date"] = pd.Timestamp.today(timezone('UTC'))
    df_user_insights_audience_gender_age["created_date"] = pd.Timestamp.today(timezone('UTC'))

    #rename period column ready for upload
    df_user_insights_audience_gender_age.rename(columns = {"age": "age_range"}, inplace = True)
    df_user_insights_audience_country.rename(columns = {"country": "country_code"}, inplace = True)

    #add on other columns
    df_user_insights_audience_city["time_period"] = "lifetime"
    df_user_insights_audience_country["time_period"] = "lifetime"
    df_user_insights_audience_gender_age["time_period"] = "lifetime"

    df_user_insights_audience_city["end_timestamp"] = df_user_insights_lifetime["end_timestamp"].tolist()[0]
    df_user_insights_audience_country["end_timestamp"] = df_user_insights_lifetime["end_timestamp"].tolist()[0]
    df_user_insights_audience_gender_age["end_timestamp"] = df_user_insights_lifetime["end_timestamp"].tolist()[0]


#######################
#### Get Media Info ###
#######################

# create a nested dictionary to hold data on our media information
MEDIA_INFO_DICT = nested_dict()

# Send API Call
params = getCreds()

response_page_1 = getMediaInfo(params)
response_page_2 = getMediaInfo(
    params, response_page_1["json_data"]["paging"]["next"])

response = response_page_1["json_data"]["data"]
for elm2 in response_page_2["json_data"]["data"]:
    response.append(elm2)

for post in response:
    MEDIA_ID = post["id"]
    MEDIA_INFO_DICT[MEDIA_ID]["media_type"] = post["media_type"]
    MEDIA_INFO_DICT[MEDIA_ID]["caption"] = post["caption"]
    MEDIA_INFO_DICT[MEDIA_ID]["timestamp"] = post["timestamp"]
    MEDIA_INFO_DICT[MEDIA_ID]["comments_count"] = post["comments_count"]
    MEDIA_INFO_DICT[MEDIA_ID]["likes_count"] = post["like_count"]
    MEDIA_INFO_DICT[MEDIA_ID]["permalink"] = post["permalink"]
    MEDIA_INFO_DICT[MEDIA_ID]["media_url"] = post["media_url"]
    if MEDIA_INFO_DICT[MEDIA_ID]["media_type"] == "VIDEO":
        MEDIA_INFO_DICT[MEDIA_ID]["thumbnail_url"] = post["thumbnail_url"]
    MEDIA_INFO_DICT[MEDIA_ID]["media_product_type"] = post["media_product_type"]
    


# convert nested dictionaries into dataframes
df_media_info_light = pd.DataFrame.from_dict(
    MEDIA_INFO_DICT, orient='index').reset_index()

# rename index columns
df_media_info_light.rename(columns={"index": "media_id"}, inplace=True)

# add timestamp of when the data was pulled
df_media_info_light["created_date"] = pd.Timestamp.today(timezone('UTC'))

#convert media id to integer
df_media_info_light["media_id"] = df_media_info_light["media_id"].astype(int)


############################
#### Get Media Insights ####
############################

# create a nested dictionary to hold data on our media information
MEDIA_INSIGHTS_DICT = nested_dict()

# Send API Call
params = getCreds()

for post in MEDIA_INFO_DICT:
    response = getMediaInsights(params, post)
    MEDIA_INSIGHTS_DICT[post]["engagement"] = response["json_data"]["data"][0]["values"][0]["value"]
    MEDIA_INSIGHTS_DICT[post]["impressions"] = response["json_data"]["data"][1]["values"][0]["value"]
    MEDIA_INSIGHTS_DICT[post]["reach"] = response["json_data"]["data"][2]["values"][0]["value"]
    MEDIA_INSIGHTS_DICT[post]["saved"] = response["json_data"]["data"][3]["values"][0]["value"]
    MEDIA_INSIGHTS_DICT[post]["period"] = response["json_data"]["data"][0]["period"]


# convert nested dictionaries into dataframes
df_media_insight_light = pd.DataFrame.from_dict(
    MEDIA_INSIGHTS_DICT, orient='index').reset_index()

# rename index columns
df_media_insight_light.rename(columns={"index": "media_id"}, inplace=True)
df_media_insight_light.rename(columns = {"period": "time_period"}, inplace = True)

# add timestamp of when the data was pulled
df_media_insight_light["created_date"] = pd.Timestamp.today(timezone('UTC'))

#convert media id to integer
df_media_insight_light["media_id"] = df_media_insight_light["media_id"].astype(int)


#create a .env file that contains the following info
#DATABASE_URL=[INSERT URI FROM HEROKU]
conn = config("DATABASE_URL")
if conn.startswith("postgres://"):
    conn = conn.replace("postgres://", "postgresql://", 1)

conn = psycopg2.connect(conn)
conn.autocommit = True 


#run the execute batch method 
execute_batch(conn, df_media_insight_light[["media_id", "engagement", "impressions", "reach", "saved", "time_period", "created_date"]], "light_media_insights", len(df_media_insight_light.columns))
execute_batch(conn, df_media_info_light[["media_id", "media_type", "caption", "timestamp", "comments_count", "likes_count", "permalink", "created_date", "media_url", "thumbnail_url", "media_product_type"]], "light_media_info", len(df_media_info_light.columns))
execute_batch(conn, df_user_info_light[["user_account", "followers_count", "follows_count", "media_count", "created_date", "followers_count_group"]], "light_user_info", len(df_user_info_light.columns))

#if current time is 12:00:00 (UTC) then upload the below (as these only get uploaded once daily)
if time_utc > midday_start_datetime and time_utc < midday_end_datetime:
    execute_batch(conn, df_user_insights_audience_city[["city", "follower_count", "created_date", "time_period", "end_timestamp"]], "light_user_insights_audience_city", len(df_user_insights_audience_city.columns))
    execute_batch(conn, df_user_insights_audience_country[["country_code", "follower_count", "created_date", "time_period", "end_timestamp"]], "light_user_insights_audience_country", len(df_user_insights_audience_country.columns))
    execute_batch(conn, df_user_insights_audience_gender_age[["gender", "age_range", "follower_count", "created_date", "time_period", "end_timestamp"]], "light_user_insights_audience_gender_age", len(df_user_insights_audience_gender_age.columns))
    execute_batch(conn, df_user_insights_daily[["metric", "time_period", "end_timestamp", "value", "created_date"]], "light_user_insights_daily", len(df_user_insights_daily.columns))


conn.close()


