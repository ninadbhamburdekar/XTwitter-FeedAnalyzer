import requests
import os
import json
import datetime
from elasticsearch import Elasticsearch
import ssl
import yaml
import env
from argparse import ArgumentParser


# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = env.xapi_bearer_token
ESNODES = env.ESNODES
esclient = env.setup_esclient(ESNODES)
search_url = "https://api.twitter.com/2/tweets/search/recent"

### Read Polling Config
with open("xtweet_config.yaml") as stream:
    try:
        yamlconfig = yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        print(exc)

responses = 0
USERINDEX = yamlconfig['elasticconfig']['userindex']
DATAINDEX = yamlconfig['elasticconfig']['dataindex']
OPSINDEX = yamlconfig['elasticconfig']['opsindex']

def create_headers(bearer_token):
    headers = {
        "Authorization": "Bearer {}".format(bearer_token),
        "User-Agent": "v2SearchTweetPython"
    }
    return headers


def connect_to_endpoint(url, headers, params):
    response = requests.request("GET", search_url, headers=headers, params=params)
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(response.status_code, response.text)
    return response.json()


def main():

    ## Parse input arguments
    parser = ArgumentParser()
    parser.add_argument("-u", "--user", dest="userid", default="ALL",
                        help="Get specific user info")
    args = parser.parse_args()

    userinfo = get_user(args.userid, esclient)




def get_user(user, esclient):

    headers = create_headers(bearer_token)
    json_response = connect_to_endpoint("https://api.twitter.com/2/users/:"+user, headers, query_params)
    ###




if __name__ == "__main__":
    main()