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
    parser.add_argument("-p", "--poll", dest="pollcfg", default="ALL",
                        help="Run a specific poll config")
    parser.add_argument("-l", "--lookback", dest="lookback", default="150",
                        help="Override lookback time (default is 150)")
    args = parser.parse_args()

    if args.pollcfg != "ALL":
        print("Running poll config: {}".format(args.pollcfg))
        try:
            pull_tweets(yamlconfig['pollconfig'][args.pollcfg], esclient)
        except Exception as e:
            print("Exception reading poll config: %s" %(e))

    else:
        for config in yamlconfig['pollconfig']:
            print("Processing config: {}".format(config))
            pull_tweets(yamlconfig['pollconfig'][config], esclient)


def pull_tweets(config, esclient):

    json_response = ""
    newest_item = ""
    responses = 0

    query_params = {
        'query': "POPULATE_SEARCH_TERM",
        'max_results': 100,
        'sort_order': 'recency',
        'tweet.fields': 'created_at,author_id,geo,id,public_metrics,text,note_tweet,referenced_tweets,context_annotations,entities',
        'place.fields': 'contained_within,country,country_code,full_name,geo,id,name,place_type',
        'user.fields': 'created_at,description,entities,id,location,most_recent_tweet_id,name,pinned_tweet_id,'
                       'profile_image_url,protected,public_metrics,url,username,verified,verified_type',
        'expansions': 'geo.place_id,author_id,referenced_tweets.id',
        'start_time': '',
    }

    try:
        query_params_doc = esclient.get(index=OPSINDEX, id="latest_query_" + config['topic'])
        query_params_prev = query_params_doc['_source']['query_params']
        if 'next_token' in query_params_prev or 'since_id' in query_params_prev:
            ## If paginating
            if 'next_token' in query_params_prev and query_params_prev['next_token'] != "NULL":
                query_params['next_token'] = query_params_prev['next_token']

            ## If continuining from last poll run
            else:
                if 'since_id' in query_params_prev:
                    query_params['since_id'] = query_params_prev['since_id']
                    query_params.pop('start_time')
        else:
            query_params['start_time'] = (
                    datetime.datetime.now(datetime.UTC) - datetime.timedelta(
                hours=config['lookback_h'])).isoformat().replace('+00:00',
                                                                 'Z')


        query_params['query'] = config['search']

        query_doc = {'query_params': query_params,
                     'run_time': datetime.datetime.now(datetime.UTC),
                     'topic': config['topic'],
                     'stage': "CONTINUING"
                     }

        resp = esclient.index(index=OPSINDEX, id="latest_query_" + config['topic'], document=query_doc)

    except Exception as e:

        search_term = config['search']  # Replace this value with your search term

        query_params['start_time'] = (
                datetime.datetime.now(datetime.UTC) - datetime.timedelta(hours=config['lookback_h'])).isoformat().replace('+00:00',
                                                                                                                          'Z')
        query_params['query'] = search_term

        query_doc = {'query_params': query_params,
                     'run_time': datetime.datetime.now(datetime.UTC),
                     'topic': config['topic'],
                     'stage': "STARTED"
                     }

        resp = esclient.index(index=OPSINDEX, id="latest_query_" + config['topic'], document=query_doc)


    try:
        ### Initial run, or query to paginate
        while (json_response == "") or (len(json_response['meta']['next_token']) > 0):

            headers = create_headers(bearer_token)
            json_response = connect_to_endpoint(search_url, headers, query_params)

            if len(newest_item) == 0:
                newest_item = json_response['meta']['newest_id']

            tweet_enrichments = {}
            collection_time = datetime.datetime.now()
            for tweet in json_response['includes']['tweets']:

                tweet_enrichments[tweet['id']] = tweet
            for user in json_response['includes']['users']:

                try:
                    resp = esclient.index(index=USERINDEX, id="user_"+user['id'], document=user)
                except Exception as e:
                    print(e)
                tweet_enrichments[user['id']] = user

            for resp in json_response['data']:
                print("Processing Tweet: %s" %(resp))
                try:
                    resp = tweet_enrichments[resp['id']]
                except Exception as e:
                    print("Enrichment for %s not available" %(resp['id']))

                try:
                    resp['author_info'] = tweet_enrichments[resp['author_id']]
                except Exception as e:
                    print("Author info for Tweet %s not available" %(resp['id']))

                xcollect_tweet = {
                    'source': 'X_tweets',
                    'tool': 'xcollector_v3',
                    'collection_meta': {'topic': config['topic'],
                                        'case': 'generic',
                                        'mode': 'ondemand'
                                        },
                    'collection_time': collection_time,
                    'timestamp': datetime.datetime.strptime(resp['created_at'][:-5], "%Y-%m-%dT%H:%M:%S"),
                    'tweet_data': resp
                }

                responses += 1
                resp = esclient.index(index=DATAINDEX, id="xcollect_doc_"+resp['id'], document=xcollect_tweet)

            try:
                query_params['next_token'] = json_response['meta']['next_token']
                print("Next token: %s" %(query_params['next_token']))

                query_doc = {'query_params': query_params,
                             'run_time': datetime.datetime.now(datetime.UTC),
                             'topic': config['topic'],
                             'stage': "PAGINATING"
                             }
                resp = esclient.index(index=OPSINDEX, id="latest_query_" + config['topic'], document=query_doc)
            except Exception as e:
                print("Next token not found, reached end of page")
                query_params['since_id'] = newest_item
                query_params.pop('start_time',None)
                query_params['next_token'] = "NULL"

                query_doc = {'query_params': query_params,
                             'run_time': datetime.datetime.now(datetime.UTC),
                             'topic': config['topic'],
                             'stage': "COMPLETED"
                             }
                resp = esclient.index(index=OPSINDEX, id="latest_query_" + config['topic'], document=query_doc)

            if responses > config['collectcap']:
                break


    except Exception as e:

        query_doc = {'query_params': query_params,
                     'run_time': datetime.datetime.now(datetime.UTC),
                     'topic': config['topic'],
                     'stage': "COMPLETED_E",
                     'message': ("Exception: %s"%str(e))
                     }
        resp = esclient.index(index=OPSINDEX, id="latest_query_" + config['topic'] + "_error", document=query_doc)
        resp = esclient.index(index=OPSINDEX, id="latest_query_" + config['topic'], document=query_doc)
        if 'since_id' in str(e) and 'that is larger than' in str(e):
            print("OPSINDEX contains last poll date older than seven days. Deleting old ops entry")
            resp = esclient.delete(index=OPSINDEX, id="latest_query_" + config['topic'])

        print(e)
    try:
        if responses is not None:
            print("Total Tweets collected: %s for topic %s" % (responses, config['topic']))
            responses = 0
        else:
            print("No results since ID %s , %s" % (query_params['since_id'], query_params))
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()