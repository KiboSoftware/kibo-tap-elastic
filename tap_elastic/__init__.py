#!/usr/bin/env python3
import os
import json
import singer
import json
import time
import requests
from datetime import datetime, timedelta
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema



REQUIRED_CONFIG_KEYS = ["host", "username", "password"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)

def replace_date(body_string, start_date, end_date,interval):    
    start_date_str = str(time.mktime(start_date.timetuple())*1000)
    end_date_str =  str(time.mktime(end_date.timetuple())*1000)
    body_string = body_string.replace('START_DATE', start_date_str)
    body_string = body_string.replace('END_DATE', end_date_str)
    body_string = body_string.replace('INTERVAL', interval)
    return body_string

def QueryStream( config,state,stream):
    lastWeek = datetime.now() - timedelta(days=2)
    lastWeek = lastWeek.replace(hour=0, minute=0, second=0, microsecond=0)    
    lastWeek = time.mktime( lastWeek.timetuple())*1000

    lastHour = datetime.utcnow()
    lastHour = lastHour.replace(minute=0, second=0, microsecond=0)
    lastHour = lastHour - timedelta(hours=1)
    lastHour = time.mktime(lastHour.timetuple()) *1000
    
    
    while True:
        singer.write_state(state)
        min =  state.get('bookmarks', {}).get(stream.tap_stream_id, lastWeek)
        if ( min >= lastHour):
            return
        end_date = datetime.fromtimestamp(min/1000 ) + timedelta(hours=1)
        start_date = datetime.fromtimestamp(min/1000 )
        state['bookmarks'][stream.tap_stream_id] = time.mktime( end_date.timetuple()) * 1000
        rows = query(config,stream,start_date,end_date)
        for row in rows:
            yield row      

def query (config,stream,start_date,end_date):
    index = stream.metadata[0]['metadata']['index']
    body =stream.metadata[0]['metadata']['body']
    interval = stream.metadata[0]['metadata'].get('interval', '10m')

    body_string = json.dumps(body)
    body_string = replace_date(body_string,start_date,end_date,interval)
    url = config['host'] + '/'+ index +'/_search?search_type=query_then_fetch&ignore_unavailable=true&max_concurrent_shard_requests=5' 
    
    LOGGER.info(f'elastic:querying: {stream.tap_stream_id} from {start_date} to {end_date} at interval: {interval}')
    log_body = body_string.replace("\n",'');
    LOGGER.info(f'elastic:queryBody: {log_body}');
    response = requests.post(
        url, 
        data=body_string, 
        headers={'Content-Type': 'application/json'},
        auth=(config['username'], config['password']) ,
        #proxies={"https": "http://localhost:8866"},
        #verify=False
        )
    # todo deal with error
    response_body = response.json()
    
    aggregations = response_body['aggregations']
    rows =[]
    fields = stream.metadata[0]['metadata']['fields']
    flatten(rows, aggregations,{}, fields)
   

    return rows

def flatten (rows, aggreate , dic , keys ): 
  
    key_copy = list(keys)
    key = key_copy.pop(0)
    data = aggreate[key]
    if ( 'buckets' in data):
        buckets = data['buckets']
        for bucket in buckets:
            dic = dic.copy()
            dic[key] = bucket['key']
            dic[key +'_count'] = bucket['doc_count']           
            if (len(key_copy) == 0):
                rows.append(dic)
            else:
                flatten(rows, bucket, dic, key_copy)
    else:
        dic = dic.copy()
        for k, v in data.items():            
            dic[key + '_' + k] = v
        rows.append(dic)







def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
  
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        if state.get("bookmarks") is  None:
            state["bookmarks"] = {}

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )
      
        tap_data = QueryStream(config,state,stream)
        for row in tap_data:            
            singer.write_records(stream.tap_stream_id, [row])           
        singer.write_state(state)
       
    return





@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
