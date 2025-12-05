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
    # Fix for Elasticsearch 7.2+: replace "interval" with "fixed_interval" in date_histogram
    body_string = body_string.replace('"interval":', '"fixed_interval":')
    return body_string

def QueryStream( config,state,stream):
    lastWeek = datetime.utcnow() - timedelta(days=2)
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
    filter_path = stream.metadata[0]['metadata'].get('filter_path', '')
    body_string = json.dumps(body)
    body_string = replace_date(body_string,start_date,end_date,interval)
    url = config['host'] + '/'+ index +'/_search?search_type=query_then_fetch&ignore_unavailable=true&max_concurrent_shard_requests=5&filter_path='+ filter_path 
    
    LOGGER.info(f'elastic:querying: {stream.tap_stream_id} from {start_date} to {end_date} at interval: {interval}')
    log_body = body_string.replace("\n",'');
    LOGGER.info(f'elastic:queryBody: {log_body}');

    # Build proxies configuration from environment variables
    proxies = {}
    http_proxy = os.environ.get('HTTP_PROXY') or os.environ.get('http_proxy')
    https_proxy = os.environ.get('HTTPS_PROXY') or os.environ.get('https_proxy')
    if http_proxy:
        proxies['http'] = http_proxy
    if https_proxy:
        proxies['https'] = https_proxy

    response = requests.post(
        url,
        data=body_string,
        headers={'Content-Type': 'application/json'},
        auth=(config['username'], config['password']),
        proxies=proxies if proxies else None
        )
    # Check for HTTP errors
    if response.status_code != 200:
        LOGGER.error(f'elastic:error: HTTP {response.status_code} - {response.text}')
        raise Exception(f'Elasticsearch query failed with status {response.status_code}: {response.text}')

    response_body = response.json()

    # Log the full response for debugging
    LOGGER.info(f'elastic:response: {json.dumps(response_body)}')

    # Check if aggregations exist in response
    if 'aggregations' not in response_body:
        LOGGER.error(f'elastic:error: No aggregations in response. Response keys: {list(response_body.keys())}')
        if 'error' in response_body:
            LOGGER.error(f'elastic:error: {json.dumps(response_body["error"])}')
            raise Exception(f'Elasticsearch error: {response_body["error"]}')
        # raise Exception(f'No aggregations found in Elasticsearch response. Response: {json.dumps(response_body)}')

    aggregations = response_body.get('aggregations', {})
    rows =[]
    if ( not aggregations):
        return rows 
    fields = stream.metadata[0]['metadata']['fields']
    flatten(rows, aggregations,{}, fields)
    clean_rows = clean (rows);

    return clean_rows

def clean_value(value):
    """Recursively clean keys in nested objects."""
    if isinstance(value, dict):
        clean_dict = {}
        for k, v in value.items():
            clean_key = sanitize_key_for_bigquery(k)
            clean_dict[clean_key] = clean_value(v)
        return clean_dict
    elif isinstance(value, list):
        return [clean_value(item) for item in value]
    else:
        return value

def clean (rows):
    clean_rows = []
    for row in rows:
        clean_row = {}
        for key, value in row.items():
            clean_key = sanitize_key_for_bigquery(key)
            clean_row[clean_key] = clean_value(value)
        clean_rows.append(clean_row)
    return clean_rows
def sanitize_key_for_bigquery(key):
    """
    Sanitize keys to be valid BigQuery table/column names.

    BigQuery naming rules:
    - Must start with a letter or underscore
    - Can only contain letters, numbers, and underscores
    - Cannot be a reserved keyword
    - Max length 300 characters

    Returns sanitized key name.
    """
    import re

    # Convert to string if not already
    key_str = str(key)

    # Trim trailing non-significant decimal zeros from numbers (e.g., "25.0" -> "25")
    # Match numbers that have a decimal point followed by only zeros
    key_str = re.sub(r'(\d+)\.0+$', r'\1', key_str)
    # Also handle cases like "25.50" -> "25.5" (trim trailing zeros after decimal)
    key_str = re.sub(r'(\d+\.\d*?)0+$', r'\1', key_str)
    # Clean up if we end with just a decimal point
    key_str = re.sub(r'\.$', '', key_str)

    # Check if key starts with a number or is purely numeric
    if key_str and (key_str[0].isdigit() or key_str.replace('.', '').replace('-', '').replace('+', '').isdigit()):
        key_str = f'n_{key_str}'

    # Replace invalid characters with underscores
    # Valid chars are: letters, numbers, underscores
    key_str = re.sub(r'[^a-zA-Z0-9_]', '_', key_str)

    # If key starts with underscore, that's valid in BigQuery, but multiple underscores should be collapsed
    key_str = re.sub(r'_+', '_', key_str)

    # Ensure it starts with letter or underscore (shouldn't happen after n_ prefix, but safety check)
    if key_str and not (key_str[0].isalpha() or key_str[0] == '_'):
        key_str = f'col_{key_str}'

    # Handle empty string edge case
    if not key_str:
        key_str = 'empty_key'

    # Truncate to 300 characters (BigQuery limit)
    if len(key_str) > 300:
        key_str = key_str[:300]

    return key_str

def flatten (rows, aggreate , dic , keys ): 
  
    key_copy = list(keys)
    key = key_copy.pop(0)
    if isinstance(key, list):
        for k in key:
            dic[k] = aggreate[k]['value']
        rows.append(dic)
        return
    data = aggreate.get(key, None)
    if data is None:
        return
    if data and 'buckets' in data:
        buckets = data['buckets']
        for bucket in buckets:
            dic = dic.copy()
            dic[key] = bucket['key']
            dic[key +'_count'] = bucket.get('doc_count', 0)           
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
