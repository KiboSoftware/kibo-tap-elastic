# tap-elastic

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from eleastic Search (https://www.elastic.co/) given an aggregate query
- Incrementally pulls data for each configured aggregate query based on START_DATE and END_DATE filter paramaters over a given INTERVAL window

---



