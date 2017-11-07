# InfluxDB Connector

## Getting Started

## Configuration
### InfluxDB Mount
**Property** | **Required** | **Type** | **Description** | **Example**
host  |  true  | string | InfluxDB endpoint. | http://192.168.1.100:8086
db    |  true  | string | Database Name | weather
rp    |  false | RetentionPolicy | Retention Policy. |
```json
{
    "name": "10m.events",
    "db": "weather",
    "duration": "10m",
    "replication": 1,
    "shardDuration": "30m"
}
```
host  |  true  | string | InfluxDB endpoint. | http://192.168.1.100:8086

### Retention Policy
**Property** | **Required** | **Type** | **Description** | **Example**

## Known Limitations
### Timestamps

InfluxDB is a timeseries database, thus it should be expected that data will
be stored with timestamps. Timestamps can be manually provided by the app
(when did event X occur) or automatically defined by InfluxDB. This behavior
is defined by the model of the object being saved.

Be conscious of the following when connecting the InfluxDB mount.
* Objects with a `timestamp` member of type (corto/time) will manually specify
  the update timestamp in the line protocol POST Request.
* Objects with a `timestamp` member and a `time` member will conflict. InfluxDB
  responses provide the data sample's _timestamp_ in the `time` column. `time`
  will not update due to the logic built to support _timestamps_.
