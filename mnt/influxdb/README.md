# InfluxDB Connector

## Getting Started

## Configuration
### InfluxDB Mount
|**Property** | **Required** | **Type** | **Description** | **Example**|
|-------------|--------------|----------|-----------------|------------|
|host  |  true  | string | InfluxDB endpoint. | http://192.168.1.100:8086|
|db    |  true  | string | Database Name | weather|
|rp    |  false | RetentionPolicy | Retention Policy. | JSON Object |
| username  | false  | string | InfluxDB Username | admin |
| password  | false  | string | InfluxDB Password | password |

**Retention Policy JSON Object**
```javascript
{
    "name": "10m.events",
    "db": "weather",
    "duration": "10m",
    "replication": 1,
    "shardDuration": "30m"
}
```

**Note:** InfluxDB mounts can be created without specifying a Retention Policy. When Retention 
policy is not defined, `autogen` will be used as the default policy. 

### Retention Policy
[Retention Policies](https://docs.influxdata.com/influxdb/v1.3/query_language/database_management/#retention-policy-management) Specify how long InfluxDB samples will be persisted to disk. 

|**Property** | **Required** | **Type** | **Description** | **Example**|
|-------------|--------------|----------|-----------------|------------|
| name     |  true  | string | Name of retention policy | 10m.events (10 minute events) |
| database |  true  | string | Database Name for retetnion policy | weather |
| duration |  true | string | Length of time to store series data.  | 180m (3 hours) |
| replication | true  | integer |  Number of independent copies of each point are stored in the cluster  | 1 |
| shardDuration  | false  | string | Time range covered by a shard group. | 1h |

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

### Void Objects NOT SUPPORTED

InfluxDB will fail to serialize objects of void type. Void objects do not have datafields, 
thus are not supported by InfluxDB. 

**Consequences**
* Be aware that mounting InfluxDB to a scope with void objects will create holes in your tree path.
  It may be impossible to query for children of void objects. 
