#ifndef __DRIVER_MNT_INFLUXDB_QUERY_TOOL_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_TOOL_H__

#include <driver/mnt/influxdb/influxdb.h>

/* Struct holding the results of SHOW_RETENTION_POLICIES */
typedef struct _influxdb_Query_RetentionPolicyResult {
    corto_string    name;
    corto_string    duration;
    corto_string    sgDuration;     /* Shard Group Duration */
    int16_t         replication;
    bool            def;            /* Default */
} influxdb_Query_RetentionPolicyResult;

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_show_measurements(
    influxdb_Mount this,
    corto_string pattern,
    corto_ll results);

/* Releases the measurement result list content
 * NOTE: Does not free the results list - user is responsible for results list.
 */
DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_show_measurements_free(
    corto_ll results);

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_create_database(
    corto_string host,
    corto_string db);

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_show_databases(
    corto_string host,
    corto_string db,
    corto_ll results);

/* Releases the database result list content
 * NOTE: Does not free the results list - user is responsible for results list.
 */
DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_show_databases_free(
    corto_ll results);

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_show_retentionPolicies(
    corto_string host,
    corto_string db,
    corto_ll results);

/* Releases the measurement result list content
 * NOTE: Does not free the results list - user is responsible for results list.
 */
DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_show_retentionPolicies_free(
    corto_ll results);

#endif //__DRIVER_MNT_INFLUXDB_QUERY_TOOL_H__
