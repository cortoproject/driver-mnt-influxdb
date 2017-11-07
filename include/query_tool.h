#ifndef __DRIVER_MNT_INFLUXDB_QUERY_TOOL_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_TOOL_H__

#include <driver/mnt/influxdb/influxdb.h>

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_show_measurements(
    influxdb_Mount this,
    corto_string pattern,
    corto_ll results);

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_show_retentionPolicies(
    influxdb_Mount this,
    corto_ll results);

#endif //__DRIVER_MNT_INFLUXDB_QUERY_TOOL_H__
