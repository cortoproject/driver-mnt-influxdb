#ifndef __DRIVER_MNT_INFLUXDB_QUERY_BUILDER_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_BUILDER_H__

#include <include/influxdb.h>

DRIVER_MNT_INFLUXDB_EXPORT
corto_string influxdb_Mount_query_builder_select(
    influxdb_Mount this,
    corto_query *query);

DRIVER_MNT_INFLUXDB_EXPORT
corto_string influxdb_Mount_query_builder_from(
    influxdb_Mount this,
    corto_query *query);

DRIVER_MNT_INFLUXDB_EXPORT
corto_string influxdb_Mount_query_builder_where(
    influxdb_Mount this,
    corto_query *query);

DRIVER_MNT_INFLUXDB_EXPORT
corto_string influxdb_Mount_query_builder_regex(
    corto_string pattern);


#endif //__DRIVER_MNT_INFLUXDB_QUERY_BUILDER_H__
