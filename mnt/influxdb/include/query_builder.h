#ifndef __DRIVER_MNT_INFLUXDB_QUERY_BUILDER_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_BUILDER_H__

#include <include/influxdb.h>

corto_string influxdb_Mount_query_builder_select(
    influxdb_Mount this,
    corto_query *query);
corto_string influxdb_Mount_query_builder_from(
    influxdb_Mount this,
    corto_query *query);
corto_string influxdb_Mount_query_builder_where(
    influxdb_Mount this,
    corto_query *query);


#endif //__DRIVER_MNT_INFLUXDB_QUERY_BUILDER_H__
