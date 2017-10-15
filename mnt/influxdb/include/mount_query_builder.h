#ifndef DRIVER_MNT_INFLUXDB_MOUNT_QUERY_BUILDER_H
#define DRIVER_MNT_INFLUXDB_MOUNT_QUERY_BUILDER_H

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


#endif //DRIVER_MNT_INFLUXDB_MOUNT_QUERY_BUILDER_H
