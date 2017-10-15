/* influxdb.h
 *
 * This is the main package file. Include this file in other projects.
 * Only modify inside the header-end and body-end sections.
 */

#ifndef DRIVER_MNT_INFLUXDB_MOUNT_QUERY_H
#define DRIVER_MNT_INFLUXDB_MOUNT_QUERY_H

#include <include/influxdb.h>

corto_string influxdb_Mount_query_select(
    influxdb_Mount this,
    corto_query *query);
corto_string influxdb_Mount_query_from(
    influxdb_Mount this,
    corto_query *query);
corto_string influxdb_Mount_query_where(
    influxdb_Mount this,
    corto_query *query);


#endif //DRIVER_MNT_INFLUXDB_MOUNT_QUERY_H
