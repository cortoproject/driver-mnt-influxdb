#ifndef DRIVER_MNT_INFLUXDB_MOUNT_QUERY_RESPONSE_HANDLER_H
#define DRIVER_MNT_INFLUXDB_MOUNT_QUERY_RESPONSE_HANDLER_H

#include <include/influxdb.h>

int16_t influxdb_Mount_query_response_handler(
    influxdb_Mount this,
    corto_query *query,
    httpclient_Result *result,
    bool historical);

#endif //DRIVER_MNT_INFLUXDB_MOUNT_QUERY_RESPONSE_HANDLER_H
