#ifndef __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_H__

#include <driver/mnt/influxdb/influxdb.h>

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_query_response_handler(
    influxdb_Mount this,
    httpclient_Result *r);

#endif //__DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_H__
