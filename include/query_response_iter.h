#ifndef __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_ITER_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_ITER_H__

#include <driver/mnt/influxdb/influxdb.h>

typedef struct _influxdb_Mount_iterData {
    influxdb_Query_SeriesResult *series;
    corto_result *result;
    int pos;
} influxdb_Mount_iterData;

DRIVER_MNT_INFLUXDB_EXPORT
bool influxdb_Mount_iterDataHasNext(
    corto_iter *iter);

DRIVER_MNT_INFLUXDB_EXPORT
void *influxdb_Mount_iterDataNext(
    corto_iter *iter);

DRIVER_MNT_INFLUXDB_EXPORT
void influxdb_Mount_iterDataRelease(
    corto_iter *iter);

DRIVER_MNT_INFLUXDB_EXPORT
influxdb_Mount_iterData *influxdb_Mount_iterDataNew(
    influxdb_Query_SeriesResult *series);

#endif //__DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_ITER_H__
