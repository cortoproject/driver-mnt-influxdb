#ifndef __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_ITER_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_ITER_H__

#include <driver/mnt/influxdb/influxdb.h>

typedef struct _influxdb_mount_iterData {
    influxdb_Query_SeriesResult *series;
    corto_record *result;
    int pos;
} influxdb_mount_iterData;

DRIVER_MNT_INFLUXDB_EXPORT
bool influxdb_mount_iterDataHasNext(
    corto_iter *iter);

DRIVER_MNT_INFLUXDB_EXPORT
void *influxdb_mount_iterDataNext(
    corto_iter *iter);

DRIVER_MNT_INFLUXDB_EXPORT
void influxdb_mount_iterDataRelease(
    corto_iter *iter);

DRIVER_MNT_INFLUXDB_EXPORT
influxdb_mount_iterData *influxdb_mount_iterDataNew(
    influxdb_Query_SeriesResult *series);

#endif //__DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_ITER_H__
