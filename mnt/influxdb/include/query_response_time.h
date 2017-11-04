#ifndef __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_TIME_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_TIME_H__

#include <driver/mnt/influxdb/influxdb.h>

#define JSON_PTR_VERIFY(ptr, msg) if (!ptr) { corto_seterr(msg); goto error; }
#define JSON_SAFE_FREE(v)if (v) { json_value_free(v); v = NULL; }

int16_t influxdb_Mount_response_time(JSON_Object *output, JSON_Value *value);

#endif //__DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_TIME_H__
