#ifndef __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_PARSER_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_PARSER_H__

#include <driver/mnt/influxdb/influxdb.h>

#define JSON_PTR_VERIFY(ptr, msg) if (!ptr) { corto_seterr(msg); goto error; }
#define JSON_SAFE_FREE(v)if (v) { json_value_free(v); v = NULL; }

struct influxdb_Query_Result {
    const char* name;
    JSON_Array *values;
    JSON_Array *columns;
    size_t valueCount;
};

DRIVER_MNT_INFLUXDB_EXPORT
corto_string influxdb_Mount_response_column_name(JSON_Array *cols, int pos);

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_response_parse(
    JSON_Value *responseValue,
    struct influxdb_Query_Result *result);

#endif //__DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_PARSER_H__
