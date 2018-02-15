#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <include/test.h>
#include <driver/mnt/influxdb/query_tool.h>

extern corto_id INFLUX_MOUNT_ID;
extern corto_string INFLUX_DB_HOST;
extern corto_string INFLUX_DB_NAME;
extern corto_int16 INFLUX_DB_PORT;
extern influxdb_Mount influxdbMount;

int create_manual_mount(corto_object mountPoint);
int create_historical_manual_mount(corto_object mountPoint);
int16_t create_weather_objects(corto_object weather);

#endif
