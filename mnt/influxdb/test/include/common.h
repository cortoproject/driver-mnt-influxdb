#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <include/test.h>
#include <driver/mnt/influxdb/query_tool.h>

extern corto_id INFLUX_MOUNT_ID;
extern corto_string INFLUX_DB_HOST;
extern corto_string INFLUX_DB_NAME;
extern influxdb_Mount influxdbMount;

int CreateManualMount(corto_object mountPoint);
int16_t test_write_weather(corto_object weather);

#endif
