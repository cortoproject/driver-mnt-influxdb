#ifndef TEST_COMMON_H
#define TEST_COMMON_H

#include <include/lateinit.h>

extern corto_id INFLUX_MOUNT_ID;
extern corto_string INFLUX_DB_HOST;
extern corto_string INFLUX_DB_NAME;
extern influxdb_Mount influxdbMount;

int CreateHistoricalManualMount(void);
int16_t UpdateWeather(void);

#endif
