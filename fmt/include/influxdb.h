/* influxdb.h
 *
 * This is the main package file. Include this file in other projects.
 * Only modify inside the header-end and body-end sections.
 */

#ifndef DRIVER_FMT_INFLUXDB_H
#define DRIVER_FMT_INFLUXDB_H

#include <corto/corto.h>
#include <driver/fmt/influxdb/_project.h>

/* $header() */
#ifdef __cplusplus
extern "C" {
#endif

/* Implements the line protocol used by InfluxDb */
corto_string influxdb_fromCorto(corto_object o);

/* Stub: line protocol is only used to store data in InfluxDb so this function
 * will never be used. */
corto_int16 influxdb_toCorto(corto_object o, corto_string data);

void influxdb_release(corto_string data);

corto_string influxdb_copy(corto_string data);

#ifdef __cplusplus
}
#endif
/* $end */

/* $body() */
/* Definitions here that need your package headers go here. */
/* $end */

#endif

