/* influx.h
 *
 * This file is generated. Do not modify.
 */

#ifndef CORTO_FMT_INFLUX_H
#define CORTO_FMT_INFLUX_H

#include <corto/corto.h>

/* $header() */
#ifdef __cplusplus
extern "C" {
#endif

/* Implements the line protocol used by InfluxDb */
corto_string influx_fromCorto(corto_object o);

/* Stub: line protocol is only used to store data in InfluxDb so this function
 * will never be used. */
corto_int16 influx_toCorto(corto_object o, corto_string data);

void influx_release(corto_string data);

#ifdef __cplusplus
}
#endif
/* $end */

#ifdef __cplusplus
extern "C" {
#endif

#ifdef __cplusplus
}
#endif
#endif

