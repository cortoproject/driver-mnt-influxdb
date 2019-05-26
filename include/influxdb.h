/* influxdb.h
 * This is the main package file. Include this file in other projects.
 * Only modify inside the header-end and body-end sections.
 */

#ifndef DRIVER_MNT_INFLUXDB_H
#define DRIVER_MNT_INFLUXDB_H

#include <corto/corto.h>
#include <driver/mnt/influxdb/_project.h>
#include <corto/httpclient/c/c.h>
#include <corto/c/c.h>
#include <corto/httpclient/httpclient.h>
#include <driver/fmt/influxdb/influxdb.h>
#include <parson/parson.h>

/* $header() */
/* Definitions that are required by package headers (native types) go here. */
/* $end */

#include <driver/mnt/influxdb/_type.h>
#include <driver/mnt/influxdb/_interface.h>
#include <driver/mnt/influxdb/_load.h>
#include <driver/mnt/influxdb/_binding.h>
#include <driver/mnt/influxdb/c/_api.h>

/* $body() */
#ifdef __cplusplus
extern "C" {
#endif

bool influxdb_serialize_scalar(
    corto_buffer *buffer,
    corto_string member,
    corto_object o);
void influxdb_safeString(
    corto_buffer *b,
    char* source);

#ifdef __cplusplus
}
#endif
/* $end */

#endif
