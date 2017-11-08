/* influxdb.h
 *
 * This is the main package file. Include this file in other projects.
 * Only modify inside the header-end and body-end sections.
 */

#ifndef DRIVER_MNT_INFLUXDB_H
#define DRIVER_MNT_INFLUXDB_H

#include <corto/corto.h>
#include <corto/corto.h>
#include <driver/mnt/influxdb/_project.h>
#include <corto/c/c.h>
#include <corto/httpclient/httpclient.h>
#include <driver/fmt/json/json.h>

/* $header() */
/* Definitions that are required by package headers (native types) go here. */
/* $end */

#include <driver/mnt/influxdb/_type.h>
#include <driver/mnt/influxdb/_interface.h>
#include <driver/mnt/influxdb/_load.h>
#include <driver/mnt/influxdb/c/_api.h>

/* $body() */
#include <driver/mnt/influxdb/query_tool.h>
/* $end */

#endif

