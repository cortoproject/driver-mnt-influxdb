/* _project.h
 *
 * This file contains generated code. Do not modify!
 */

#if BUILDING_DRIVER_FMT_INFLUXDB && defined _MSC_VER
#define DRIVER_FMT_INFLUXDB_EXPORT __declspec(dllexport)
#elif BUILDING_DRIVER_FMT_INFLUXDB
#define DRIVER_FMT_INFLUXDB_EXPORT __attribute__((__visibility__("default")))
#elif defined _MSC_VER
#define DRIVER_FMT_INFLUXDB_EXPORT __declspec(dllimport)
#else
#define DRIVER_FMT_INFLUXDB_EXPORT
#endif

