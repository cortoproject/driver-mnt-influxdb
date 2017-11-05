#ifndef __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_H__
#define __DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_H__

#include <driver/mnt/influxdb/influxdb.h>

DRIVER_MNT_INFLUXDB_EXPORT
int16_t influxdb_Mount_query_response_handler(
    influxdb_Mount this,
    httpclient_Result *r);

    ///TODO REMOVE NOTES
    // struct corto_result {
    //     corto_string id;
    //     corto_string name;
    //     corto_string parent;
    //     corto_string type;
    //     uintptr_t value;
    //     corto_resultMask flags;
    //     corto_object object;
    //     corto_sampleIter history;
    //     corto_object owner;
    // };

#endif //__DRIVER_MNT_INFLUXDB_QUERY_RESPONSE_H__
