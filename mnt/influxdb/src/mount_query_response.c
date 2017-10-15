#include <include/mount_query_response.h>

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

int16_t influxdb_Mount_query_response_handler(
    influxdb_Mount this,
    corto_query *query,
    httpclient_Result *result)
{
    corto_info("GET Result STATUS [%d] RESPONSE [%s]",
        result->status, result->response);

    if (result->status != 200) {
        corto_seterr("Query failed. Status [%d] Response [%s]",
            result->status, result->response);
        goto error;
    }

    
    return 0;
error:
    return -1;
}
