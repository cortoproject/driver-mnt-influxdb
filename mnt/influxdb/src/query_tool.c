#include <driver/mnt/influxdb/query_response_parser.h>
#include <driver/mnt/influxdb/query_tool.h>

int16_t influxdb_Mount_parse_measurements(
    struct influxdb_Query_Result *result,
    corto_ll results)
{
    size_t i;
    for (i = 0; i < result->valueCount; i++) {
        JSON_Array *values = json_array_get_array(result->values, i);
        JSON_PTR_VERIFY(values, "Failed to parse response values JSON array.")

        size_t j;
        size_t cnt = json_array_get_count(values);
        for (j = 0; j < cnt; j++) {
            JSON_Value *v = json_array_get_value(values, j);
            JSON_PTR_VERIFY(v, "Failed to get response JSON value.")
            const char* measurement = json_value_get_string(v);
            corto_ll_append(results, (void*)corto_strdup(measurement));
            corto_info("Value: %s", measurement);
        }
    }

    return 0;

error:
    return -1;
}

int16_t influxdb_Mount_show_measurements(
    influxdb_Mount this,
    corto_string pattern,
    corto_ll results)
{
    corto_string request = corto_asprintf("SHOW MEASUREMENTS");
    char *encodedBuffer = httpclient_encode_fields(request);
    corto_string url = corto_asprintf("%s/query?db=%s", this->host, this->db);
    corto_string queryStr = corto_asprintf("q=%s", encodedBuffer);
    httpclient_Result r = httpclient_get(url, queryStr);
    corto_dealloc(queryStr);
    corto_dealloc(url);
    corto_dealloc(encodedBuffer);
    corto_dealloc(request);

    JSON_Value *response = NULL;

    corto_info("GET Result STATUS [%d] RESPONSE [%s]", r.status, r.response);

    if (r.status != 200) {
        corto_error("Show Measurements Query failed. Status [%d] Response [%s]",
            r.status, r.response);
        goto error;
    }

    struct influxdb_Query_Result result;
    response = json_parse_string(r.response);
    JSON_PTR_VERIFY(response, "Parson failed to parse Influxdb JSON response")

    if (influxdb_Mount_response_parse(response, &result)) {
        goto error;
    }

    if (influxdb_Mount_parse_measurements(&result, results)) {
        goto error;
    }

    JSON_SAFE_FREE(response)

    return 0;
error:
    JSON_SAFE_FREE(response)
    corto_error("Failed to process response. Error: [%s]", corto_lasterr());
    return -1;
}
