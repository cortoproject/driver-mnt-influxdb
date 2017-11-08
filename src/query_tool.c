#include <driver/mnt/influxdb/query_response_parser.h>
#include <driver/mnt/influxdb/query_builder.h>
#include <driver/mnt/influxdb/query_tool.h>

int16_t influxdb_Mount_parse_show_measurements_process(
    influxdb_Mount this,
    struct influxdb_Query_SeriesResult *result,
    void* data)
{
    corto_ll resultList = (corto_ll)data;
    size_t i;
    for (i = 0; i < result->valueCount; i++) {
        JSON_Array *values = json_array_get_array(result->values, i);
        JSON_PTR_VERIFY(values, "Failed to parse response values JSON array.")

        size_t j;
        size_t cnt = json_array_get_count(values);
        for (j = 0; j < cnt; j++) {
            JSON_Value *v = json_array_get_value(values, j);
            JSON_PTR_VERIFY(v, "Failed to get response JSON value.")
            const char* r = json_value_get_string(v);
            corto_ll_append(resultList, (void*)corto_strdup(r));
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
    corto_string regex = influxdb_Mount_query_builder_regex(pattern);
    corto_string request = corto_asprintf("SHOW MEASUREMENTS WITH MEASUREMENT =~/%s/",
        regex);
    char *encodedBuffer = httpclient_encode_fields(request);
    corto_string url = corto_asprintf("%s/query?db=%s", this->host, this->db);
    corto_string queryStr = corto_asprintf("q=%s", encodedBuffer);
    httpclient_Result r = httpclient_get(url, queryStr);
    corto_dealloc(queryStr);
    corto_dealloc(url);
    corto_dealloc(encodedBuffer);
    corto_dealloc(request);
    corto_dealloc(regex);

    JSON_Value *response = NULL;

    if (r.status != 200) {
        corto_error("Show Measurements Query failed. Status [%d] Response [%s]",
            r.status, r.response);
        corto_seterr("Response Status = 400");
        goto error;
    }

    struct influxdb_Query_Result result = {
        &influxdb_Mount_parse_show_measurements_process,
        this,
        results
    };

    response = json_parse_string(r.response);
    JSON_PTR_VERIFY(response, "Parson failed to parse Influxdb JSON response")
    if (influxdb_Mount_response_parse(response, &result)) {
        goto error;
    }

    JSON_SAFE_FREE(response)

    return 0;
error:
    JSON_SAFE_FREE(response)
    corto_error("Failed to process response. Error: [%s]", corto_lasterr());
    return -1;
}

int16_t influxdb_Mount_show_measurements_free(
    corto_ll results)
{
    while (corto_ll_size(results) > 0) {
        corto_string str = (corto_string)corto_ll_takeFirst(results);
        corto_dealloc(str);
    }

    return 0;
}

int16_t influxdb_Mount_show_retentionPolicies_process(
    influxdb_Mount this,
    struct influxdb_Query_SeriesResult *result,
    void* data)
{
    corto_ll resultList = (corto_ll)data;
    size_t i;
    for (i = 0; i < result->valueCount; i++) {
        JSON_Array *values = json_array_get_array(result->values, i);
        JSON_PTR_VERIFY(values, "Failed to parse response values JSON array.")

        size_t j;
        size_t cnt = json_array_get_count(values);

        influxdb_Query_RetentionPolicyResult *rp =
            malloc(sizeof(influxdb_Query_RetentionPolicyResult));
        for (j = 0; j < cnt; j++) {
            const char* name = influxdb_Mount_response_column_name(
                result->columns, j);
            if (!name) {
                corto_seterr("Invalid rp column index [%d]", j);
                free(rp);
                goto error;
            }

            JSON_Value *v = json_array_get_value(values, j);
            if (!v) {
                corto_seterr("Invalid rp value index [%d]", j);
                free(rp);
                goto error;
            }

            if (strcmp(name, "name") == 0) {
                rp->name = corto_strdup(json_value_get_string(v));
            }
            else if (strcmp(name, "duration") == 0) {
                rp->duration = corto_strdup(json_value_get_string(v));
            }
            else if (strcmp(name, "shardGroupDuration") == 0) {
                rp->sgDuration = corto_strdup(json_value_get_string(v));
            }
            else if (strcmp(name, "replicaN") == 0) {
                rp->replication = (int)json_value_get_number(v);
            }
            else if (strcmp(name, "default") == 0) {
                rp->def = json_value_get_boolean(v);
            }

        }
        corto_ll_append(resultList, (void*)rp);
    }

    return 0;

error:
    return -1;
}

int16_t influxdb_Mount_show_retentionPolicies(
    influxdb_Mount this,
    corto_ll results)
{
    corto_string request = corto_asprintf("SHOW RETENTION POLICIES");
    char *encodedBuffer = httpclient_encode_fields(request);
    corto_string url = corto_asprintf("%s/query?db=%s", this->host, this->db);
    corto_string queryStr = corto_asprintf("q=%s", encodedBuffer);
    httpclient_Result r = httpclient_get(url, queryStr);
    corto_dealloc(queryStr);
    corto_dealloc(url);
    corto_dealloc(encodedBuffer);
    corto_dealloc(request);

    JSON_Value *response = NULL;

    if (r.status != 200) {
        corto_error("Show Measurements Query failed. Status [%d] Response [%s]",
            r.status, r.response);
        corto_seterr("Response Status = 400");
        goto error;
    }

    corto_info("Show Retention Policy Query Status [%d] Response [%s]",
        r.status, r.response);

    struct influxdb_Query_Result result = {
        &influxdb_Mount_show_retentionPolicies_process,
        this,
        results
    };

    response = json_parse_string(r.response);
    JSON_PTR_VERIFY(response, "Parson failed to parse Influxdb JSON response")
    if (influxdb_Mount_response_parse(response, &result)) {
        goto error;
    }

    JSON_SAFE_FREE(response)

    return 0;
error:
    JSON_SAFE_FREE(response)
    corto_error("Failed to process response. Error: [%s]", corto_lasterr());
    return -1;
}

int16_t influxdb_Mount_show_retentionPolicies_free(
    corto_ll results)
{
    while (corto_ll_size(results) > 0) {
        influxdb_Query_RetentionPolicyResult *rp =
            (influxdb_Query_RetentionPolicyResult*)corto_ll_takeFirst(results);
        corto_dealloc(rp->name);
        corto_dealloc(rp->duration);
        corto_dealloc(rp->sgDuration);
        free(rp);
    }

    return 0;
}
