#include <driver/mnt/influxdb/query_response.h>
#include <driver/mnt/influxdb/query_response_parser.h>
#include <driver/mnt/influxdb/query_response_time.h>
#include <driver/mnt/influxdb/query_response_iter.h>

const corto_string INFLUX_TIMESTAMP_MEMBER = "timestamp";

int16_t influxdb_Mount_response_parse_id(
    const char* resultId,
    corto_string *parent,
    corto_string *id);

int16_t influxdb_Mount_response_result_value(
    corto_result *result,
    JSON_Value *value,
    JSON_Object *resultJson,
    corto_string name,
    bool convertTime)
{
    if (strcmp(name, "type") == 0) {
        return 0;
    }

    /* InfluxDB returns timestamps as "Time" --- Short circuit after update. */
    if (strcmp(name, "time") == 0) {
        if (convertTime == true) {
            if (influxdb_Mount_response_time(resultJson, value) == 0) {
                return 0;
            }
            else {
                corto_error("Failed to build timestamp JSON object.");
                goto error;
            }
        }
        else {
            /* Ignore time value if type does not have timestamp member */
            return 0;
        }
    }

    JSON_Value *v = json_value_deep_copy(value);
    JSON_Status ret = json_object_set_value(resultJson, name, v);
    if (ret != JSONSuccess) {
        corto_throw("Failed to set JSON result value.");
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_response_result_update(
    influxdb_Query_SeriesResult *series,
    JSON_Array *values,
    corto_result *r)
{
    corto_ptr_setstr(&r->type, series->type);

    JSON_Value *jsonValue = json_value_init_object();
    JSON_PTR_VERIFY(jsonValue, "Failed to create result JSON Value.")
    JSON_Object *json = json_value_get_object(jsonValue);
    JSON_PTR_VERIFY(json, "Failed to retrieve JSON result object.")

    JSON_Array *cols = series->columns;
    size_t i;
    size_t cnt = json_array_get_count(values);
    for (i = 0; i < cnt; i++) {
        corto_string col = influxdb_Mount_response_column_name(cols, i);
        if (!col) {
            goto error;
        }

        JSON_Value *target = json_array_get_value(values, i);
        JSON_PTR_VERIFY(target, "Failed to get response JSON value.")

        if (influxdb_Mount_response_result_value(
                r, target, json, col, series->convertTime)) {
            goto error;
        }
    }

    if (influxdb_Mount_response_parse_id(series->name, &r->parent, &r->id)) {
        goto error;
    }

    corto_string str = json_serialize_to_string(jsonValue);
    r->value = (corto_word)corto_strdup(str);

    json_free_serialized_string(str);
    JSON_SAFE_FREE(jsonValue)

    return 0;
error:
    JSON_SAFE_FREE(jsonValue)
    return -1;
}

int16_t influxdb_Mount_response_historical(
    influxdb_Mount this,
    influxdb_Query_SeriesResult *series,
    corto_result *result)
{
    influxdb_Mount_iterData *data = influxdb_Mount_iterDataNew(series);
    if (!data) {
        corto_throw("Failed to create historical response iterator data.");
        goto error;
    }

    result->history.ctx = data;
    result->history.hasNext = &influxdb_Mount_iterDataHasNext;
    result->history.next = influxdb_Mount_iterDataNext;
    result->history.release = influxdb_Mount_iterDataRelease;

    corto_info("Historical built");

    return 0;
error:
    return -1;
}

/*
 * Process Values Array
 * "values": []
 */
int16_t influxdb_Mount_response_process_values(
    influxdb_Mount this,
    influxdb_Query_SeriesResult *series,
    void* data)
{
    if (influxdb_Mount_response_result_type(series)) {
        goto error;
    }
    influxdb_Mount_ResonseFilter *filter = (influxdb_Mount_ResonseFilter *)data;

    JSON_Array *v = json_array_get_array(series->values, 0);
    JSON_PTR_VERIFY(v, "Resolved invalid JSON value.")

    corto_result *r = corto_ptr_new(corto_result_o);

    if (influxdb_Mount_response_result_update(series, v, r)) {
        corto_throw("Failed to process query response.");
        goto error;
    }

    if (filter->historical == true) {
        if (influxdb_Mount_response_historical(this, series, r) != 0) {
            corto_throw("Failed to process historical query response.");
            goto error;
        }
    }

    corto_mount_return(this, r);
    corto_ptr_free(r, corto_result_o);

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_query_response_handler(
    influxdb_Mount this,
    httpclient_Result *r,
    influxdb_Mount_ResonseFilter *filter)
{
    JSON_Value *response = NULL;

    corto_trace("GET Result STATUS [%d] RESPONSE [%s]", r->status, r->response);

    if (r->status != 200) {
        corto_throw("Query failed. Status [%d] Response [%s]",
            r->status, r->response);
        goto error;
    }

    influxdb_Query_Result result = {
        &influxdb_Mount_response_process_values,
        this,
        filter
    };

    response = json_parse_string(r->response);
    JSON_PTR_VERIFY(response, "Parson failed to parse Influxdb JSON response")

    if (influxdb_Mount_response_parse(response, &result) != 0) {
        goto error;
    }

    JSON_SAFE_FREE(response)

    return 0;
error:
    JSON_SAFE_FREE(response)
    corto_error("Failed to process response.");
    return -1;
}

int16_t influxdb_Mount_response_result_type(
    influxdb_Query_SeriesResult *series)
{
    JSON_Array *values = json_array_get_array(series->values, 0);
    JSON_PTR_VERIFY(values, "Parsing index [0] values for type.")

    int typePos = influxdb_Mount_response_column_index(series->columns, "type");
    if (typePos == -1) {
        corto_throw("Parsing [type] index.");
        goto error;
    }

    corto_string type = (corto_string)json_array_get_string(values, (size_t)typePos);
    if (!type) {
        corto_throw("Failed to identify result type. Expected [str] at [%zu].",
            (size_t)typePos);
        goto error;
    }

    corto_type t = corto_resolve(root_o, type);
    if (!t) {
        corto_error("Failed to resolve type [%s]", type);
        goto error;
    }

    series->type = type;

    corto_member m = corto_interface_resolveMember(t, INFLUX_TIMESTAMP_MEMBER);
    if (m) {
        series->convertTime = true;
    }
    else {
        series->convertTime = false;
    }

    return 0;
error:
    corto_throw("Failed to resolve series type.");
    return -1;
}

int16_t influxdb_Mount_response_parse_id(
    const char* resultId,
    corto_string *parent,
    corto_string *id)
{
    size_t length = strlen(resultId);
    int pos;
    for (pos = length-1; pos > 0; pos--) {
        if (resultId[pos] == '/') {
            break;
        }
    }

    if (pos == 0) {
        *parent = corto_strdup(".");
        *id = corto_strdup((corto_string)resultId);
        return 0;
    }

    int idLength = length-pos;
    char newParent[pos];
    char newId[idLength+1];
    int i;

    for (i = 0; i < pos; i++) {
        newParent[i] = resultId[i];
    }
    newParent[pos] = '\0';

    for (i = 0; i < idLength; i++) {
        newId[i] = resultId[i + pos + 1];
    }
    newId[length-pos] = '\0';

    *parent = corto_strdup(newParent);
    *id = corto_strdup(newId);

    return 0;
}
