#include <driver/mnt/influxdb/query_response_parser.h>

int16_t influxdb_Mount_response_parse_verify_result(
    struct influxdb_Query_SeriesResult *series)
{
    if (!series->name) {
        corto_seterr("Failed to parse [name] in response.");
        goto error;
    }
    if (!series->values) {
        corto_seterr("Failed to parse [values] in response.");
        goto error;
    }
    if (!series->columns) {
        corto_seterr("Failed to parse [columns] in response.");
        goto error;
    }
    if (series->valueCount <= 0) {
        corto_seterr("Empty value count.");
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_response_parse_series(
    JSON_Object *series,
    struct influxdb_Query_Result *result)
{
    struct influxdb_Query_SeriesResult r = {NULL, NULL, NULL, 0, false, NULL};
    const char* name = json_object_get_string(series, "name");
    if (name) {
        r.name = name;
    }
    else {
        r.name = " ";
    }

    r.columns = json_object_get_array(series, "columns");
    JSON_PTR_VERIFY(r.columns, "Failed to find [columns] array.")

    r.values = json_object_get_array(series, "values");
    JSON_PTR_VERIFY(r.values, "Failed to find [values] object in series.")

    r.valueCount = json_array_get_count(r.values);
    if (r.valueCount <= 0) {
        corto_trace("No matching samples in [%s] database", result->ctx->db);
        goto error;
    }

    if (influxdb_Mount_response_parse_verify_result(&r) != 0) {
        goto error;
    }
    if (result->callback(result->ctx, &r, result->data) != 0) {
        corto_seterr("Failed to process series response. Callback failed "\
            "with error [%s]", corto_lasterr());
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_response_parse_results(
    JSON_Object *jsonResult,
    struct influxdb_Query_Result *result)
{
    JSON_Array *series = json_object_get_array(jsonResult, "series");
    if (series == NULL) {
        corto_trace("No results matching query.");
        goto empty;
    }

    size_t seriesCount = json_array_get_count(series);
    size_t i;
    for (i = 0; i < seriesCount; i++) {
        JSON_Object *o = json_array_get_object(series, i);
        JSON_PTR_VERIFY(o, "Failed to resolve series response JSON object.");
        if (influxdb_Mount_response_parse_series(o, result) != 0) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
empty:
    return 1;
}

int16_t influxdb_Mount_response_parse(
    JSON_Value *responseValue,
    struct influxdb_Query_Result *result)
{
    JSON_Object *response = json_value_get_object(responseValue);
    JSON_PTR_VERIFY(response, "JSON Response is not an object")

    JSON_Array *results = json_object_get_array(response, "results");
    JSON_PTR_VERIFY(results, "Could not parse JSON Response for [results]")

    size_t cnt = json_array_get_count(results);
    size_t i;
    for (i = 0; i < cnt; i++) {
        JSON_Value *resultVal = json_array_get_value(results, i);
        JSON_PTR_VERIFY(resultVal, "Failed to parse results json array.")
        if (json_value_get_type(resultVal) == JSONObject) {
            JSON_Object *obj = json_value_get_object(resultVal);
            JSON_PTR_VERIFY(obj, "Failed to resolve results object.")
            int ret = influxdb_Mount_response_parse_results(obj, result);
            if (ret == -1) {
                goto error;
            }
            else if (ret == 1) {
                goto empty;
            }
        }
    }

    return 0;
empty:
    return 0;
error:
    corto_seterr("Failed to parse response. Error: [%s]", corto_lasterr());
    return -1;
}

corto_string influxdb_Mount_response_column_name(
    JSON_Array *cols,
    int pos)
{
    const char* column = json_array_get_string(cols, pos);
    JSON_PTR_VERIFY(column, "Failed to get column name from JSON columns.")

    return (corto_string)column;
error:
    return NULL;
}

int influxdb_Mount_response_column_index(
    JSON_Array *columns,
    size_t count,
    corto_string name)
{
    size_t i;
    for (i = 0; i < count; i++) {
        const char* column = json_array_get_string(columns, i);
        JSON_PTR_VERIFY(column, "Failed to resolve column index.");
        if (strcmp(column, name) == 0) {
            return i;
        }
    }

    return -1;
error:
    return -1;
}
