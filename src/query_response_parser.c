#include <driver/mnt/influxdb/query_response_parser.h>

int16_t influxdb_mount_series_deepCopy(
    influxdb_Query_SeriesResult *src,
    influxdb_Query_SeriesResult *dest)
{
    JSON_Array *values = NULL;
    JSON_Array *columns = NULL;
    JSON_Value *valuesCopy = NULL;
    JSON_Value *columnsCopy = NULL;

    JSON_Value *srcValues = json_array_get_wrapping_value(src->values);
    JSON_Value *srcColumns = json_array_get_wrapping_value(src->columns);

    valuesCopy = json_value_deep_copy(srcValues);
    JSON_PTR_VERIFY(valuesCopy, "Iterator value data copy.");
    values = json_value_get_array(valuesCopy);
    if (!values) {
        corto_throw("Failed to create iterator values.");
        goto error;
    }

    columnsCopy = json_value_deep_copy(srcColumns);
    JSON_PTR_VERIFY(columnsCopy, "Iterator column data copy.");
    columns = json_value_get_array(columnsCopy);
    if (!columns) {
        corto_throw("Failed to create iterator columns.");
        goto error;
    }

    size_t size = sizeof(influxdb_Query_SeriesResult);
    dest = (influxdb_Query_SeriesResult*)malloc(size);

    dest->name = corto_strdup(src->name);
    dest->values = values;
    dest->columns = columns;
    dest->valueCount = src->valueCount;
    dest->convertTime = src->convertTime;
    dest->type = corto_strdup(src->type);

    return 0;
error:
    JSON_SAFE_FREE(columnsCopy);
    JSON_SAFE_FREE(valuesCopy);
    return -1;
}

void influxdb_mount_series_free(
    influxdb_Query_SeriesResult *series)
{
    JSON_Value *values = json_array_get_wrapping_value(series->values);
    JSON_Value *columns = json_array_get_wrapping_value(series->columns);

    corto_dealloc(series->name);
    corto_dealloc(series->type);
    json_value_free(values);
    json_value_free(columns);

    free(series);
}

int16_t influxdb_mount_response_parse_verify_result(
    influxdb_Query_SeriesResult *series)
{
    if (!series->name) {
        corto_throw("Failed to parse [name] in response.");
        goto error;
    }
    if (!series->values) {
        corto_throw("Failed to parse [values] in response.");
        goto error;
    }
    if (!series->columns) {
        corto_throw("Failed to parse [columns] in response.");
        goto error;
    }
    if (series->valueCount <= 0) {
        corto_throw("Empty value count.");
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_mount_response_parse_series(
    JSON_Object *series,
    influxdb_Query_Result *result)
{
    influxdb_Query_SeriesResult r = {NULL, NULL, NULL, 0, false, NULL};
    corto_string name = (corto_string)json_object_get_string(series, "name");
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

    if (influxdb_mount_response_parse_verify_result(&r) != 0) {
        goto error;
    }
    if (result->callback(result->ctx, &r, result->data) != 0) {
        corto_throw("Failed to process series response. Callback Failed.");
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_mount_response_parse_results(
    JSON_Object *jsonResult,
    influxdb_Query_Result *result)
{
    JSON_Array *series = json_object_get_array(jsonResult, "series");
    if (series == NULL) {
        corto_trace("No results matching query.");
        goto empty;
    }

    size_t seriesCount = json_array_get_count(series);
    size_t i;

    influxdb_mount_ResonseFilter *filter =
        (influxdb_mount_ResonseFilter*)result->data;

    /* InfluxDB SLIMIT (Series Limit) and SOFFSET (series offset) to not
     * map perfectly to Corto's request. We process & filter the full result
     * set to return only the objects (series) that Corto is interested in */
     if (filter->limit > 0) {
         if (filter->offset > seriesCount) {
             corto_info("Requested Offset [%llu] > Series Results [%zu]",
                filter->limit, seriesCount);
             goto empty;
         }
         if ((filter->limit + filter->offset) > seriesCount) {
             i = filter->offset;
         } else {
             i = filter->offset;
             seriesCount = filter->limit + i;
         }
     } else {
         i = 0;
     }

    for (i = 0; i < seriesCount; i++) {
        JSON_Object *o = json_array_get_object(series, i);
        JSON_PTR_VERIFY(o, "Failed to resolve series response JSON object.");
        if (influxdb_mount_response_parse_series(o, result) != 0) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
empty:
    return 1;
}

int16_t influxdb_mount_response_parse(
    JSON_Value *responseValue,
    influxdb_Query_Result *result)
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
            int ret = influxdb_mount_response_parse_results(obj, result);
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
    corto_throw("Failed to parse response.");
    return -1;
}

corto_string influxdb_mount_response_column_name(
    JSON_Array *cols,
    int pos)
{
    const char* column = json_array_get_string(cols, pos);
    JSON_PTR_VERIFY(column, "Failed to get column name from JSON columns.")

    return (corto_string)column;
error:
    return NULL;
}

int influxdb_mount_response_column_index(
    JSON_Array *columns,
    corto_string name)
{
    size_t i;
    for (i = 0; i < json_array_get_count(columns); i++) {
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
