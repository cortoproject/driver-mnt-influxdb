#include <driver/mnt/influxdb/query_response.h>
#include <driver/mnt/influxdb/query_response_time.h>

#define JSON_PTR_VERIFY(ptr, msg) if (!ptr) { corto_seterr(msg); goto error; }
#define JSON_SAFE_FREE(v)if (v) { json_value_free(v); v = NULL; }

struct influxdb_Mount_response_column {
    int             pos;
    corto_string    name;
};

JSON_Array *influxdb_response_columns = NULL;
corto_string influxdb_response_name = NULL;

corto_string influxdb_Mount_response_column_name(
    int pos);

int16_t influxdb_Mount_response_build_result(
    corto_result *result,
    JSON_Value *value,
    JSON_Object *resultJson,
    corto_string name)
{
    if (strcmp(name, "type") == 0) {
        const char* type = json_value_get_string(value);
        JSON_PTR_VERIFY(type, "Failed to identify result type. Expected [str].")
        corto_ptr_setstr(&result->type, (corto_string)type);
        return 0;
    }

    /* InfluxDB returns timestamps as "Time" --- Short circuit after update. */
    if (strcmp(name, "time") == 0) {
        if (influxdb_Mount_response_time(resultJson, value) == 0) {
            return 0;
        }
        else {
            corto_error("Failed to build timestamp JSON object.");
            goto error;
        }
    }

    JSON_Value *v = json_value_deep_copy(value);
    JSON_Status ret = json_object_set_value(resultJson, name, v);
    if (ret != JSONSuccess) {
        corto_seterr("Failed to set JSON result value.");
        goto error;
    }

    return 0;
error:
    return -1;
}

/*
*  Hitorical Query Response
 * Process Value Array Index
 * ["valueA", "valueB", "ValueC", ...]
 */
int16_t influxdb_Mount_response_history_value(
    influxdb_Mount this,
    JSON_Array *values)
{
    corto_result *r = corto_ptr_new(corto_result_o);

    JSON_Value *jsonValue = json_value_init_object();
    JSON_PTR_VERIFY(jsonValue, "Failed to create result JSON Value.")
    JSON_Object *json = json_value_get_object(jsonValue);
    JSON_PTR_VERIFY(json, "Failed to retrieve JSON result object.")

    size_t i;
    size_t cnt = json_array_get_count(values);
    for (i = 0; i < cnt; i++) {
        corto_string name = influxdb_Mount_response_column_name(i);
        if (!name) {
            goto error;
        }

        JSON_Value *target = json_array_get_value(values, i);
        JSON_PTR_VERIFY(target, "Failed to get response JSON value.")

        if (influxdb_Mount_response_build_result(r, target, json, name) != 0) {
            goto error;
        }
    }

    corto_ptr_setstr(&r->id, influxdb_response_name);
    corto_ptr_setstr(&r->parent, ".");

    corto_string jsonStr = json_serialize_to_string(jsonValue);
    r->value = (corto_word)corto_strdup(jsonStr);
    json_free_serialized_string(jsonStr);

    corto_mount_return(this, r);

    corto_ptr_free(r, corto_result_o);

    return 0;
error:
    JSON_SAFE_FREE(jsonValue)
    corto_ptr_free(r, corto_result_o);
    return -1;
}

/*
 * Processes and Returns onQuery Response
 *
 * Process Value Array Index
 * ["valueA", "valueB", "ValueC", ...]
 *
 */
int16_t influxdb_Mount_response_query_value(
    influxdb_Mount this,
    JSON_Array *values)
{
    corto_result *r = corto_ptr_new(corto_result_o);

    JSON_Value *jsonValue = json_value_init_object();
    JSON_PTR_VERIFY(jsonValue, "Failed to create result JSON Value.")
    JSON_Object *json = json_value_get_object(jsonValue);
    JSON_PTR_VERIFY(json, "Failed to retrieve JSON result object.")

    size_t i;
    size_t cnt = json_array_get_count(values);
    for (i = 0; i < cnt; i++) {
        corto_string name = influxdb_Mount_response_column_name(i);
        if (!name) {
            goto error;
        }

        JSON_Value *target = json_array_get_value(values, i);
        JSON_PTR_VERIFY(target, "Failed to get response JSON value.")

        if (influxdb_Mount_response_build_result(r, target, json, name) != 0) {
            goto error;
        }
    }

    corto_ptr_setstr(&r->id, influxdb_response_name);
    corto_ptr_setstr(&r->parent, ".");

    corto_string str = json_serialize_to_string(jsonValue);
    r->value = (corto_word)corto_strdup(str);
    corto_info("Query Result Type [%s] Value: %s", r->type, str);
    json_free_serialized_string(str);

    corto_mount_return(this, r);
    corto_ptr_free(r, corto_result_o);
    return 0;
error:
    JSON_SAFE_FREE(jsonValue)
    corto_ptr_free(r, corto_result_o);
    return -1;
}

/*
 * Process Values Array
 * "values": []
 */
int16_t influxdb_Mount_response_process_values(
    influxdb_Mount this,
    JSON_Array *values)
{
    size_t cnt = json_array_get_count(values);
    if (cnt <= 0) {
        corto_seterr("Response does not contain any values.");
        goto error;
    }

    if (this->super.policy.mask == CORTO_MOUNT_HISTORY_QUERY) {
        size_t i;
        for (i = 0; i < cnt; i++) {
            JSON_Array *v = json_array_get_array(values, i);
            JSON_PTR_VERIFY(v, "Failed to parse response values JSON array.")
            if (influxdb_Mount_response_history_value(this, v) != 0) {
                goto error;
            }
        }
    }
    else {
        JSON_Array *v = json_array_get_array(values, 0);
        JSON_PTR_VERIFY(v, "Failed to parse query response values JSON array.")
        if (influxdb_Mount_response_query_value(this, v) != 0) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
}

/*
 * Build a linked list of column names and their position for parsing values
 */
int16_t influxdb_Mount_response_process_columns(JSON_Array *columns)
{
    influxdb_response_columns = columns;

    return 0;
}

int16_t influxdb_Mount_response_parse_series(
    influxdb_Mount this,
    JSON_Object *series)
{
    const char *name = json_object_get_string(series, "name");
    JSON_PTR_VERIFY(name, "Failed to find [name] in series object.")
    influxdb_response_name = (corto_string)name;
    JSON_Array *columns = json_object_get_array(series, "columns");
    JSON_PTR_VERIFY(columns, "Failed to find [columns] object in series.")
    if (influxdb_Mount_response_process_columns(columns) != 0) {
        goto error;
    }

    JSON_Array *values = json_object_get_array(series, "values");
    JSON_PTR_VERIFY(values, "Failed to find [values] object in series.")
    if (influxdb_Mount_response_process_values(this, values)) {
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_response_parse_results(
    influxdb_Mount this,
    JSON_Object *result)
{
    JSON_Array *series = json_object_get_array(result, "series");
    if (series == NULL) {
        corto_trace("No results matching query.");
        goto empty;
    }

    size_t cnt = json_array_get_count(series);
    size_t i;
    for (i = 0; i < cnt; i++) {
        JSON_Object *o = json_array_get_object(series, i);
        JSON_PTR_VERIFY(o, "Failed to resolve series response JSON object.");
        if (influxdb_Mount_response_parse_series(this, o) != 0) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
empty:
    return 0;
}

int16_t influxdb_Mount_response_handler(
    influxdb_Mount this,
    httpclient_Result *r)
{
    corto_info("GET Result STATUS [%d] RESPONSE [%s]", r->status, r->response);

    if (r->status != 200) {
        corto_seterr("Query failed. Status [%d] Response [%s]",
            r->status, r->response);
        goto error;
    }

    JSON_Value *responseVal = json_parse_string(r->response);
    JSON_PTR_VERIFY(responseVal, "Failed to parse Influxdb JSON response")

    JSON_Object *response = json_value_get_object(responseVal);
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
            if (influxdb_Mount_response_parse_results(this, obj)) {
                goto error;
            }
        }
    }

    return 0;
error:
    JSON_SAFE_FREE(responseVal)
    corto_error("Failed to process response. Error: [%s]", corto_lasterr());
    return -1;
}

corto_string influxdb_Mount_response_column_name(int pos) {
    if (!influxdb_response_columns) {
        return NULL;
    }

    const char* column = json_array_get_string(influxdb_response_columns, pos);
    JSON_PTR_VERIFY(column, "Failed to get column name from JSON columns.")

    return (corto_string)column;
error:
    return NULL;
}
