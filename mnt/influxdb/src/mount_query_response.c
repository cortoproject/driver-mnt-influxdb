#include <include/mount_query_response.h>
#include <driver/fmt/json/json.h>

#define VERIFY_JSON_PTR(ptr, msg) if (!ptr) { \
    corto_seterr(msg); \
    goto error; }

struct influxdb_Mount_response_column {
    int             pos;
    corto_string    name;
};

influxdb_Mount influxdb_response_mount = NULL;
corto_query *influxdb_response_query = NULL;
JSON_Array *influxdb_response_columns = NULL;
corto_string influxdb_response_name = NULL;

corto_string influxdb_Mount_query_response_column_name(int pos);

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

/* https://stackoverflow.com/questions/7114690/how-to-parse-syslog-timestamp */
int16_t influxdb_Mount_parse_rfc3339_string_time(const char *timeStr, struct timespec *ts)
{
    struct tm tm;
    time_t t;
    char *extra = strptime(timeStr, "%C%y-%m-%dT%H:%M:%S", &tm );
    tm.tm_isdst = -1;
    t = mktime(&tm);

    if (extra && extra[0] == '.') {
        char *endptr;
        double fraction = strtod( extra, &endptr );
        extra = endptr;

        /* use timespec if fractional seconds required */
        ts->tv_sec = t;
        ts->tv_nsec = fraction * 1000000000;
    }
    else {
        goto error;
    }

    return 0;
error:
    return -1;
invalid:
    ///TODO improve format checks
    // corto_seterr("RFC3339 String Format Invalid [%s]", timeStr);
    ts->tv_sec = 0;
    ts->tv_nsec = 0;
    return 0;
}

int16_t influxdb_Mount_query_response_build_result_time(
    JSON_Object *output,
    JSON_Value *value)
{
    struct timespec ts;
    JSON_Value *v = NULL;
    if (json_value_get_type(value) == JSONString) {
        const char *timeStr = json_value_get_string(value);
        if (!timeStr) {
            corto_seterr("Failed to parse timestamp string.");
            goto error;
        }
        if (influxdb_Mount_parse_rfc3339_string_time(timeStr, &ts) != 0) {
            corto_error("Failed to parse timestamp.");
            goto error;
        }

        // corto_info("Format [%s] Output Sec [%d] Nano [%d]",
            // timeStr, ts.tv_sec, ts.tv_nsec);
        // corto_info("EPOCH [%lld.%.9ld]", (long long)ts.tv_sec, ts.tv_nsec);
    }

    v = json_value_init_object();
    VERIFY_JSON_PTR(v, "Failed to create timestamp JSON Value.")
    JSON_Object *o = json_value_get_object(v);
    VERIFY_JSON_PTR(o, "Failed to retrieve timestamp JSON object.")

    JSON_Status s;

    s = json_object_set_number(o, "sec", ts.tv_sec);
    if (s != JSONSuccess) {
        corto_seterr("Failed to set timestamp object seconds");
        goto error;
    }

    s = json_object_set_number(o, "nanosec", ts.tv_nsec);
    if (s != JSONSuccess) {
        corto_seterr("Failed to set timestamp object nanoseconds");
        goto error;
    }

    s = json_object_set_value(output, "timestamp", json_value_deep_copy(v));
    if (s != JSONSuccess) {
        corto_seterr("Failed to set JSON result value.");
        goto error;
    }

    json_value_free(v);

    return 0;
error:
    if (v) {
        json_value_free(v);
    }
    return -1;
}

int16_t influxdb_Mount_query_response_build_result(
    corto_result *result,
    JSON_Value *value,
    JSON_Object *resultJson,
    corto_string name)
{
    if (strcmp(name, "type") == 0) {
        const char* type = json_value_get_string(value);
        VERIFY_JSON_PTR(type, "Failed to identify result type. Expected [str].")
        corto_ptr_setstr(&result->type, (corto_string)type);
        return 0;
    }

    /* InfluxDB returns timestamps as "Time"
     * Format 2017-10-17T02:25:41.60012734Z
     */
    if (strcmp(name, "time") == 0) {
        int ret = influxdb_Mount_query_response_build_result_time(
            resultJson, value);
        if (ret == 0) {
            return 0;
        }
        else {
            corto_error("Failed to build timestamp JSON object.");
            goto error;
        }
    }

    JSON_Status ret = json_object_set_value(
        resultJson,
        name,
        json_value_deep_copy(value));
    if (ret != JSONSuccess) {
        corto_seterr("Failed to set JSON result value.");
        goto error;
    }

    return 0;
error:
    return -1;
}

/*
* Hitorical Query Response
 * Process Value Array Index from influxdb_Mount_query_response_process_values
 * ["valueA", "valueB", "ValueC", ...]
 */
int16_t influxdb_Mount_query_response_process_history_value(JSON_Array *values)
{
    corto_result *result = corto_ptr_new(corto_result_o);

    JSON_Value *jsonValue = json_value_init_object();
    VERIFY_JSON_PTR(jsonValue, "Failed to create result JSON Value.")
    JSON_Object *jsonResult = json_value_get_object(jsonValue);
    VERIFY_JSON_PTR(jsonResult, "Failed to retrieve JSON result object.")

    size_t i;
    size_t cnt = json_array_get_count(values);
    for (i = 0; i < cnt; i++) {
        corto_string name = influxdb_Mount_query_response_column_name(i);
        if (!name) {
            goto error;
        }

        JSON_Value *targetValue = json_array_get_value(values, i);
        VERIFY_JSON_PTR(targetValue, "Failed to get response JSON value.")

        if (influxdb_Mount_query_response_build_result(
            result, targetValue, jsonResult, name) != 0) {
            goto error;
        }
    }

    corto_ptr_setstr(&result->id, influxdb_response_name);
    corto_ptr_setstr(&result->parent, ".");

    corto_string jsonStr = json_serialize_to_string(jsonValue);
    result->value = (corto_word)corto_strdup(jsonStr);
    json_free_serialized_string(jsonStr);

    corto_mount_return(influxdb_response_mount, result);
    corto_ptr_free(result, corto_result_o);
    return 0;
error:
    if (jsonValue) {
        json_value_free(jsonValue);
        jsonValue = NULL;
    }
    corto_ptr_free(result, corto_result_o);
    return -1;
}

/*
 * Process Value Array Index from influxdb_Mount_query_response_process_values
 * ["valueA", "valueB", "ValueC", ...]
 */
int16_t influxdb_Mount_query_response_process_value(JSON_Array *values)
{
    corto_result *result = corto_ptr_new(corto_result_o);

    JSON_Value *jsonValue = json_value_init_object();
    VERIFY_JSON_PTR(jsonValue, "Failed to create result JSON Value.")
    JSON_Object *jsonResult = json_value_get_object(jsonValue);
    VERIFY_JSON_PTR(jsonResult, "Failed to retrieve JSON result object.")

    size_t i;
    size_t cnt = json_array_get_count(values);
    for (i = 0; i < cnt; i++) {
        corto_string name = influxdb_Mount_query_response_column_name(i);
        if (!name) {
            goto error;
        }

        JSON_Value *targetValue = json_array_get_value(values, i);
        VERIFY_JSON_PTR(targetValue, "Failed to get response JSON value.")

        if (influxdb_Mount_query_response_build_result(
                result, targetValue, jsonResult, name) != 0) {
            goto error;
        }
    }

    corto_ptr_setstr(&result->id, influxdb_response_name);
    corto_ptr_setstr(&result->parent, ".");

    corto_string jsonStr = json_serialize_to_string(jsonValue);
    result->value = (corto_word)corto_strdup(jsonStr);
    // corto_info("Query Result: %s", jsonStr);
    json_free_serialized_string(jsonStr);

    corto_mount_return(influxdb_response_mount, result);
    corto_ptr_free(result, corto_result_o);
    return 0;
error:
    if (jsonValue) {
        json_value_free(jsonValue);
        jsonValue = NULL;
    }
    corto_ptr_free(result, corto_result_o);
    return -1;
}

/*
 * Process Values Array
 * "values": []
 */
int16_t influxdb_Mount_query_response_process_values(
    JSON_Array *values,
    bool historical)
{
    size_t cnt = json_array_get_count(values);
    if (cnt <= 0) {
        corto_seterr("Response does not contain any values.");
        goto error;
    }

    if (historical == true) {
        size_t i;
        for (i = 0; i < cnt; i++) {
            JSON_Array *v = json_array_get_array(values, i);
            VERIFY_JSON_PTR(v, "Failed to parse response values JSON array.")
            if (influxdb_Mount_query_response_process_history_value(v) != 0) {
                goto error;
            }
        }
    }
    else {
        JSON_Array *v = json_array_get_array(values, 0);
        VERIFY_JSON_PTR(v, "Failed to parse query response values JSON array.")
        if (influxdb_Mount_query_response_process_value(v) != 0) {
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
int16_t influxdb_Mount_query_response_process_columns(JSON_Array *columns)
{
    influxdb_response_columns = columns;

    return 0;
}

int16_t influxdb_Mount_query_response_parse_series(
    JSON_Object *series,
    bool historical)
{
    const char *name = json_object_get_string(series, "name");
    VERIFY_JSON_PTR(name, "Failed to find [name] in series object.")
    influxdb_response_name = (corto_string)name;
    JSON_Array *columns = json_object_get_array(series, "columns");
    VERIFY_JSON_PTR(columns, "Failed to find [columns] object in series.")
    if (influxdb_Mount_query_response_process_columns(columns) != 0) {
        goto error;
    }

    JSON_Array *values = json_object_get_array(series, "values");
    VERIFY_JSON_PTR(values, "Failed to find [values] object in series.")
    if (influxdb_Mount_query_response_process_values(values, historical) != 0) {
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_query_response_parse_results(
    JSON_Object *result,
    bool historical)
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
        VERIFY_JSON_PTR(o, "Failed to resolve series response JSON object.");
        if (influxdb_Mount_query_response_parse_series(o, historical) != 0) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
empty:
    return 0;
}

int16_t influxdb_Mount_query_response_handler(
    influxdb_Mount this,
    corto_query *query,
    httpclient_Result *result,
    bool historical)
{
    influxdb_response_mount = this;
    influxdb_response_query = query;

    corto_trace("GET Result STATUS [%d] RESPONSE [%s]",
        result->status, result->response);

    if (result->status != 200) {
        corto_seterr("Query failed. Status [%d] Response [%s]",
            result->status, result->response);
        goto error;
    }

    JSON_Value *responseVal = json_parse_string(result->response);
    VERIFY_JSON_PTR(responseVal, "Failed to parse Influxdb JSON response")

    JSON_Object *response = json_value_get_object(responseVal);
    VERIFY_JSON_PTR(response, "JSON Response is not an object")

    JSON_Array *results = json_object_get_array(response, "results");
    VERIFY_JSON_PTR(results, "Could not parse JSON Response for [results]")

    size_t cnt = json_array_get_count(results);
    size_t i;
    for (i = 0; i < cnt; i++) {
        JSON_Value *resultVal = json_array_get_value(results, i);
        VERIFY_JSON_PTR(resultVal, "Failed to parse results json array.")
        if (json_value_get_type(resultVal) == JSONObject) {
            JSON_Object *resultsObj = json_value_get_object(resultVal);
            VERIFY_JSON_PTR(resultsObj, "Failed to resolve results object.")
            if (influxdb_Mount_query_response_parse_results(
                resultsObj,
                historical) != 0) {
                goto error;
            }
        }
    }

    return 0;
error:
    if (responseVal) {
        json_value_free(responseVal);
        responseVal = NULL;
    }
    corto_error("Failed to process response. Error: [%s]", corto_lasterr());
    return -1;
}

corto_string influxdb_Mount_query_response_column_name(int pos) {
    if (!influxdb_response_columns) {
        return NULL;
    }

    corto_string column = (corto_string)json_array_get_string(
        influxdb_response_columns, pos);
    VERIFY_JSON_PTR(column, "Failed to get column name from JSON columns.")

    return column;
error:
    return NULL;
}