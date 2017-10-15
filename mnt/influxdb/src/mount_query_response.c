#include <include/mount_query_response.h>
#include <driver/fmt/json/json.h>

#define VERIFY_JSON_PTR(ptr, msg) if (!ptr) { \
    corto_seterr(msg); \
    goto error; }

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

struct influxdb_Mount_response_column {
    int             pos;
    corto_string    name;
};

influxdb_Mount influxdb_response_mount = NULL;
corto_query *influxdb_response_query = NULL;
corto_ll influxdb_response_columns = NULL;
///TODO Ensure Columns are clear

void influxdb_Mount_query_response_destruct_columns(void);

int16_t influxdb_Mount_query_response_process_values(JSON_Array *values)
{

    return 0;
error:
    return -1;
}

/*
 * Build a linked list of column names and their position for parsing values
 */
int16_t influxdb_Mount_query_response_process_columns(JSON_Array *columns)
{
    influxdb_Mount_query_response_destruct_columns(); // Clear existing columns
    influxdb_response_columns = corto_ll_new();

    size_t cnt = json_array_get_count(columns);
    size_t i;
    for (i = 0; i < cnt; i++) {
        const char *columnName = json_array_get_string(columns, i);
        VERIFY_JSON_PTR(columnName, "Failed to parse results [columns].")

        struct influxdb_Mount_response_column *column = malloc(sizeof(struct influxdb_Mount_response_column));
        column->pos = i;
        column->name = corto_asprintf("%s", columnName);

        corto_ll_append(influxdb_response_columns, (void*)column);
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_query_response_parse_results(JSON_Object *result)
{
    JSON_Array *columns = json_object_get_array(result, "columns");
    VERIFY_JSON_PTR(columns, "Failed to find [columns] object in response.")
    if (influxdb_Mount_query_response_process_columns(columns) != 0) {
        goto error;
    }

    JSON_Array *values = json_object_get_array(result, "values");
    VERIFY_JSON_PTR(values, "Failed to find [values] object in response.")
    if (influxdb_Mount_query_response_process_values(values) != 0) {
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_query_response_handler(
    influxdb_Mount this,
    corto_query *query,
    httpclient_Result *result)
{
    influxdb_response_mount = this;
    influxdb_response_query = query;

    corto_info("GET Result STATUS [%d] RESPONSE [%s]",
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
            if (influxdb_Mount_query_response_parse_results(resultsObj) != 0) {
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
    return -1;
}

void influxdb_Mount_query_response_destruct_columns(void) {
    if (!influxdb_response_columns) {
        return;
    }

    while (corto_ll_size(influxdb_response_columns) > 0) {
        struct influxdb_Mount_response_column *column =
            (struct influxdb_Mount_response_column*)corto_ll_takeFirst(influxdb_response_columns);
        corto_dealloc(column->name);
        free(column);
    }

    corto_ll_clear(influxdb_response_columns); // Ensure list is empty
    corto_ll_free(influxdb_response_columns);
    influxdb_response_columns = NULL;
}
