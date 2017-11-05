#include <driver/mnt/influxdb/query_response.h>
#include <driver/mnt/influxdb/query_response_parser.h>
#include <driver/mnt/influxdb/query_response_time.h>

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
int16_t influxdb_Mount_response_historical(
    influxdb_Mount this,
    JSON_Array *values,
    struct influxdb_Query_Result *result)
{
    corto_result *r = corto_ptr_new(corto_result_o);
    JSON_Array *cols = result->columns;

    JSON_Value *jsonValue = json_value_init_object();
    JSON_PTR_VERIFY(jsonValue, "Failed to create result JSON Value.")
    JSON_Object *json = json_value_get_object(jsonValue);
    JSON_PTR_VERIFY(json, "Failed to retrieve JSON result object.")

    size_t i;
    size_t cnt = json_array_get_count(values);
    for (i = 0; i < cnt; i++) {
        corto_string col = influxdb_Mount_response_column_name(cols, i);
        if (!col) {
            goto error;
        }

        JSON_Value *target = json_array_get_value(values, i);
        JSON_PTR_VERIFY(target, "Failed to get response JSON value.")

        if (influxdb_Mount_response_build_result(r, target, json, col) != 0) {
            goto error;
        }
    }

    corto_ptr_setstr(&r->id, (corto_string)result->name);
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
int16_t influxdb_Mount_response_query(
    influxdb_Mount this,
    JSON_Array *values,
    struct influxdb_Query_Result *result)
{
    corto_result *r = corto_ptr_new(corto_result_o);
    JSON_Array *cols = result->columns;

    JSON_Value *jsonValue = json_value_init_object();
    JSON_PTR_VERIFY(jsonValue, "Failed to create result JSON Value.")
    JSON_Object *json = json_value_get_object(jsonValue);
    JSON_PTR_VERIFY(json, "Failed to retrieve JSON result object.")

    size_t i;
    size_t cnt = json_array_get_count(values);
    for (i = 0; i < cnt; i++) {
        corto_string col = influxdb_Mount_response_column_name(cols, i);
        if (!col) {
            goto error;
        }

        JSON_Value *target = json_array_get_value(values, i);
        JSON_PTR_VERIFY(target, "Failed to get response JSON value.")

        if (influxdb_Mount_response_build_result(r, target, json, col) != 0) {
            goto error;
        }
    }

    corto_ptr_setstr(&r->id, (corto_string)result->name);
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
    struct influxdb_Query_Result *result)
{
    if (this->super.policy.mask == CORTO_MOUNT_HISTORY_QUERY) {
        size_t i;
        for (i = 0; i < result->valueCount; i++) {
            JSON_Array *v = json_array_get_array(result->values, i);
            if (v == NULL) {
                corto_string error =
                corto_asprintf("Resolved invalid JSON value at index [%zu]", i);
                JSON_PTR_VERIFY(v, error)
                corto_dealloc(error);
            }
            if (influxdb_Mount_response_historical(this, v, result) != 0) {
                goto error;
            }
        }
    }
    else {
        JSON_Array *v = json_array_get_array(result->values, 0);
        JSON_PTR_VERIFY(v, "Resolved invalid JSON value.")
        if (influxdb_Mount_response_query(this, v, result) != 0) {
            goto error;
        }
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_Mount_query_response_handler(
    influxdb_Mount this,
    httpclient_Result *r)
{
    corto_info("GET Result STATUS [%d] RESPONSE [%s]", r->status, r->response);

    if (r->status != 200) {
        corto_seterr("Query failed. Status [%d] Response [%s]",
            r->status, r->response);
        goto error;
    }

    struct influxdb_Query_Result result = {NULL, NULL, NULL, 0};
    JSON_Value *response = json_parse_string(r->response);
    JSON_PTR_VERIFY(response, "Parson failed to parse Influxdb JSON response")

    if (influxdb_Mount_response_parse(response, &result) != 0) {
        goto error;
    }

    if (influxdb_Mount_response_process_values(this, &result) != 0) {
        goto error;
    }

    JSON_SAFE_FREE(response)

    return 0;
error:
    JSON_SAFE_FREE(response)
    corto_error("Failed to process response. Error: [%s]", corto_lasterr());
    return -1;
}
