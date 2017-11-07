#include <driver/mnt/influxdb/query_response.h>
#include <driver/mnt/influxdb/query_response_parser.h>
#include <driver/mnt/influxdb/query_response_time.h>

const corto_string INFLUX_TIMESTAMP_MEMBER = "timestamp";

int16_t influxdb_Mount_response_result_type(
    struct influxdb_Query_SeriesResult *result);

int16_t influxdb_Mount_response_result_id(
    struct influxdb_Query_SeriesResult *result,
    corto_result *r);

int16_t influxdb_Mount_response_build_result(
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
    struct influxdb_Query_SeriesResult *result)
{
    corto_result *r = corto_ptr_new(corto_result_o);
    corto_ptr_setstr(&r->type, (corto_string)result->type);
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

        if (influxdb_Mount_response_build_result(
                r, target, json, col, result->convertTime)) {
            goto error;
        }
    }

    influxdb_Mount_response_result_id(result, r);

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
    struct influxdb_Query_SeriesResult *result)
{
    corto_result *r = corto_ptr_new(corto_result_o);
    corto_ptr_setstr(&r->type, (corto_string)result->type);
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

        if (influxdb_Mount_response_build_result(
                r, target, json, col, result->convertTime)) {
            goto error;
        }
    }

    influxdb_Mount_response_result_id(result, r);

    corto_string str = json_serialize_to_string(jsonValue);
    r->value = (corto_word)corto_strdup(str);
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
    struct influxdb_Query_SeriesResult *result,
    void* data)
{
    if (influxdb_Mount_response_result_type(result)) {
        goto error;
    }

    if (this->super.policy.mask == CORTO_MOUNT_HISTORY_QUERY) {
        size_t i;
        for (i = 0; i < result->valueCount; i++) {
            JSON_Array *v = json_array_get_array(result->values, i);
            if (!v) {
                corto_seterr("Resolved invalid JSON value at index [%s]", i);
                goto error;
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
    corto_trace("GET Result STATUS [%d] RESPONSE [%s]", r->status, r->response);

    if (r->status != 200) {
        corto_seterr("Query failed. Status [%d] Response [%s]",
            r->status, r->response);
        goto error;
    }

    struct influxdb_Query_Result result = {
        &influxdb_Mount_response_process_values,
        this,
        NULL
    };

    JSON_Value *response = json_parse_string(r->response);
    JSON_PTR_VERIFY(response, "Parson failed to parse Influxdb JSON response")

    if (influxdb_Mount_response_parse(response, &result) != 0) {
        goto error;
    }

    JSON_SAFE_FREE(response)

    return 0;
error:
    JSON_SAFE_FREE(response)
    corto_error("Failed to process response. Error: [%s]", corto_lasterr());
    return -1;
}

int16_t influxdb_Mount_response_result_type(
    struct influxdb_Query_SeriesResult *result)
{
    JSON_Array *values = json_array_get_array(result->values, 0);
    JSON_PTR_VERIFY(values, "Parsing index [0] values for type.")

    int typePos = influxdb_Mount_response_column_index(
        result->columns, result->valueCount, "type");
    if (typePos == -1) {
        corto_seterr("Parsing [type] index.");
        goto error;
    }

    const char* type = json_array_get_string(values, (size_t)typePos);
    if (!type) {
        corto_seterr("Failed to identify result type. Expected [str] at [%zu].",
            (size_t)typePos);
        goto error;
    }

    corto_type t = corto_resolve(root_o, (corto_string)type);
    if (!t) {
        corto_error("Failed to resolve type [%s]", type);
        goto error;
    }

    result->type = type;

    corto_member m = corto_interface_resolveMember(t, INFLUX_TIMESTAMP_MEMBER);
    if (m) {
        result->convertTime = true;
    }
    else {
        result->convertTime = false;
    }

    return 0;
error:
    corto_seterr("Failed to resolve series type. Error: %s",
        corto_lasterr());
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

int16_t influxdb_Mount_response_result_id(
    struct influxdb_Query_SeriesResult *result,
    corto_result *r)
{
    if (influxdb_Mount_response_parse_id(result->name, &r->parent, &r->id)) {
        goto error;
    }

    return 0;
error:
    return -1;
}
