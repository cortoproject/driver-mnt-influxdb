#include <driver/mnt/influxdb/query_response.h>
#include <driver/mnt/influxdb/query_response_iter.h>

void *influxdb_Mount_iterDataNext(corto_iter *iter)
{
    influxdb_Mount_iterData *data = (influxdb_Mount_iterData*)iter->ctx;

    JSON_Array *values = json_array_get_array(data->series->values, data->pos);
    if (!values) {
        corto_seterr("Resolved invalid JSON value at index [%d]", data->pos);
        goto error;
    }

    if (influxdb_Mount_response_result_update(
        data->series, values, data->result)) {
        corto_seterr("Failed to update result for historical index [%d]",
            data->pos);
        goto error;
    }

    data->result->name = NULL;
    data->pos++;

    return &data->result;
error:
    return NULL;
}

int influxdb_Mount_iterDataHasNext(corto_iter *iter)
{
    influxdb_Mount_iterData *data = (influxdb_Mount_iterData*)iter->ctx;

    if (data == NULL)
    {
        corto_error("InfluxDB iterator is uninitialized.");
        return -1;
    }

    if ((data->pos+1) >= data->series->valueCount) {
        goto nodata;
    }

    return 1;
nodata:
    return 0;
}

void influxdb_Mount_iterDataRelease(corto_iter *iter)
{
    if (iter->ctx) {
        influxdb_Mount_iterData *data = (influxdb_Mount_iterData*)iter->ctx;

        if (!data) {
            iter->ctx = NULL;
            return;
        }

        JSON_Value *vals = json_array_get_wrapping_value(data->series->values);
        JSON_Value *cols = json_array_get_wrapping_value(data->series->columns);

        corto_dealloc(data->series->name);
        corto_dealloc(data->series->type);
        json_value_free(vals);
        json_value_free(cols);
        corto_ptr_free(data->result, corto_result_o);
        free(data->series);
        free(data);

        iter->ctx = NULL;
    }
}

influxdb_Mount_iterData *influxdb_Mount_iterDataNew(
    influxdb_Query_SeriesResult *series)
{
    JSON_Array *vals = NULL;
    JSON_Array *cols = NULL;
    JSON_Value *v = NULL;
    JSON_Value *c = NULL;

    JSON_Value *values = json_array_get_wrapping_value(series->values);
    JSON_Value *columns = json_array_get_wrapping_value(series->columns);

    v = json_value_deep_copy(values);
    JSON_PTR_VERIFY(v, "Iterator value data copy.");
    vals = json_value_get_array(v);
    if (!vals) {
        corto_seterr("Failed to create iterator values.");
        goto error;
    }

    c = json_value_deep_copy(values);
    JSON_PTR_VERIFY(v, "Iterator columns data copy.");
    cols = json_value_get_array(c);
    if (!columns) {
        corto_seterr("Failed to create iterator columns.");
        goto error;
    }

    influxdb_Mount_iterData *data =
        (influxdb_Mount_iterData*)calloc(1, sizeof(influxdb_Mount_iterData));

    influxdb_Query_SeriesResult *seriesCopy =
        (influxdb_Query_SeriesResult*)malloc(sizeof(influxdb_Query_SeriesResult));
    corto_result *result = corto_ptr_new(corto_result_o);

    seriesCopy->values = vals;
    seriesCopy->columns = cols;

    data->pos = 0;
    data->result = result;
    data->series = series;

    return data;
error:
    JSON_SAFE_FREE(c);
    JSON_SAFE_FREE(v);
    return NULL;
}
