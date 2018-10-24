#include <driver/mnt/influxdb/query_response.h>
#include <driver/mnt/influxdb/query_response_iter.h>

void *influxdb_mount_iterDataNext(corto_iter *iter)
{
    influxdb_mount_iterData *data = (influxdb_mount_iterData*)iter->ctx;

    JSON_Array *values = json_array_get_array(data->series->values, data->pos);
    if (!values) {
        corto_throw("Resolved invalid JSON value at index [%d]", data->pos);
        goto error;
    }

    if (influxdb_mount_response_result_update(
        data->series, values, data->result)) {
        corto_throw("Failed to update result for historical index [%d]",
            data->pos);
        goto error;
    }

    data->result->name = NULL;
    data->pos++;

    return &data->result;
error:
    return NULL;
}

bool influxdb_mount_iterDataHasNext(corto_iter *iter)
{
    influxdb_mount_iterData *data = (influxdb_mount_iterData*)iter->ctx;

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

void influxdb_mount_iterDataRelease(corto_iter *iter)
{
    if (iter->ctx) {
        influxdb_mount_iterData *data = (influxdb_mount_iterData*)iter->ctx;

        if (!data) {
            iter->ctx = NULL;
            return;
        }

        corto_ptr_free(data->result, corto_record_o);
        influxdb_mount_series_free(data->series);
        free(data);

        iter->ctx = NULL;
    }
}

influxdb_mount_iterData *influxdb_mount_iterDataNew(
    influxdb_Query_SeriesResult *series)
{
    influxdb_mount_iterData *data =
        (influxdb_mount_iterData*)calloc(1, sizeof(influxdb_mount_iterData));

    influxdb_Query_SeriesResult *seriesCopy = NULL;
    if (influxdb_mount_series_deepCopy(series, seriesCopy)) {
        goto error;
    }

    corto_record *result = corto_ptr_new(corto_record_o);
    data->pos = 0;
    data->result = result;
    data->series = seriesCopy;

    return data;
error:
    return NULL;
}
