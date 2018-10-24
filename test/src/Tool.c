/* This is a managed file. Do not delete this comment. */

#include <include/test.h>

/* $header() */
bool influxdb_mount_TestMeasurementQuery(corto_string pattern, int results)
{
    corto_ll list = corto_ll_new();

    if (influxdb_mount_show_measurements(influxdbMount, pattern, list)) {
        return false;
    }

    int size = corto_ll_count(list);
    if (size != results) {
        corto_error("Results [%d] != List Size [%d]", results, size);
        return false;
    }

    test_assert(influxdb_mount_show_measurements_free(list) == 0);

    corto_ll_free(list);

    return true;
}
/* $end */

void test_Tool_measurements(
    test_Tool this)
{
    corto_object weather = corto_void__create(root_o, "weather");

    if (create_manual_mount(weather))
    {
        goto error;
    }

    if (create_weather_objects(weather))
    {
        goto error;
    }

    test_assert(influxdb_mount_TestMeasurementQuery("kentucky", 2) == true);
    test_assert(influxdb_mount_TestMeasurementQuery("kentucky/lexington", 1) == true);
    test_assert(influxdb_mount_TestMeasurementQuery(".", 3) == true);
    test_assert(influxdb_mount_TestMeasurementQuery("Ohio", 0) == true);
    test_assert(influxdb_mount_TestMeasurementQuery("San Diego", 0) == true);

    corto_release(weather);
    corto_release(influxdbMount);

    return;
error:
    return;
}
