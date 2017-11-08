/* This is a managed file. Do not delete this comment. */

#include <include/test.h>

/* $header() */
bool influxdb_Mount_TestMeasurementQuery(corto_string pattern, int results)
{
    corto_ll list = corto_ll_new();

    if (influxdb_Mount_show_measurements(influxdbMount, pattern, list)) {
        corto_error("Measurement query failed. Error: %s", corto_lasterr());
        return false;
    }

    int size = corto_ll_size(list);
    if (size != results) {
        corto_error("Results [%d] != List Size [%d]", results, size);
        return false;
    }

    test_assert(influxdb_Mount_show_measurements_free(list) == 0);
    
    corto_ll_free(list);

    return true;
}
/* $end */

void test_Tool_measurements(
    test_Tool this)
{
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    if (CreateManualMount(weather))
    {
        goto error;
    }

    if (CreateWeatherObjects(weather))
    {
        goto error;
    }

    test_assert(influxdb_Mount_TestMeasurementQuery("kentucky", 2) == true);
    test_assert(influxdb_Mount_TestMeasurementQuery("kentucky/lexington", 1) == true);
    test_assert(influxdb_Mount_TestMeasurementQuery(".", 3) == true);
    test_assert(influxdb_Mount_TestMeasurementQuery("Ohio", 0) == true);
    test_assert(influxdb_Mount_TestMeasurementQuery("San Diego", 0) == true);

    corto_release(weather);
    corto_release(influxdbMount);

    return;
error:
    corto_error("%s", corto_lasterr());
    return;
}
