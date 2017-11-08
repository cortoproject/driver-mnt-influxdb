/* This is a managed file. Do not delete this comment. */

#include <include/test.h>
/* $header() */
bool influxdb_Mount_TestRetntionPolicyQuery(corto_string pattern, int results)
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

    corto_ll_free(list);
    return true;
}

/* $end */

void test_RetentionPolicy_tc_1_create(
    test_RetentionPolicy this)
{
    influxdb_RetentionPolicy rp = influxdb_RetentionPolicyCreate(
        "test_rp",
        INFLUX_DB_HOST,
        INFLUX_DB_NAME,
        "120m",
        1,
        NULL);

    corto_object weather = corto_voidCreateChild(root_o, "weather");

    test_assert(CreateManualMount(weather) == 0);

    corto_ll results = corto_ll_new();
    influxdb_Mount_show_retentionPolicies(influxdbMount, results);

    int i;
    bool foundTest = false;
    for (i = 0; i < corto_ll_size(results); i++) {
        influxdb_Query_RetentionPolicyResult *rp =
            (influxdb_Query_RetentionPolicyResult*)corto_ll_get(results, i);
        if (strcmp(rp->name, "test_rp") == 0) {
            foundTest = true;
            test_assert(strcmp(rp->duration, "2h0m0s") == 0); //Influx converts
        }
    }

    test_assert(foundTest == true);

    influxdb_Mount_show_retentionPolicies_free(results);
    corto_ll_free(results);

    test_assert(rp != NULL);
}

void test_RetentionPolicy_tc_2_create(
    test_RetentionPolicy this)
{
    /* Insert implementation */
}
