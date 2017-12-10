/* This is a managed file. Do not delete this comment. */

#include <include/test.h>
/* $header() */
/* $end */
void test_RetentionPolicy_tc_1_create(
    test_RetentionPolicy this)
{
    influxdb_RetentionPolicy rp = influxdb_RetentionPolicyCreate(
        "test_rp",
        INFLUX_DB_HOST,
        INFLUX_DB_NAME,
        "2h0m0s",
        1,
        NULL);

    corto_ll ret = corto_ll_new();
    influxdb_Mount_show_retentionPolicies(INFLUX_DB_HOST, INFLUX_DB_NAME, ret);

    bool foundTest = false;

    int i;
    for (i = 0; i < corto_ll_count(ret); i++) {
        influxdb_Query_RetentionPolicyResult *rp =
            (influxdb_Query_RetentionPolicyResult*)corto_ll_get(ret, i);
        if (strcmp(rp->name, "test_rp") == 0) {
            foundTest = true;
            test_assert(strcmp(rp->duration, "2h0m0s") == 0); //Influx converts
        }

    }

    test_assert(foundTest == true);
    influxdb_Mount_show_retentionPolicies_free(ret);
    corto_ll_free(ret);
    test_assert(rp != NULL);
}

void test_RetentionPolicy_tc_2_duplicate(
    test_RetentionPolicy this)
{
    influxdb_RetentionPolicy rp = influxdb_RetentionPolicyCreate(
        "test_rp",
        INFLUX_DB_HOST,
        INFLUX_DB_NAME,
        "2h0m0s",
        1,
        NULL);

    corto_ll ret = corto_ll_new();
    influxdb_Mount_show_retentionPolicies(INFLUX_DB_HOST, INFLUX_DB_NAME, ret);

    bool foundTest = false;

    int i;
    for (i = 0; i < corto_ll_count(ret); i++) {
        influxdb_Query_RetentionPolicyResult *rp =
            (influxdb_Query_RetentionPolicyResult*)corto_ll_get(ret, i);
        if (strcmp(rp->name, "test_rp") == 0) {
            foundTest = true;
            test_assert(strcmp(rp->duration, "2h0m0s") == 0); //Influx converts
        }
    }

    test_assert(foundTest == true);
    influxdb_Mount_show_retentionPolicies_free(ret);
    corto_ll_free(ret);
    test_assert(rp != NULL);
}

void test_RetentionPolicy_tc_2_duplicateConflict(
    test_RetentionPolicy this)
{
    influxdb_RetentionPolicy rp = influxdb_RetentionPolicyCreate(
        "test_rp",
        INFLUX_DB_HOST,
        INFLUX_DB_NAME,
        "4h0m0s",
        1,
        NULL);

    corto_ll ret = corto_ll_new();
    influxdb_Mount_show_retentionPolicies(INFLUX_DB_HOST, INFLUX_DB_NAME, ret);

    bool foundTest = false;

    int i;
    for (i = 0; i < corto_ll_count(ret); i++) {
        influxdb_Query_RetentionPolicyResult *rp =
            (influxdb_Query_RetentionPolicyResult*)corto_ll_get(ret, i);
        if (strcmp(rp->name, "test_rp") == 0) {
            foundTest = true;
            test_assert(strcmp(rp->duration, "2h0m0s") == 0); //Influx converts
        }
    }

    test_assert(foundTest == true);
    influxdb_Mount_show_retentionPolicies_free(ret);
    corto_ll_free(ret);
    test_assert(rp == NULL);
}
