/* This is a managed file. Do not delete this comment. */

#include <include/test.h>

void test_Tool_measurements(
    test_Tool this)
{
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    if (CreateManualMount(weather))
    {
        goto error;
    }

    corto_ll list = corto_ll_new();

    test_assert(influxdb_Mount_show_measurements(influxdbMount, "kentucky", list) == 0);

    corto_ll_free(list);
    corto_release(weather);
    corto_release(influxdbMount);


    return;
error:
    corto_error("%s", corto_lasterr());
    return;
}
