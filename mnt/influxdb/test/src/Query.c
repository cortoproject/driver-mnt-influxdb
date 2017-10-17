/* This is a managed file. Do not delete this comment. */

#include <include/test.h>

/* $header() */

/* $end */

void test_Query_resolve(
    test_Query this)
{
    corto_verbosity(CORTO_TRACE);

    corto_object weather = corto_voidCreateChild(root_o, "weather");

    if (CreateManualMount(weather))
    {
        goto error;
    }

    if (test_write_weather(weather) != 0) {
        goto error;
    }

    corto_trace("Test corto_resolve(/weather/houston)");
    test_Weather houston = (test_Weather)corto_resolve(weather, "houston");
    test_assert(houston != NULL);
    if (houston != NULL) {
        corto_info("Resolved houston: Temperature [%d] Humidity [%f]!",
            houston->temperature, houston->humidity);
        ///TODO Verify data.
        corto_release(houston);
    }

    corto_release(weather);
    corto_release(influxdbMount);


    return;
error:
    corto_error("%s", corto_lasterr());
    return;
}
