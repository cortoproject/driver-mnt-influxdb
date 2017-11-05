/* This is a managed file. Do not delete this comment. */

#include <include/test.h>

void test_HistoricalQuery_select(
    test_HistoricalQuery this)
{
    // corto_verbosity(CORTO_TRACE);

    corto_object weather = corto_voidCreateChild(root_o, "weather");

    if (CreateManualMount(weather))
    {
        goto error;
    }

    if (test_write_weather(weather) != 0) {
        goto error;
    }

    corto_iter it;

    /* Select children from object (o). */
    corto_int16 ret = corto_select("*").from("/weather").iter(&it);

    if (ret != 0) {
        goto error;
    }

    int cnt = 0;
    while(corto_iter_hasNext(&it) != 0)
    {
        cnt++;
        corto_result *result = (corto_result*) corto_iter_next(&it);

        corto_info("SELECT: ID [%s] Name [%s]", result->id, result->name);
    }

    test_assert(cnt > 0);

    corto_release(weather);
    corto_release(influxdbMount);


    return;
error:
    corto_error("%s", corto_lasterr());
    return;
}
