/* This is a managed file. Do not delete this comment. */

#include <include/test.h>
void test_HistoricalQuery_select(
    test_HistoricalQuery this)
{
    // corto_verbosity(CORTO_TRACE);

    corto_object weather = corto_void__create(root_o, "weather");

    if (create_historical_manual_mount(weather))
    {
        goto error;
    }

    if (create_weather_objects(weather) != 0) {
        goto error;
    }

    corto_iter it;
    corto_int16 ret = corto_select("//")
        .from("/weather").fromNow().limit(10).iter(&it);
    if (ret != 0) {
        goto error;
    }

    int cnt = 0;
    while(corto_iter_hasNext(&it) != 0)
    {
        corto_record *result = (corto_record*) corto_iter_next(&it);
        if (strcmp(result->type, corto_fullpath(NULL, (corto_type)test_Weather_o)) == 0) {
            corto_info("Historical Query: Parent [%s] ID [%s]",
                result->parent, result->id);
            // corto_sampleIterForeach(result->history, sample) {
            //     corto_string value = (corto_string)sample.value;
            //     corto_info("[%s]", value);
            // }

            while (corto_iter_hasNext(&result->history)) {
                cnt++;
                corto_record *r = corto_iter_next(&result->history);
                corto_info("got '%s' with type '%s' and value %s",
                    r->id,
                    r->type,
                    corto_record_get_text(r));
            }

        }

    }

    test_assert(cnt > 0);
    corto_release(weather);
    corto_release(influxdbMount);
    return;
error:
    return;
}

void test_HistoricalQuery_limit(
    test_HistoricalQuery this)
{
    /* Insert implementation */
}

void test_HistoricalQuery_offset(
    test_HistoricalQuery this)
{
    /* Insert implementation */
}

void test_HistoricalQuery_timeDuration(
    test_HistoricalQuery this)
{
    /* Insert implementation */
}

void test_HistoricalQuery_timeFrame(
    test_HistoricalQuery this)
{
    /* Insert implementation */
}
