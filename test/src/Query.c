/* This is a managed file. Do not delete this comment. */

#include <include/test.h>
/* $header() */
/* $end */
void test_Query_resolve(
    test_Query this)
{
    // corto_verbosity(CORTO_TRACE);
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    if (CreateManualMount(weather))
    {
        goto error;
    }

    if (CreateWeatherObjects(weather) != 0) {
        goto error;
    }

    corto_trace("Test corto_resolve(/weather/texas/houston/houston)");
    test_Weather houston = (test_Weather)corto_resolve(
        weather, "texas/houston/weather");
    test_assert(houston != NULL);
    corto_release(weather);
    corto_release(influxdbMount);
    return;
error:
    corto_error("%s", corto_lasterr());
    return;
}

void test_Query_select(
    test_Query this)
{
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    test_assert(CreateManualMount(weather) == 0);

    test_assert(CreateWeatherObjects(weather) == 0);

    corto_iter it;
    test_assert(corto_select("*").from("/weather").iter(&it) == 0);

    int weatherCnt = 0;
    int stateCnt = 0;
    while(corto_iter_hasNext(&it) == 1)
    {
        corto_result *r = (corto_result*)corto_iter_next(&it);
        corto_string nodePath = corto_asprintf("/weather/%s", r->id);
        corto_object node = corto_lookup(root_o, nodePath);
        test_assert(node != NULL);
        if (corto_instanceof((corto_type)test_Weather_o, node) == true){
            weatherCnt++;
        }

        if (corto_instanceof((corto_type)test_State_o, node) == true){
            stateCnt++;
        }

        corto_dealloc(nodePath);
        corto_release(node);
    }

    corto_trace("Received [%d] weather nodes", weatherCnt);
    corto_trace("Received [%d] state nodes", stateCnt);
    test_assert(stateCnt == 3);
    test_assert(weatherCnt == 0);
    corto_release(weather);
    corto_release(influxdbMount);
    return;
}

void test_Query_selectAll(
    test_Query this)
{
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    test_assert(CreateManualMount(weather) == 0);

    test_assert(CreateWeatherObjects(weather) == 0);

    corto_iter it;
    test_assert(corto_select("//").from("/weather").iter(&it) == 0);

    int cnt = 0;
    while(corto_iter_hasNext(&it) == 1)
    {
        corto_result *r = (corto_result*)corto_iter_next(&it);
        corto_trace("Received: Parent[%s] ID[%s]", r->parent, r->id);
        corto_string nodePath = corto_asprintf("/weather/%s/%s", r->parent, r->id);
        corto_object node = corto_lookup(root_o, nodePath);
        test_assert(node != NULL);

        corto_time now;
        corto_time epoch;
        corto_timeGet(&now);

        corto_timeGet(&now);
        if (corto_instanceof((corto_type)test_Weather_o, node) == true){
            test_Weather weather = (test_Weather)node;
            test_assert(corto_time_compare(weather->timestamp, epoch) == 1);
            test_assert(corto_time_compare(weather->timestamp, now) == -1);

            cnt++;
        }

        corto_dealloc(nodePath);
        corto_release(node);
    }

    corto_trace("Received [%d] nodes", cnt);
    test_assert(cnt == 4);
    corto_release(weather);
    corto_release(influxdbMount);
    return;
}

void test_Query_selectChild(
    test_Query this)
{
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    test_assert(CreateManualMount(weather) == 0);

    test_assert(CreateWeatherObjects(weather) == 0);

    corto_iter it;
    test_assert(corto_select("*").from("/weather/kentucky").iter(&it) == 0);

    int cnt = 0;
    while(corto_iter_hasNext(&it) == 1)
    {
        corto_result *r = (corto_result*)corto_iter_next(&it);
        corto_string nodePath = corto_asprintf("/weather/kentucky/%s", r->id);
        corto_object node = corto_lookup(root_o, nodePath);
        test_assert(node != NULL);
        if (corto_instanceof((corto_type)test_City_o, node) == true){
            cnt++;
        }

        corto_dealloc(nodePath);
        corto_release(node);
    }

    corto_trace("Received [%d] nodes", cnt);
    test_assert(cnt == 2);
    corto_release(weather);
    corto_release(influxdbMount);
    return;
}

void test_Query_limit(
    test_Query this)
{
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    test_assert(CreateManualMount(weather) == 0);

    test_assert(CreateWeatherObjects(weather) == 0);

    int limit = 1;
    corto_iter it;
    test_assert(corto_select("*").from("/weather").limit(0, limit).iter(&it) == 0);

    int weatherCnt = 0;
    int stateCnt = 0;
    while(corto_iter_hasNext(&it) == 1)
    {
        corto_result *r = (corto_result*)corto_iter_next(&it);
        corto_string nodePath = corto_asprintf("/weather/%s", r->id);
        corto_object node = corto_lookup(root_o, nodePath);
        test_assert(node != NULL);
        if (corto_instanceof((corto_type)test_Weather_o, node) == true){
            weatherCnt++;
        }

        if (corto_instanceof((corto_type)test_State_o, node) == true){
            stateCnt++;
        }

        corto_dealloc(nodePath);
        corto_release(node);
    }

    corto_trace("Received [%d] weather nodes", weatherCnt);
    corto_trace("Received [%d] state nodes", stateCnt);
    test_assert(stateCnt == limit);
    test_assert(weatherCnt == 0);
    corto_release(weather);
    corto_release(influxdbMount);
}

void test_Query_offset(
    test_Query this)
{
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    test_assert(CreateManualMount(weather) == 0);

    test_assert(CreateWeatherObjects(weather) == 0);

    int limit = 1;
    int offset = 1;
    corto_iter it;
    test_assert(corto_select("*").from("/weather").limit(offset, limit).iter(&it) == 0);

    int weatherCnt = 0;
    int stateCnt = 0;
    while(corto_iter_hasNext(&it) == 1)
    {
        corto_result *r = (corto_result*)corto_iter_next(&it);
        corto_info("%s", r->id);
        test_assert(strcmp(r->id, "texas") == 0);
        corto_string nodePath = corto_asprintf("/weather/%s", r->id);
        corto_object node = corto_lookup(root_o, nodePath);
        test_assert(node != NULL);
        if (corto_instanceof((corto_type)test_Weather_o, node) == true){
            weatherCnt++;
        }

        if (corto_instanceof((corto_type)test_State_o, node) == true){
            stateCnt++;
        }

        corto_dealloc(nodePath);
        corto_release(node);
    }

    corto_trace("Received [%d] weather nodes", weatherCnt);
    corto_trace("Received [%d] state nodes", stateCnt);
    test_assert(stateCnt == limit);
    test_assert(weatherCnt == 0);
    corto_release(weather);
    corto_release(influxdbMount);
}
