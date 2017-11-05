/* This is a managed file. Do not delete this comment. */

#include <include/test.h>
/* $header() */
/* $end */
void test_Query_resolve(
    test_Query this)
{
    // corto_verbosity(CORTO_TRACE);
    printf("\n\n\n\nRESOLVE\n\n");
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
        corto_release(houston);
    }

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
    printf("\n\n\n\nSELECT\n\n");
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    // test_assert(CreateManualMount(weather) == 0);

    // test_assert(test_write_weather(weather) == 0);

    corto_iter it;
    test_assert(corto_select("*").from("/weather").iter(&it) == 0);

    int cnt = 0;
    while(corto_iter_hasNext(&it) == 1)
    {
        corto_result *r = (corto_result*)corto_iter_next(&it);
        corto_string nodePath = corto_asprintf("/weather/%s", r->id);
        test_Weather node = (test_Weather)corto_lookup(root_o, nodePath);

        test_assert(node != NULL);

        corto_info("Select Returned Type [%s] Expected [%s]",
            corto_fullpath(NULL, corto_typeof(node)),
            corto_fullpath(NULL, (corto_type)test_Weather_o));
        test_assert(corto_instanceof((corto_type)test_Weather_o, node) == true);

        corto_info("Select Returned: [%s] Temperature [%d]", r->id, node->temperature);

        corto_dealloc(nodePath);
        corto_release(node);
        cnt++;
    }

    corto_info("Received [%d] nodes", cnt);
    test_assert(cnt == 2);
    corto_release(weather);
    corto_release(influxdbMount);
    return;
}

void test_Query_selectAll(
    test_Query this)
{
    printf("\n\n\n\nSELECT ALL\n\n");
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    test_assert(CreateManualMount(weather) == 0);

    test_assert(test_write_weather(weather) == 0);

    corto_iter it;
    test_assert(corto_select("//").from("/weather").iter(&it) == 0);

    int cnt = 0;
    while(corto_iter_hasNext(&it) == 1)
    {
        corto_result *r = (corto_result*)corto_iter_next(&it);
        corto_info("\n\n\n\n\nSelect child ID [%s]", r->id);
        corto_string nodePath = corto_asprintf("/weather/%s", r->id);
        test_Weather node = (test_Weather)corto_lookup(root_o, nodePath);

        test_assert(node != NULL);

        corto_info("Select Returned Type [%s] Expected [%s]",
            corto_fullpath(NULL, corto_typeof(node)),
            corto_fullpath(NULL, (corto_type)test_Weather_o));
        test_assert(corto_instanceof((corto_type)test_Weather_o, node) == true);

        corto_info("Select Returned: [%s] Temperature [%d]", r->id, node->temperature);

        corto_dealloc(nodePath);
        corto_release(node);
        cnt++;
    }

    corto_info("Received [%d] nodes", cnt);
    test_assert(cnt == 3);
    corto_release(weather);
    corto_release(influxdbMount);
    return;
}

void test_Query_selectChild(
    test_Query this)
{
    printf("\n\n\n\nSELECT CHILD\n\n");
    corto_object weather = corto_voidCreateChild(root_o, "weather");

    test_assert(CreateManualMount(weather) == 0);

    test_assert(test_write_weather(weather) == 0);

    corto_iter it;
    test_assert(corto_select("*").from("/weather/kentucky").iter(&it) == 0);

    int cnt = 0;
    while(corto_iter_hasNext(&it) == 1)
    {
        corto_result *r = (corto_result*)corto_iter_next(&it);

        corto_string nodePath = corto_asprintf("/weather/%s", r->id);
        test_Weather node = (test_Weather)corto_lookup(root_o, nodePath);

        test_assert(node != NULL);

        corto_info("Select Returned Type [%s] Expected [%s]",
            corto_fullpath(NULL, corto_typeof(node)),
            corto_fullpath(NULL, (corto_type)test_Weather_o));
        test_assert(corto_instanceof((corto_type)test_Weather_o, node) == true);

        corto_info("Select Returned: [%s] Temperature [%d]", r->id, node->temperature);

        corto_dealloc(nodePath);
        corto_release(node);
        cnt++;
    }

    corto_info("Received [%d] nodes", cnt);
    test_assert(cnt == 2);
    corto_release(weather);
    corto_release(influxdbMount);
    return;
}
