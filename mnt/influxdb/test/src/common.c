#include <include/common.h>

corto_id INFLUX_MOUNT_ID = "influx_weather";
corto_string INFLUX_DB_HOST = "http://localhost:8086";
corto_string INFLUX_DB_NAME = "unit_test";

influxdb_Mount influxdbMount = NULL;

int CreateManualMount(corto_object mountPoint)
{
    influxdbMount = influxdb_MountDeclareChild(root_o, INFLUX_MOUNT_ID);
    corto_string fromPath = corto_fullpath(NULL, mountPoint);
    corto_query* query = corto_queryCreate("//", fromPath, NULL, NULL, NULL, 0, 0, NULL, NULL);
    corto_queuePolicy *queue = corto_queuePolicyCreate(25);
    corto_mountPolicy *policy = corto_mountPolicyCreate(CORTO_LOCAL_OWNER,
         CORTO_MOUNT_NOTIFY|CORTO_MOUNT_QUERY,
         1,
         queue,
         0,
         false);

    if (influxdb_MountDefine(influxdbMount,
        query,
        "text/json",
        policy,
        INFLUX_DB_HOST,     /* hostname */
        INFLUX_DB_NAME))    /* database name */
    {
        corto_error("Failed to define weather mount");
        goto error;
    }

    corto_release(query);
    corto_release(queue);
    corto_release(policy);

    return 0;
error:
    return -1;
}

int16_t test_write_weather(corto_object weather)
{
    corto_time now;
    corto_timeGet(&now);
    test_Weather sanDiego = test_WeatherCreateChild(
        weather, "San Diego", 82, 45.5, 8, &now
    );
    test_Weather houston = test_WeatherCreateChild(
        weather, "houston", 95, 78.8, 6, &now
    );
    corto_voidCreateChild_auto(weather, kentucky);

    test_Weather lexington = test_WeatherCreateChild(
        kentucky, "lexington", 95, 78.8, 6, &now);
    test_WeatherCreateChild(
        kentucky, "nicholasville", 50, 48, 6, &now);
    test_WeatherCreateChild(
        lexington, "campus", 95, 78.8, 6, &now);
    test_WeatherCreateChild(
        lexington, "airport", 50, 48, 6, &now);
    corto_float32 t = 0;
    t += 0.01;
    // corto_float32Update(t1, cos(temperature) * 100);
    // corto_float32Update(t2, sin(temperature) * 100);
    corto_timeGet(&now);
    if (corto_updateBegin(houston) != 0)
    {
        corto_error("corto_updateBegin for houston.");
        return -1;
    }

    houston->temperature += cos(houston->temperature + 0.1);
    houston->humidity += sin(houston->humidity + 0.1);
    houston->timestamp = now;

    if (corto_updateEnd(houston) != 0)
    {
        corto_error("corto_updateBegin for houston.");
        return -1;
    }

    if (corto_updateBegin(sanDiego) != 0)
    {
        corto_error("corto_updateBegin for houston.");
        return -1;
    }

    sanDiego->temperature += cos(sanDiego->temperature + 0.1);
    sanDiego->humidity += sin(sanDiego->humidity + 0.1);
    sanDiego->timestamp = now;

    if (corto_updateEnd(sanDiego) != 0)
    {
        corto_error("corto_updateBegin for houston.");
        return -1;
    }

    sleep(2);

    corto_release(houston);
    corto_release(sanDiego);

    return 0;
}
