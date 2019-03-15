#include <include/common.h>

corto_id INFLUX_MOUNT_ID = "influx_weather";
corto_string INFLUX_DB_HOST = "localhost";
corto_string INFLUX_DB_NAME = "unit_test";
corto_int16 INFLUX_DB_PORT = 8086;

influxdb_mount influxdbMount = NULL;

int create_manual_mount(corto_object mountPoint)
{
    influxdb_mount mount = corto_declare(
        root_o,
        INFLUX_MOUNT_ID,
        influxdb_mount_o);
    corto_query query = {
        .select = "//",
        .from = corto_fullpath(NULL, mountPoint)
    };

    corto_mountCallbackMask callbacks = 0;
    callbacks |= CORTO_MOUNT_NOTIFY;
    callbacks |= CORTO_MOUNT_QUERY;
    callbacks |= CORTO_MOUNT_HISTORY_BATCH_NOTIFY;

    mount->super.ownership = CORTO_REMOTE_SOURCE;
    mount->super.callbacks = callbacks;
    mount->super.super.query = query;
    mount->super.sample_rate = 20.0;
    mount->super.queue_max = 25;

    influxdb_UdpConn udp = influxdb_UdpConn__create(
        NULL,
        NULL,
        "localhost",
        "8089",
        0);

    influxdb_RetentionPolicy rp = influxdb_RetentionPolicy__create(
        NULL,
        NULL,
        "unit_test",
        INFLUX_DB_HOST,
        INFLUX_DB_PORT,
        INFLUX_DB_NAME,
        "1h0m0s",
        1,
        NULL);
    test_assert(rp != NULL);

    influxdb_mount__assign(
        mount,
        "localhost",  /* hostname */
        8086,
        udp,
        "udp_test",   /* database name */
        rp,         /* Retention Policy */
        NULL,         /* username */
        NULL);        /* password */

    return corto_define(mount);
}

int create_historical_manual_mount(corto_object mountPoint)
{
    influxdb_mount mount = corto_declare(root_o, INFLUX_MOUNT_ID, influxdb_mount_o);
    corto_query query = {
        .select = "//",
        .from = corto_fullpath(NULL, mountPoint)
    };

    corto_mountCallbackMask callbacks = 0;
    callbacks |= CORTO_MOUNT_HISTORY_QUERY;

    mount->super.ownership = CORTO_REMOTE_SOURCE;
    mount->super.callbacks = callbacks;
    mount->super.super.query = query;
    mount->super.sample_rate = 20.0;
    mount->super.queue_max = 10;

    influxdb_UdpConn udp = influxdb_UdpConn__create(
        NULL,
        NULL,
        "localhost",
        "8089",
        0);

    influxdb_RetentionPolicy rp = influxdb_RetentionPolicy__create(
        NULL,
        NULL,
        "unit_test",
        INFLUX_DB_HOST,
        INFLUX_DB_PORT,
        INFLUX_DB_NAME,
        "1h0m0s",
        1,
        NULL);
    test_assert(rp != NULL);

    influxdb_mount__assign(
        mount,
        "localhost",  /* hostname */
        8086,
        udp,
        "udp_test",   /* database name */
        rp,         /* Retention Policy */
        NULL,         /* username */
        NULL);        /* password */

    return corto_define(mount);
}

int DumbRandom(int min, int max){
   return min + rand() / (RAND_MAX / (max - min + 1) + 1);
}

void QuickWeatherUpdate(test_Weather weather, int temp, int humidity, int uv) {
    corto_time now;
    corto_time_get(&now);
    if (test_Weather__update(
        weather,
        DumbRandom(temp-20, temp+20),
        DumbRandom(humidity-10, humidity+10),
        DumbRandom(uv-3, 12),
        &now)) {
        corto_error("Weather update failed.");
    }

}

int16_t create_weather_objects(corto_object weather)
{
    corto_time now;
    corto_time_get(&now);

    test_State kentucky = test_State__create(weather, "kentucky", "south", true);
    test_State texas =  test_State__create(weather, "texas", "southwest", false);
    test_State california =  test_State__create(weather, "california", "west", false);
    test_City houston = test_City__create(texas, "houston", 1837, 8538000);
    test_City lexington = test_City__create(kentucky, "lexington", 1782, 318449);
    test_City nicholasville = test_City__create(kentucky, "nicholasville", 1798, 30006);
    test_City sanDiego = test_City__create(california, "San Diego", 1769, 1407000);

    test_Weather weatherH = test_Weather__create(houston, "weather", 82, 45.5, 8, &now);
    test_Weather weatherL = test_Weather__create(lexington, "weather", 95, 78.8, 6, &now);
    test_Weather weatherN = test_Weather__create(nicholasville, "weather", 45, 47, 4, &now);
    test_Weather weatherS = test_Weather__create(sanDiego, "weather", 50, 48, 6, &now);

    sleep(2);

    int cnt = 0;
    while (cnt < 2) {
        cnt++;
        QuickWeatherUpdate(weatherL, 82, 45, 8);
        QuickWeatherUpdate(weatherH, 95, 78, 6);
        QuickWeatherUpdate(weatherN, 45, 47, 4);
        QuickWeatherUpdate(weatherS, 50, 48, 6);
        usleep(1000);
    }

    corto_release(kentucky);
    corto_release(texas);
    corto_release(california);
    corto_release(houston);
    corto_release(lexington);
    corto_release(nicholasville);
    corto_release(sanDiego);

    return 0;
}
