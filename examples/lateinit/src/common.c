#include <include/common.h>

corto_id INFLUX_MOUNT_ID = "influx_weather";
corto_string INFLUX_DB_HOST = "http://localhost:8086";
corto_string INFLUX_DB_NAME = "unit_test";
lateinit_Weather weatherH = NULL;
lateinit_Weather weatherL = NULL;
lateinit_Weather weatherN = NULL;
lateinit_Weather weatherS = NULL;

influxdb_Mount influxdbMount = NULL;
corto_object weather = NULL;

int CreateHistoricalManualMount(void)
{
    if (weather == NULL) {
        corto_voidCreateChild_auto(root_o, data);
        weather = corto_createChild(data, "weather", corto_void_o);
    }
    influxdbMount = influxdb_MountDeclareChild(root_o, INFLUX_MOUNT_ID);

    corto_string fromPath = corto_fullpath(NULL, weather);
    corto_query* query = corto_queryCreate(
        "//", fromPath, NULL, NULL, NULL, 0, 0, NULL, NULL);
    corto_queuePolicy *queue = corto_queuePolicyCreate(25);
    int policyMask = 0;
    policyMask |= CORTO_MOUNT_HISTORY_BATCH_NOTIFY;
    corto_mountPolicy *policy = corto_mountPolicyCreate(
        CORTO_REMOTE_OWNER,
        policyMask,
        20.0,
        queue,
        0,
        false);

    influxdb_RetentionPolicy rp = influxdb_RetentionPolicyCreate(
        "unit_test",
        INFLUX_DB_HOST,
        INFLUX_DB_NAME,
        "1h0m0s",
        1,
        NULL);

    corto_info("Call Define.");
    if (influxdb_MountDefine(
        influxdbMount,
        query,
        "text/json",
        policy,
        INFLUX_DB_HOST,     /* hostname */
        INFLUX_DB_NAME,    /* database name */
        rp,                /* retention policy */
        NULL,              /* username */
        NULL))             /* password */
    {
        corto_error("Failed to define weather mount");
        goto error;
    }

    corto_info("Weather Historian INITIALIZED - InfluxDB syncing.");

    corto_release(query);
    corto_release(queue);
    corto_release(policy);

    return 0;
error:
    return -1;
}

int DumbRandom(int min, int max){
   return min + rand() / (RAND_MAX / (max - min + 1) + 1);
}

void QuickWeatherUpdate(lateinit_Weather weather, int temp, int humidity, int uv) {
    corto_time now;
    corto_timeGet(&now);
    if (lateinit_WeatherUpdate(
        weather,
        DumbRandom(temp-20, temp+20),
        DumbRandom(humidity-10, humidity+10),
        DumbRandom(uv-3, 12),
        &now)) {
        corto_error("Weather update failed. Error: %s", corto_lasterr());
    }

}

int16_t UpdateWeather(void)
{
    corto_time now;
    corto_timeGet(&now);

    if (weatherH == NULL) {
        if (weather == NULL) {
            if (weather == NULL) {
                corto_voidCreateChild_auto(root_o, data);
                weather = corto_createChild(data, "weather", corto_void_o);
            }
        }
        lateinit_State kentucky = lateinit_StateCreateChild(weather, "kentucky", "south", true);
        lateinit_State texas =  lateinit_StateCreateChild(weather, "texas", "southwest", false);
        lateinit_State california =  lateinit_StateCreateChild(weather, "california", "west", false);
        lateinit_City houston = lateinit_CityCreateChild(texas, "houston", 1837, 8538000);
        lateinit_City lexington = lateinit_CityCreateChild(kentucky, "lexington", 1782, 318449);
        lateinit_City nicholasville = lateinit_CityCreateChild(kentucky, "nicholasville", 1798, 30006);
        lateinit_City sanDiego = lateinit_CityCreateChild(california, "San Diego", 1769, 1407000);

        weatherH = lateinit_WeatherCreateChild(houston, "weather", 82, 45.5, 8, &now);
        weatherL = lateinit_WeatherCreateChild(lexington, "weather", 95, 78.8, 6, &now);
        weatherN = lateinit_WeatherCreateChild(nicholasville, "weather", 45, 47, 4, &now);
        weatherS = lateinit_WeatherCreateChild(sanDiego, "weather", 50, 48, 6, &now);

        corto_release(kentucky);
        corto_release(texas);
        corto_release(california);
        corto_release(houston);
        corto_release(lexington);
        corto_release(nicholasville);
        corto_release(sanDiego);

        sleep(2);
    }

    int cnt = 0;
    while (cnt < 2) {
        cnt++;
        QuickWeatherUpdate(weatherL, 82, 45, 8);
        QuickWeatherUpdate(weatherH, 95, 78, 6);
        QuickWeatherUpdate(weatherN, 45, 47, 4);
        QuickWeatherUpdate(weatherS, 50, 48, 6);
        usleep(1000);
    }

    return 0;
}
