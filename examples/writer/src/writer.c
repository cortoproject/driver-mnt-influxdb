#include "include/writer.h"

corto_id INFLUX_MOUNT_ID = "influx_manual";
corto_string INFLUX_DB_HOST = "http://localhost";
uint16_t INFLUX_DB_PORT = 8086;
corto_string INFLUX_DB_NAME = "writer_demo";
influxdb_Mount influxdbMount = NULL;
int WriteConfig(float temperature, corto_object t1, corto_object t2)
{
    corto_float32Update(t1, cos(temperature) * 100);
    corto_float32Update(t2, sin(temperature) * 100);

    return 0;
}

int CreateManualMount(corto_object mountPoint)
{
    influxdbMount = influxdb_MountDeclareChild(root_o, INFLUX_MOUNT_ID);
    corto_query query = {
        .select = "//",
        .from = corto_fullpath(NULL, mountPoint)
    };
    corto_mountPolicy policy = {
        .ownership = CORTO_LOCAL_OWNER,
        .mask = CORTO_MOUNT_NOTIFY,
        .sampleRate = 2.0,
        .queue.max = 25
    };
    if (influxdb_MountDefine(
        influxdbMount,
        &query,
        "text/json",
        &policy,
        INFLUX_DB_HOST,    /* hostname */
        INFLUX_DB_PORT,
        0,
        false,
        INFLUX_DB_NAME,    /* database name */
        NULL,              /* retention policy */
        NULL,              /* username */
        NULL))             /* password */
    {
        corto_error("Failed to define manual mount");
        return -1;
    }

    return 0;
}

int writerMain(int argc, char *argv[])
{
    if (corto_use("config.json", 0, NULL)) {
        goto error;
    }

    corto_voidCreateChild_auto(root_o, temperature);
    corto_voidCreateChild_auto(temperature, config);
    corto_object manual = corto_voidCreateChild(temperature, "manual");
    if (CreateManualMount(manual))
    {
        goto error;
    }

    corto_float32DeclareChild_auto(config, temp1);
    corto_float32DeclareChild_auto(config, temp2);
    corto_trace("Created Temperature 1 - [%s]", corto_fullpath(NULL, temp1));
    corto_trace("Created Temperature 2 - [%s]", corto_fullpath(NULL, temp2));
    corto_float32DeclareChild_auto(manual, temp3);
    corto_float32DeclareChild_auto(manual, temp4);
    corto_string *humidity = corto_stringCreateChild(manual, "humidity", "MISERABLE");
    corto_trace("Created Temperature 3 - [%s]", corto_fullpath(NULL, temp3));
    corto_trace("Created Temperature 4 - [%s]", corto_fullpath(NULL, temp4));
    corto_float32 t = 0;
    while (1) {
        t += 0.01;
        if (WriteConfig(t, temp1, temp2)) {
            corto_error("Failed to write 1 and 2");
            goto error;
        }

        if (WriteConfig(t, temp3, temp4)) {
            goto error;
        }

        corto_sleep(0, 200000000); /* Update 5 times a second */
    }

    corto_release(humidity);
    corto_release(influxdbMount);
    return 0;
error:
    corto_error("%s", corto_lasterr());
    return -1;
}

int cortomain(int argc, char *argv[]) {

    /* Insert implementation */

    return 0;
}
