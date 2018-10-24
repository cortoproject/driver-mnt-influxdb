#include "include/writer.h"

corto_id INFLUX_MOUNT_ID = "influx_manual";
corto_string INFLUX_DB_HOST = "http://localhost";
uint16_t INFLUX_DB_PORT = 8086;
corto_string INFLUX_DB_NAME = "writer_demo";
influxdb_mount mount = NULL;
int WriteConfig(float temperature, corto_object t1, corto_object t2)
{
    corto_float32__update(t1, cos(temperature) * 100);
    corto_float32__update(t2, sin(temperature) * 100);

    return 0;
}

int create_manual_mount(corto_object mountPoint)
{
    mount = corto_declare(root_o, INFLUX_MOUNT_ID, influxdb_mount_o);
    corto_query query = {
        .select = "//",
        .from = corto_fullpath(NULL, mountPoint)
    };

    corto_mountCallbackMask callbacks = 0;
    callbacks |= CORTO_MOUNT_NOTIFY;

    mount->super.ownership = CORTO_LOCAL_SOURCE;
    mount->super.callbacks = callbacks;
    mount->super.super.query = query;
    mount->super.sample_rate = 2.0;
    mount->super.queue_max = 25;


    if (influxdb_mount__assign(
        mount,
        INFLUX_DB_HOST,    /* hostname */
        INFLUX_DB_PORT,
        NULL,              /* udp */
        INFLUX_DB_NAME,    /* database name */
        NULL,              /* retention policy */
        NULL,              /* username */
        NULL))             /* password */
    {
        corto_error("Failed to define manual mount");
        return -1;
    }

    corto_define(mount);

    return 0;
}

int cortomain(int argc, char *argv[]) {
    if (corto_use("config.json", 0, NULL)) {
        goto error;
    }

    corto_object temperature = corto_create(root_o, "temperature", corto_void_o);
    corto_object config = corto_create(temperature, "config", corto_void_o);
    corto_object manual = corto_create(temperature, "manual", corto_void_o);
    if (create_manual_mount(manual))
    {
        goto error;
    }

    corto_object temp1 = corto_float32__create(config, "temp1", 0.0);
    corto_object temp2 = corto_float32__create(config, "temp2", 0.0);
    corto_object temp3 = corto_float32__create(manual, "temp3", 0.0);
    corto_object temp4 = corto_float32__create(manual, "temp4", 0.0);
    corto_string *humidity = corto_string__create(manual, "humidity", "MISERABLE");
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
    corto_release(mount);
    return 0;
error:
    return -1;
}
