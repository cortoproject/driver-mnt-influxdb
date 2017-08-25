#include "include/writer.h"

corto_id INFLUX_MOUNT_ID = "manual";
corto_string INFLUX_DB_HOST = "http://localhost:8086";
corto_string INFLUX_DB_NAME = "writer_demo";

corto_object manualMount = NULL;
influxdb_Mount influxMount = NULL;

int WriteConfig(float temperature, corto_object temp1, corto_object temp2)
{
    corto_float32Update(temp1, cos(temperature) * 100);
    corto_float32Update(temp2, sin(temperature) * 100);

    return 0;
}

int CreateManualMount(void)
{
    influxdb_Mount influxMount = influxdb_MountCreateChild(
        manualMount,        /* connect to data in temperature scope */
        INFLUX_MOUNT_ID,    /* name */
        INFLUX_DB_HOST,     /* hostname */
        INFLUX_DB_NAME);    /* database name */

    // "sampleRate=1", /* store at most updates at 1 second intervals */
    if (!influxMount)
    {
        return -1;
    }

    return 0;
}

int writerMain(int argc, char *argv[])
{
    // corto_verbosity(CORTO_TRACE);

    if (corto_load("config.json", 0, NULL)) {
        goto error;
    }

    corto_voidCreateChild_auto(root_o, temperature);
    corto_voidCreateChild_auto(temperature, config);
    manualMount = corto_voidCreateChild(temperature, INFLUX_MOUNT_ID);

    if (CreateManualMount())
    {
        goto error;
    }

    corto_float32DeclareChild_auto(config, temp1);
    corto_float32DeclareChild_auto(config, temp2);
    corto_float32DeclareChild_auto(manualMount, temp3);
    corto_float32DeclareChild_auto(manualMount, temp4);

    corto_float32 t = 0;
    while (1) {
        t += 0.01;
        if (WriteConfig(t, temp1, temp2)) {
            goto error;
        }

        if (WriteConfig(t, temp3, temp4)) {
            goto error;
        }

        corto_sleep(0, 200000000); /* Update 5 times a second */
    }

    corto_release(influxMount);
    corto_release(manualMount);

    return 0;
error:
    corto_error("%s", corto_lasterr());
    return -1;
}
