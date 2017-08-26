#include "include/writer.h"

corto_id INFLUX_MOUNT_ID = "influx_manual";
corto_string INFLUX_DB_HOST = "http://localhost:8086";
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
    corto_string fromPath = corto_fullpath(NULL, mountPoint);
    corto_query* query = corto_queryCreate("//", fromPath, NULL, NULL, NULL, 0, 0, NULL, NULL);
    corto_mountPolicy *policy = corto_mountPolicyCreate(CORTO_LOCAL_OWNER,
         CORTO_NOTIFY,
         1,
         0);

    if (influxdb_MountDefine(influxdbMount,
        query,
        "text/json",
        policy,
        INFLUX_DB_HOST,     /* hostname */
        INFLUX_DB_NAME))    /* database name */
    {
        corto_error("Failed to define manual mount");
        return -1;
    }

    corto_release(query);
    corto_release(policy);

    return 0;
}

int writerMain(int argc, char *argv[])
{
    corto_verbosity(CORTO_TRACE);

    if (corto_load("config.json", 0, NULL)) {
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
    corto_info("Created Temperature 1 - [%s]", corto_fullpath(NULL, temp1));
    corto_float32DeclareChild_auto(manual, temp3);
    corto_float32DeclareChild_auto(manual, temp4);
    corto_info("Created Temperature 3 - [%s]", corto_fullpath(NULL, temp3));

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

    corto_release(influxdbMount);

    return 0;
error:
    corto_error("%s", corto_lasterr());
    return -1;
}
