#include "include/writer.h"

const corto_id INFLUX_MOUNT_ID = "influx";

int writerMain(int argc, char *argv[])
{
    corto_verbosity(CORTO_TRACE);

    corto_voidCreateChild_auto(root_o, temperature);

    influxdb_MountCreateChild_auto(
        temperature,    /* connect to data in temperature scope */
        INFLUX_MOUNT_ID,       /* name */
        "http://localhost:8086",  /* hostname */
        "mydb");        /* database name */

    //"sampleRate=1", /* store at most updates at 1 second intervals */
    corto_float32DeclareChild_auto(temperature, temp1);
    corto_float32DeclareChild_auto(temperature, temp2);

    corto_float32 t = 0;
    while (1) {
        t += 0.01;
        corto_float32Update(temp1, cos(t) * 100);
        corto_float32Update(temp2, sin(t) * 100);
        corto_sleep(0, 200000000); /* Update 5 times a second */
    }

    return 0;
}
