#include "writer.h"

int writerMain(int argc, char *argv[]) {

    corto_verbosity(CORTO_TRACE);

    corto_voidCreateChild_auto(root_o, temperature);

    influxdb_ConnectorCreate_auto(
      influxdb,       /* name */
      temperature,    /* connect to data in temperature scope */
      "sampleRate=1", /* store at most updates at 1 second intervals */
      "http://localhost:8086",  /* hostname */
      "mydb");        /* database name */

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
