#include "writer.h"

int writerMain(int argc, char *argv[]) {

    corto_voidCreateChild_auto(root_o, measurements);

    influxdb_ConnectorCreate_auto(
      influxdb,       /* name */
      measurements,   /* connect to data in measurements scope */
      "sampleRate=1", /* store at most updates at 1 second intervals */
      CORTO_ON_SCOPE, /* trigger on updates in scope */
      "http://localhost:8086",  /* hostname */
      "mydb");        /* database name */

    corto_float32DeclareChild_auto(measurements, temp1);
    corto_float32DeclareChild_auto(measurements, temp2);

    corto_float32 t = 0;
    while (1) {
        t += 0.01;
        corto_float32Update(temp1, cos(t) * 100);
        corto_float32Update(temp2, sin(t) * 100);
        corto_sleep(0, 200000000); /* Update 5 times a second */
    }

    return 0;
}
