/* _project.c
 *
 * This file contains the project entry function. Here, we
 * start/stop corto and load all dependencies, before and after
 * invoking the main function of your project.
 */

#include <driver/fmt/influxdb/influxdb.h>

int influxdbMain(int argc, char* argv[]);

#ifdef __cplusplus
extern "C"
#endif
int cortomain(int argc, char* argv[]) {
    if (influxdbMain(argc, argv)) return -1;
    return 0;
}

