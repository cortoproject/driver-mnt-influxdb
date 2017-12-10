#include <include/test.h>

int testMain(int argc, char *argv[]) {
    int result = 0;
    test_Runner runner = test_RunnerCreate("driver/mnt/influxdb", argv[0], (argc > 1) ? argv[1] : NULL);
    if (!runner) return -1;
    if (corto_ll_count(runner->failures)) {
        result = -1;
    }
    corto_delete(runner);
    return result;
}
