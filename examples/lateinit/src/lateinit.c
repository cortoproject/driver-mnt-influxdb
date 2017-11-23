#include <include/lateinit.h>
#include <include/common.h>

int lateinitMain(int argc, char *argv[]) {

    /***** UNCOMMENT FOR WORKING
    if (lateinit_createMount() != 0) {
        corto_error("Failed to create mount.");
        goto error;
    }
    ******/

    if (lateinit_initialize() != 0) {
        corto_error("Failed to initialize.");
        goto error;
    }

    int i;
    for (i = 0; i < 100; i++) {
        if (lateinit_update() != 0) {
            corto_error("Failed to update.");
            goto error;
        }
        usleep(50*1000); // 50ms
        if (i  == 6) {
            /*** Comment this out for working */
            if (lateinit_createMount() != 0) {
                corto_error("Failed to create mount.");
                goto error;
            }
        }
    }

    return 0;
error:
    corto_error("Exit.");
    return -1;
}

int16_t lateinit_createMount(void)
{
    return CreateHistoricalManualMount();
}

int16_t lateinit_initialize(void)
{
    return UpdateWeather();
}

int16_t lateinit_update(void)
{
    return UpdateWeather();
}
