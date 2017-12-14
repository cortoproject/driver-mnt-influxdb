#include "include/object_writer.h"

int object_writerMain(int argc, char *argv[]) {
    // corto_verbosity(CORTO_TRACE);

    if (corto_use("config.json", 0, NULL)) {
        goto error;
    }

    corto_time now;
    corto_time_get(&now);
    corto_voidCreateChild_auto(root_o, weather);
    object_writer_Weather sanDiego = object_writer_WeatherCreateChild(
        weather, "San Diego", 82, 45.5, 8, &now
    );
    object_writer_Weather houston = object_writer_WeatherCreateChild(
        weather, "Houston", 95, 78.8, 6, &now
    );
    corto_voidCreateChild_auto(weather, voidTest);
    corto_voidCreateChild_auto(weather, voidTest2);
    object_writer_WeatherCreateChild(
        voidTest2, "test3chidl", 95, 78.8, 6, &now
    );
    corto_float32 t = 0;
    while (1) {
        t += 0.01;
        // corto_float32Update(t1, cos(temperature) * 100);
        // corto_float32Update(t2, sin(temperature) * 100);
        corto_time_get(&now);
        if (corto_updateBegin(houston) != 0)
        {
            corto_error("corto_updateBegin for houston.");
            goto error;
        }

        houston->temperature += cos(houston->temperature + 0.1);
        houston->humidity += sin(houston->humidity + 0.1);
        houston->timestamp = now;
        if (corto_updateEnd(houston) != 0)
        {
            corto_error("corto_updateBegin for houston.");
            goto error;
        }

        if (corto_updateBegin(sanDiego) != 0)
        {
            corto_error("corto_updateBegin for houston.");
            goto error;
        }

        sanDiego->temperature += cos(sanDiego->temperature + 0.1);
        sanDiego->humidity += sin(sanDiego->humidity + 0.1);
        sanDiego->timestamp = now;
        if (corto_updateEnd(sanDiego) != 0)
        {
            corto_error("corto_updateBegin for houston.");
            goto error;
        }

        corto_sleep(0, 200000000); /* Update 5 times a second */
    }

    corto_release(sanDiego);
    corto_release(weather);
    return 0;
error:
    corto_error("%s", corto_lasterr());
    return -1;
}

int cortomain(int argc, char *argv[]) {

    /* Insert implementation */
    
    return 0;
}

