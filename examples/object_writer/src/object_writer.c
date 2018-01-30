#include "include/object_writer.h"

int cortomain(int argc, char *argv[]) {
    if (corto_use("config.json", 0, NULL)) {
        corto_error("Failed to load [config.json]");
        goto error;
    }

    corto_time now;
    corto_time_get(&now);
    corto_object weather = corto_create(root_o, "weather", corto_void_o);
    object_writer_Weather sanDiego = object_writer_Weather__create(
        weather,
        "San Diego",
        82,
        45.5,
        8,
        &now
    );
    object_writer_Weather houston = object_writer_Weather__create(
        weather,
        "Houston",
        95,
        78.8,
        6,
        &now
    );
    corto_create(weather, "voidTest", corto_void_o);
    corto_object voidTest2 = corto_create(weather, "voidTest2", corto_void_o);
    object_writer_Weather__create(
        voidTest2,
        "test3chidl",
        95,
        78.8,
        6,
        &now
    );
    corto_float32 t = 0;


    corto_info("Update weather.");
    int i = 0;
    for (i = 0; i < 5; i++) {
        t += 0.01;
        // corto_float32Update(t1, cos(temperature) * 100);
        // corto_float32Update(t2, sin(temperature) * 100);
        corto_time_get(&now);
        if (corto_update_begin(houston) != 0)
        {
            corto_error("corto_update_begin for houston.");
            goto error;
        }

        houston->temperature += cos(houston->temperature + 0.1);
        houston->humidity += sin(houston->humidity + 0.1);
        houston->timestamp = now;
        if (corto_update_end(houston) != 0)
        {
            corto_error("corto_update_end for houston.");
            goto error;
        }

        if (corto_update_begin(sanDiego) != 0)
        {
            corto_error("corto_update_begin for houston.");
            goto error;
        }

        sanDiego->temperature += cos(sanDiego->temperature + 0.1);
        sanDiego->humidity += sin(sanDiego->humidity + 0.1);
        sanDiego->timestamp = now;
        if (corto_update_end(sanDiego) != 0)
        {
            corto_error("corto_update_end for houston.");
            goto error;
        }

        corto_sleep(0, 20000000); /* Update 50 times a second */
    }

    corto_info("object_writer complete.");

    return 0;
error:
    corto_error("Exit.");
    return -1;
}
