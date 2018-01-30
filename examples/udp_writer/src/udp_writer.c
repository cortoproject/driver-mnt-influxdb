#include <include/udp_writer.h>

int cortomain(int argc, char *argv[]) {

    corto_object config = corto_create(root_o, "config", corto_void_o);
    influxdb_Mount mount = corto_declare(config, "influx", influxdb_Mount_o);
    corto_query query = {
        .select = "//",
        .from = "/weather"
    };

    corto_queuePolicy queue = {
        .max = 10
    };

    corto_mountPolicy policy = {
        .ownership = CORTO_REMOTE_SOURCE,
        .mask = CORTO_MOUNT_HISTORY_BATCH_NOTIFY,
        .sampleRate = 20.0,
        .queue = queue
    };

    influxdb_UdpConn udp = influxdb_UdpConn__create(
        NULL,
        NULL,
        "localhost",
        "8089",
        0);

    influxdb_Mount__assign(
        mount,
        &query,
        "text/json",
        &policy,
        "localhost",  /* hostname */
        8086,
        udp,
        "udp_test",   /* database name */
        NULL,         /* Retention Policy */
        NULL,         /* username */
        NULL);        /* password */

    int ret = corto_define(mount);

    if (ret != 0)
    {
        corto_error("Failed to define manual mount.");
        return false;
    }


    corto_time now;
    corto_time_get(&now);
    corto_object weather = corto_create(root_o, "weather", corto_void_o);
    udp_writer_Weather sanDiego = udp_writer_Weather__create(
        weather,
        "San Diego",
        82,
        45.5,
        8,
        &now
    );
    udp_writer_Weather houston = udp_writer_Weather__create(
        weather,
        "Houston",
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
            corto_error("corto_update_begin for houston.");
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
            corto_error("corto_update_begin for houston.");
            goto error;
        }
        usleep(1000*50);
    }

    corto_info("udp_writer complete.");

    return 0;
error:
    return -1;
}
