#include <include/container_writer.h>

const corto_string ETC_PATH = "/usr/local/etc/corto/1.3/influxdb/examples/container_writer";
const corto_string INFLUX_CONFIG = "influxdb.json";

void Update()
{
    corto_time time;
    corto_string typeStr;
    corto_type type;
    corto_timeGet(&time);
    void *value = nullptr;

    container_writer_Bool result = container_writer_BoolCreate(true, &time);
    if (result == nullptr)
    {
        corto_error("Failed to create.");
        return;
    }
    type = corto_typeof(result);
    typeStr = corto_fullpath(nullptr, type);
    if (corto_ptr_copy(&value, type, &result) != 0)
    {
        corto_error("Failed to copy Amphion String value. Error: %s",
            corto_lasterr());
        return;
    }
    corto_release(result);

    corto_publish(CORTO_UPDATE, "/data/alarms/alarm1/result", typeStr, "binary/corto", value);

    // container_writer_String message = container_writer_StringCreate("Test_Message #", &time);
    // typeStr = corto_fullpath(nullptr, corto_typeof(message));
    // corto_publish(CORTO_UPDATE, "/data/alarms/alarm1/message", typeStr, "binary/corto", value);
    //
    // container_writer_Int preset = container_writer_IntCreate(32, &time);
    // typeStr = corto_fullpath(nullptr, corto_typeof(preset));
    // corto_publish(CORTO_UPDATE, "/data/alarms/alarm1/preset", typeStr, "binary/corto", value);
    //
    // container_writer_Float timer = container_writer_FloatCreate(58.1, &time);
    // typeStr = corto_fullpath(nullptr, corto_typeof(timer));
    // corto_publish(CORTO_UPDATE, "/data/alarms/alarm1/timer", typeStr, "binary/corto", value);
}

int container_writerMain(int argc, char *argv[]) {

    corto_string path = corto_asprintf("%s/%s", ETC_PATH, INFLUX_CONFIG);

    if (path != nullptr)
    {
        if (corto_load(path, 0, nullptr) != 0)
        {
            corto_error("Failed to load [%s] - Error [%s]", path, corto_lasterr());
            if (path) {
                corto_dealloc(path);
            }
            return -1;
        }
        corto_dealloc(path);
    }

    corto_voidCreateChild_auto(root_o, data);
    corto_voidCreateChild_auto(data, alarms);
    corto_voidCreateChild_auto(alarms, alarm1);

    while (1) {
        Update();
        usleep(1000*500);
    }

    return 0;
}
