/* This is a managed file. Do not delete this comment. */

#include <driver/mnt/influxdb/influxdb.h>

int cortomain(int argc, char *argv[]) {

    /* Insert code that must be run when component is loaded */

    return 0;
}

bool influxdb_serialize_scalar(
    corto_buffer *buffer,
    corto_string member,
    corto_object o)
{
    corto_type t = corto_typeof(o);
    corto_value v = corto_value_object(
        o,
        t);
    corto_string str = NULL;
    void *ptr = corto_value_ptrof(&v);


    /* Only serialize types supported by influxdb */
    switch(corto_primitive(t)->kind) {
    case CORTO_BOOLEAN:
    case CORTO_INTEGER:
    case CORTO_UINTEGER:
    case CORTO_FLOAT:
    case CORTO_TEXT:
    case CORTO_ENUM:
        break;
    default:
        goto unsupported;
    }

    // member=
    corto_buffer_appendstr(buffer, member);
    corto_buffer_appendstr(buffer, "=");

    switch(corto_primitive(t)->kind) {
    case CORTO_BOOLEAN:
        if (*(corto_bool*)ptr == true) {
            corto_buffer_appendstr(buffer, "true");
        } else {
            corto_buffer_appendstr(buffer, "false");
        }
        break;
    case CORTO_INTEGER:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(buffer, str);
        corto_dealloc(str);
        corto_buffer_appendstr(buffer, "i");
        break;
    case CORTO_UINTEGER:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(buffer, str);
        corto_dealloc(str);
        corto_buffer_appendstr(buffer, "i");
        break;
    case CORTO_FLOAT:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(buffer, str);
        if ((corto_primitive(t)->kind != CORTO_FLOAT) && (corto_primitive(t)->kind != CORTO_BOOLEAN)) {
            corto_buffer_appendstr(buffer, "i");
        }
        corto_dealloc(str);
        break;
    case CORTO_TEXT:
        if (*(corto_string*)ptr) {
            corto_buffer_appendstrn(buffer, "\"", 1);
            corto_buffer_appendstr(buffer, *(corto_string*)ptr);
            corto_buffer_appendstrn(buffer, "\"", 1);
        } else {
            corto_buffer_appendstrn(buffer, "\"\"", 2);
        }
        break;
    case CORTO_ENUM:
        corto_buffer_appendstrn(buffer, "\"", 1);
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(buffer, str);
        corto_dealloc(str);
        corto_buffer_appendstrn(buffer, "\"", 1);
        break;
    default:
        corto_assert(0, "unreachable code");
        break;
    }

    return true;
unsupported:
    return false;
}

void influxdb_safeString(
    corto_buffer *b,
    char *source)
{
    /* Measurements and Tags names cannot contain non-espaced spaces */
    char *ptr, ch;
    for (ptr = source; (ch = *ptr); ptr++) {
        if (ch == ' ') {
            corto_buffer_appendstrn(b, "\\ ", 2);
        } else {
            corto_buffer_appendstrn(b, ptr, 1);
        }
    }
}

corto_string influxdb_sample(
    const char *measurement_id,
    const char *type,
    const char *data)
{
    corto_buffer b = CORTO_BUFFER_INIT;
    /* Map measurement & tag to parent and id
    * Format: measurement(path),type dataFields
    */

    influxdb_safeString(&b, (char*)measurement_id);
    corto_buffer_appendstrn(&b, ",type=", 6);
    influxdb_safeString(&b, (char*)type);
    corto_buffer_appendstrn(&b, " ", 1);
    influxdb_safeString(&b, (char*)data);

    return corto_buffer_str(&b);
}
