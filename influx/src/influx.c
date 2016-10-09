
#include "corto/fmt/influx/influx.h"

typedef struct influxSer_t {
    corto_buffer b;
    corto_uint32 fieldCount;
} influxSer_t;

corto_int16 influx_serScalar(
    corto_serializer s,
    corto_value *info,
    void *userData)
{
    influxSer_t *data = userData;
    void *ptr = corto_value_getPtr(info);
    corto_type t = corto_value_getType(info);
    corto_string str = NULL;

    /* Only serialize types supported by influxdb */
    switch(corto_primitive(t)->kind) {
    case CORTO_BOOLEAN:
    case CORTO_INTEGER:
    case CORTO_UINTEGER:
    case CORTO_FLOAT:
    case CORTO_TEXT:
      break;
    default:
      goto unsupported;
    }

    if (data->fieldCount) {
        corto_buffer_appendstr(&data->b, ",");
    }

    if (info->kind == CORTO_MEMBER) {
        corto_buffer_append(&data->b, "%s=", corto_idof(info->is.member.t));
    } else {
        corto_buffer_appendstr(&data->b, "value=");
    }

    switch(corto_primitive(t)->kind) {
    case CORTO_BOOLEAN:
    case CORTO_INTEGER:
    case CORTO_UINTEGER:
    case CORTO_FLOAT:
        corto_convert(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(&data->b, str);
        if ((corto_primitive(t)->kind != CORTO_FLOAT) && (corto_primitive(t)->kind != CORTO_BOOLEAN)) {
            corto_buffer_appendstr(&data->b, "i");
        }
        corto_dealloc(str);
        break;
    case CORTO_TEXT:
        if (*(corto_string*)ptr) {
            corto_buffer_append(&data->b, "\"%s\"", *(corto_string*)ptr);
        } else {
            corto_buffer_append(&data->b, "\"\"");
        }
        break;
    default:
        corto_assert(0, "unreachable code");
        break;
    }

    data->fieldCount ++;

unsupported:
    return 0;
}

corto_int16 influx_serObject(
    corto_serializer s,
    corto_value *info,
    void *userData)
{
    influxSer_t *data = userData;
    corto_object o = corto_value_getObject(info);

    /* Map measurement & tag to parent and id */
    corto_buffer_append(&data->b, "%s,id=%s ",
      corto_idof(corto_parentof(o)),
      corto_idof(o));

    return corto_serializeValue(s, info, userData);
}

corto_string influx_fromCorto(corto_object o) {
    influxSer_t walkData = {CORTO_BUFFER_INIT, 0};
    struct corto_serializer_s s;
    corto_serializerInit(&s);

    /* Only serialize scalars */
    s.access = CORTO_LOCAL|CORTO_PRIVATE;
    s.accessKind = CORTO_NOT;
    s.metaprogram[CORTO_OBJECT] = influx_serObject;
    s.program[CORTO_PRIMITIVE] = influx_serScalar;

    corto_serialize(&s, o, &walkData);

    return corto_buffer_str(&walkData.b);
}

/* Not supported */
corto_int16 influx_toCorto(corto_object o, corto_string data) {
    corto_seterr("conversion from influx to corto not supported");
    return -1;
}

void influx_release(corto_string data) {
    corto_release(data);
}

int influxMain(int argc, char *argv[]) {

    return 0;
}
