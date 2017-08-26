#include <driver/fmt/influxdb/influxdb.h>

typedef struct influxdbSer_t {
    corto_buffer b;
    corto_uint32 fieldCount;
} influxdbSer_t;

corto_int16 influxdb_serScalar(
    corto_walk_opt *walk,
    corto_value *info,
    void *userData)
{
    influxdbSer_t *data = userData;
    void *ptr = corto_value_ptrof(info);
    corto_type t = corto_value_typeof(info);
    corto_object o = corto_value_objectof(info);
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
        corto_string line;
        corto_asprintf(&line, "%s %s=",
            corto_fullpath(NULL, corto_parentof(o)), corto_idof(o));
        corto_buffer_append(&data->b, "%s", line);
        corto_info("POST [%s] FULLPATH [%s]", line, corto_fullpath(NULL, o));
        // corto_buffer_appendstr(&data->b, line);
    }

    corto_info("Walk Scalar fieldCount[%i] [%s=]", data->fieldCount,
        corto_idof(info->is.member.t));

    switch(corto_primitive(t)->kind) {
    case CORTO_BOOLEAN:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(&data->b, str);
        corto_dealloc(str);
        break;
    case CORTO_INTEGER:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(&data->b, str);
        corto_dealloc(str);
        corto_buffer_appendstr(&data->b, "i");
        break;
    case CORTO_UINTEGER:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
        corto_buffer_appendstr(&data->b, str);
        corto_dealloc(str);
        corto_buffer_appendstr(&data->b, "i");
        break;
    case CORTO_FLOAT:
        corto_ptr_cast(t, ptr, corto_string_o, &str);
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

    data->fieldCount++;

    // corto_release(o);

unsupported:
    return 0;
}

int16_t influxdb_serObject(
    corto_walk_opt *walk,
    corto_value *info,
    void *userData)
{
    influxdbSer_t *data = userData;
    corto_object o = corto_value_objectof(info);

    /* Map measurement & tag to parent and id */
    corto_buffer_append(&data->b, "%s,id=%s ",
    corto_idof(corto_parentof(o)),
    corto_idof(o));

    corto_info("Walk Object [%s] [%s,id=%s]", corto_fullpath(NULL, o),
        corto_idof(corto_parentof(o)), corto_idof(o));

    if (corto_walk_value(walk, info, userData)) {
        goto error;
    }

    return 0;
error:
    return -1;
}

corto_string influxdb_fromValue(corto_value *v) {
    influxdbSer_t walkData = {CORTO_BUFFER_INIT, 0};
    corto_walk_opt walk;
    corto_walk_init(&walk);

    /* Only serialize scalars */
    walk.access = CORTO_LOCAL|CORTO_PRIVATE;
    walk.accessKind = CORTO_NOT;
    walk.metaprogram[CORTO_OBJECT] = influxdb_serObject;
    walk.program[CORTO_PRIMITIVE] = influxdb_serScalar;

    corto_walk_value(&walk, v, &walkData);

    return corto_buffer_str(&walkData.b);
}

/* Not supported */
corto_int16 influxdb_toValue(corto_value *v, corto_string data) {
    corto_seterr("conversion from influx to corto not supported");
    return -1;
}

corto_int16 influxdb_toObject(corto_object* o, corto_string s) {
    corto_seterr("conversion from influx to corto not supported");
    return -1;
}

corto_string influxdb_fromObject(corto_object o) {
    influxdbSer_t walkData = {CORTO_BUFFER_INIT, 0};
    corto_walk_opt walk;
    corto_walk_init(&walk);

    /* Only serialize scalars */
    walk.access = CORTO_LOCAL|CORTO_PRIVATE;
    walk.accessKind = CORTO_NOT;
    walk.metaprogram[CORTO_OBJECT] = influxdb_serObject;
    walk.program[CORTO_PRIMITIVE] = influxdb_serScalar;

    corto_walk(&walk, o, &walkData);

    return corto_buffer_str(&walkData.b);
}

corto_word influxdb_fromResult(corto_result *r) {
    CORTO_UNUSED(r);
    return 0;
}

corto_int16 influxdb_toResult(corto_result *r, corto_string influx) {
    corto_seterr("conversion from influx to corto not supported");
    return -1;
}

void influxdb_release(corto_string data) {
    corto_dealloc(data);
}

corto_string influxdb_copy(corto_string data) {
    return corto_strdup(data);
}

int influxdbMain(int argc, char *argv[]) {
    CORTO_UNUSED(argc);
    CORTO_UNUSED(argv);
    return 0;
}
