#include <driver/mnt/influxdb/query_builder.h>

corto_string influxdb_Mount_query_builder_select(
    influxdb_Mount this,
    corto_query *query)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    ///TODO TEST MEMBER
    if (query->member != NULL) {
        if (strcmp(query->member, "*") != 0) {
            // sprintf(idFilter, "id = '%s'", query->select);
            //TODO Handle Select targets
            corto_buffer_appendstr(&buffer, query->member);
        }
        else {
            corto_buffer_appendstr(&buffer, query->member);
        }
    }
    else {
        corto_buffer_appendstr(&buffer, "*");
    }

    return corto_buffer_str(&buffer);
}

corto_string influxdb_Mount_query_builder_from(
    influxdb_Mount this,
    corto_query *query)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    if (strcmp(query->from, ".") != 0) {
        corto_string from = corto_asprintf(" FROM \"%s\".\"autogen\".\"%s\"",
            this->db, query->from);
        if (from) {
            corto_buffer_appendstr(&buffer, from);
            corto_dealloc(from);
        }
        else {
            corto_seterr("Failed to create InfluxDB FROM statement.");
            goto error;
        }
    }
    else {
        corto_string from = NULL;
        if (strcmp(query->select, "*") != 0) {
            from = corto_asprintf(" FROM \"%s\".\"autogen\".\"%s\"",
            this->db, query->select);
        }
        // else {
        //     from = corto_asprintf(" FROM \"%s\".\"autogen\".\"%s\"",
        //     this->db, mountFrom);
        // }

        if (from) {
            corto_buffer_appendstr(&buffer, from);
            corto_dealloc(from);
        }
        else {
            corto_seterr("Failed to create InfluxDB FROM statement.");
            goto error;
        }
    }

    return corto_buffer_str(&buffer);
error:
    corto_buffer_reset(&buffer);
    return NULL;
}

corto_string influxdb_Mount_query_builder_where(
    influxdb_Mount this,
    corto_query *query)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    corto_string timeLimit = NULL;
    corto_string sampleLimit = NULL;
    corto_string depthLimit = NULL;
    if (query->timeBegin.kind == CORTO_FRAME_NOW) {
        if (query->timeBegin.kind == CORTO_FRAME_TIME) {
            corto_time t = corto_frame_getTime(&query->timeBegin);
            timeLimit = corto_asprintf(" WHERE time > %ds", t.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
            corto_time t = corto_frame_getTime(&query->timeEnd);
            timeLimit = corto_asprintf(" WHERE time > (now() - %ds)", t.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_SAMPLE) {
            if (query->timeEnd.value) {
                sampleLimit = corto_asprintf(" OFFSET %ld", query->timeEnd.value);
            }

        } else if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
            depthLimit = corto_asprintf(" ORDER BY time DESC LIMIT %ld", query->timeEnd.value);
        }
    } else if (query->timeBegin.kind == CORTO_FRAME_TIME) {
        corto_time from = corto_frame_getTime(&query->timeBegin);
        if (query->timeEnd.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&query->timeEnd);
            timeLimit = corto_asprintf(" WHERE time < %ds AND time > %ds", from.sec, to.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
            corto_time to = corto_frame_getTime(&query->timeEnd);
            timeLimit = corto_asprintf(" WHERE time < %ds AND time > %ds",
                from.sec,
                from.sec - to.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_SAMPLE) {
            timeLimit = corto_asprintf(" WHERE time < %ds", from.sec);
            if (query->timeEnd.value) {
                sampleLimit = corto_asprintf(" OFFSET %ld", query->timeEnd.value);
            }
        } else if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
            timeLimit = corto_asprintf(" WHERE time < %ds", from.sec);
            depthLimit = corto_asprintf(" ORDER BY time DESC LIMIT %ld", query->timeEnd.value);
        }
    } else if (query->timeBegin.kind == CORTO_FRAME_SAMPLE) {
        if (query->timeBegin.value) {
            sampleLimit = corto_asprintf(" OFFSET %ld", query->timeBegin.value);
        }
        if (query->timeEnd.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&query->timeEnd);
            timeLimit = corto_asprintf(" WHERE time < %ds", to.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
            /* Unsupported */
        } else if (query->timeEnd.kind == CORTO_FRAME_SAMPLE) {
            if (query->timeEnd.value) {
                depthLimit = corto_asprintf(" LIMIT %ld", query->timeEnd.value - query->timeBegin.value);
            }
        } else if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
            depthLimit = corto_asprintf(" ORDER BY time DESC LIMIT %ld", query->timeEnd.value);
        }
    }

    if (timeLimit) {
        corto_buffer_appendstr(&buffer, timeLimit);
        corto_dealloc(timeLimit);
    }

    if (sampleLimit) {
        corto_buffer_appendstr(&buffer, sampleLimit);
        corto_dealloc(sampleLimit);
    }

    if (depthLimit) {
        corto_buffer_appendstr(&buffer, depthLimit);
        corto_dealloc(depthLimit);
    }

    return corto_buffer_str(&buffer);
}
