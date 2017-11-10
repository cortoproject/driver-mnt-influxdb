#include <driver/mnt/influxdb/query_builder.h>

#define SAFE_DEALLOC(s) if (s) { corto_dealloc(s); s = NULL; }

corto_string influxdb_Mount_query_builder_url(
    influxdb_Mount this)
{
    corto_string user = "";
    corto_string pass = "";
    bool userFree = false;
    bool passFree = false;

    corto_string rp = corto_asprintf("&rp=%s",
        influxdb_Mount_retentionPolicy(this));

    if (this->username) {
        user = corto_asprintf("&u=%s", this->username);
        userFree = true;
    }
    if (this->password) {
        pass = corto_asprintf("&p=%s", this->password);
        passFree = true;
    }

    corto_string url = corto_asprintf("%s/write?db=%s%s%s%s",
        this->host, this->db, rp, user, pass);

    corto_dealloc(rp);

    if (userFree) {
        corto_dealloc(user);
    }

    if (passFree) {
        corto_dealloc(pass);
    }

    return url;
}

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
    corto_string from = NULL;
    corto_string db = corto_asprintf("\"%s\"", this->db);
    corto_string rp = corto_asprintf("\"%s\"",
        influxdb_Mount_retentionPolicy(this));

    if (strcmp(query->select, "*") == 0) {
        corto_string pattern = influxdb_Mount_query_builder_regex(query->from);
        from = corto_asprintf(" FROM %s.%s./%s/", db, rp, pattern);
        SAFE_DEALLOC(pattern)
    }
    else {
        if (strcmp(query->from, ".") == 0) {
            from = corto_asprintf(" FROM %s.%s.\"%s\"",
                db, rp, query->select);
        }
        else {
            from = corto_asprintf(" FROM %s.%s.\"%s/%s\"",
                db, rp, query->from, query->select);
        }
    }

    if (from) {
        corto_buffer_appendstr(&buffer, from);
    }
    else {
        corto_seterr("Error generating FROM expression for [%s]", query->from);
        goto error;
    }

    SAFE_DEALLOC(from)
    SAFE_DEALLOC(db)
    SAFE_DEALLOC(rp)

    return corto_buffer_str(&buffer);
error:
    SAFE_DEALLOC(from)
    SAFE_DEALLOC(db)
    SAFE_DEALLOC(rp)
    corto_buffer_reset(&buffer);
    return NULL;
}

corto_string influxdb_Mount_query_builder_where(
    influxdb_Mount this,
    corto_query *query)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    // corto_string timeLimit = NULL;
    // corto_string sampleLimit = NULL;
    // corto_string depthLimit = NULL;
    // if (query->timeBegin.kind == CORTO_FRAME_NOW) {
    //     if (query->timeBegin.kind == CORTO_FRAME_TIME) {
    //         corto_time t = corto_frame_getTime(&query->timeBegin);
    //         timeLimit = corto_asprintf(" WHERE time > %ds", t.sec);
    //     } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
    //         corto_time t = corto_frame_getTime(&query->timeEnd);
    //         timeLimit = corto_asprintf(" WHERE time > (now() - %ds)", t.sec);
    //     } else if (qu       ery->timeEnd.kind == CORTO_FRAME_SAMPLE) {
    //         if (query->timeEnd.value) {
    //             sampleLimit = corto_asprintf(" OFFSET %ld", query->timeEnd.value);
    //         }
    //
    //     } else if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
    //         depthLimit = corto_asprintf(" ORDER BY time DESC LIMIT %ld", query->timeEnd.value);
    //     }
    // } else if (query->timeBegin.kind == CORTO_FRAME_TIME) {
    //     corto_time from = corto_frame_getTime(&query->timeBegin);
    //     if (query->timeEnd.kind == CORTO_FRAME_TIME) {
    //         corto_time to = corto_frame_getTime(&query->timeEnd);
    //         timeLimit = corto_asprintf(" WHERE time < %ds AND time > %ds", from.sec, to.sec);
    //     } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
    //         corto_time to = corto_frame_getTime(&query->timeEnd);
    //         timeLimit = corto_asprintf(" WHERE time < %ds AND time > %ds",
    //             from.sec,
    //             from.sec - to.sec);
    //     } else if (query->timeEnd.kind == CORTO_FRAME_SAMPLE) {
    //         timeLimit = corto_asprintf(" WHERE time < %ds", from.sec);
    //         if (query->timeEnd.value) {
    //             sampleLimit = corto_asprintf(" OFFSET %ld", query->timeEnd.value);
    //         }
    //     } else if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
    //         timeLimit = corto_asprintf(" WHERE time < %ds", from.sec);
    //         depthLimit = corto_asprintf(" ORDER BY time DESC LIMIT %ld", query->timeEnd.value);
    //     }
    // } else if (query->timeBegin.kind == CORTO_FRAME_SAMPLE) {
    //     if (query->timeEnd.kind == CORTO_FRAME_TIME) {
    //         corto_time to = corto_frame_getTime(&query->timeEnd);
    //         timeLimit = corto_asprintf(" WHERE time < %ds", to.sec);
    //     }
    // }
    //
    // if (timeLimit) {
    //     corto_buffer_appendstr(&buffer, timeLimit);
    //     corto_dealloc(timeLimit);
    // }
    //
    // if (sampleLimit) {
    //     corto_buffer_appendstr(&buffer, sampleLimit);
    //     corto_dealloc(sampleLimit);
    // }
    //
    // if (depthLimit) {
    //     corto_buffer_appendstr(&buffer, depthLimit);
    //     corto_dealloc(depthLimit);
    // }

    return corto_buffer_str(&buffer);
}

corto_string influxdb_Mount_query_builder_limit(
    influxdb_Mount this,
    corto_query *query)
{
    corto_string limit = NULL;

    if (query->limit > 0) {
        limit = corto_asprintf(" LIMIT %llu", query->limit);
    }

    return limit;
}

DRIVER_MNT_INFLUXDB_EXPORT
corto_string influxdb_Mount_query_builder_offset(
    influxdb_Mount this,
    corto_query *query)
{
    corto_string offset = NULL;

    /* The OFFSET clause requires a LIMIT clause. Using the OFFSET clause
     * without a LIMIT clause can cause inconsistent query results.
     */

    if ((query->offset > 0) && (query->limit > 0)) {
        offset = corto_asprintf(" OFFSET %llu", query->offset);
    }

    return offset;
}

corto_string influxdb_Mount_query_builder_regex(
    corto_string pattern)
{
    corto_string regex = NULL;
    if (strcmp(pattern, ".") == 0) {
        regex = corto_asprintf("^[^\\/]+$");
    }
    else {
        /* escape pattern (path) for regex */
        corto_string escaped = corto_replace(pattern, "/", "\\/");
        regex = corto_asprintf("^(%s\\/)[^\\/]+$", escaped);
        corto_dealloc(escaped);
    }

    return regex;
}
