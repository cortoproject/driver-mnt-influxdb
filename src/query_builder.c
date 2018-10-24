#include <driver/mnt/influxdb/query_builder.h>
#include <corto/string.h>

#define SAFE_DEALLOC(s) if (s) { corto_dealloc(s); s = NULL; }

corto_string influxdb_mount_query_builder_time(
    influxdb_mount this,
    corto_query *query);

corto_string influxdb_mount_query_builder_limit(
    influxdb_mount this,
    corto_query *query);

corto_string influxdb_mount_query_builder_slimit(
    influxdb_mount this,
    corto_query *query);

corto_string influxdb_mount_query_builder_offset(
    influxdb_mount this,
    corto_query *query);

corto_string influxdb_mount_query_builder_soffset(
    influxdb_mount this,
    corto_query *query);

corto_string influxdb_mount_query_builder_url(
    influxdb_mount this)
{
    corto_string user = "";
    corto_string pass = "";
    bool userFree = false;
    bool passFree = false;

    corto_string rp = corto_asprintf("&rp=%s",
        influxdb_mount_retention_policy(this));

    if (this->username) {
        user = corto_asprintf("&u=%s", this->username);
        userFree = true;
    }
    if (this->password) {
        pass = corto_asprintf("&p=%s", this->password);
        passFree = true;
    }

    corto_string url = corto_asprintf("%s:%d/write?db=%s%s%s%s",
        this->host, this->port, this->db, rp, user, pass);

    corto_dealloc(rp);

    if (userFree) {
        corto_dealloc(user);
    }

    if (passFree) {
        corto_dealloc(pass);
    }

    return url;
}

corto_string influxdb_mount_query_builder_select(
    influxdb_mount this,
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

corto_string influxdb_mount_query_builder_from(
    influxdb_mount this,
    corto_query *query)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;
    corto_string from = NULL;
    corto_string db = corto_asprintf("\"%s\"", this->db);
    corto_string rp = corto_asprintf("\"%s\"",
        influxdb_mount_retention_policy(this));

    if (strcmp(query->select, "*") == 0) {
        corto_string pattern = influxdb_mount_query_builder_regex(query->from);
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
        corto_throw("Error generating FROM expression for [%s]", query->from);
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

corto_string influxdb_mount_query_builder_where(
    influxdb_mount this,
    corto_query *query)
{
    corto_string where = NULL;

    corto_string time = influxdb_mount_query_builder_time(this, query);

    if (strlen(time) > 0) {
        where = corto_asprintf(" WHERE %s", time);
    }
    else {
        where = corto_asprintf("");
    }

    SAFE_DEALLOC(time)

    return where;
}

corto_string influxdb_mount_query_builder_type(
    influxdb_mount this,
    corto_query *query)
{
    corto_string type = NULL;

    ///TODO Figure out why Corto is recursively querying with type. We may not
    ///     be able to support type in query, until we can passthrough //
    ///     (select all) requests.
    // if (query->type && strlen(query->type) > 0) {
    //     if (strcmp(query->select, "*") != 0) {
    //         type = corto_asprintf(" type=\"%s\"", query->type);
    //     }
    // }

    if (!type) {
        type = corto_asprintf("");
    }

    return type;
}

corto_string influxdb_mount_query_builder_time(
    influxdb_mount this,
    corto_query *query)
{
    corto_string clause = NULL;

    corto_time begin = corto_frame_getTime(&query->frame_begin);
    corto_time end = corto_frame_getTime(&query->frame_end);

    if (query->frame_begin.kind == CORTO_FRAME_TIME) {
        if (query->frame_end.kind == CORTO_FRAME_NOW) {
            clause = corto_asprintf(" WHERE time > %ds", begin.sec);
        } else if (query->frame_end.kind == CORTO_FRAME_TIME) {
            clause = corto_asprintf(" WHERE time < %ds AND time > %ds",
                begin.sec, end.sec);
        } else if (query->frame_end.kind == CORTO_FRAME_DURATION) {
            clause = corto_asprintf(" WHERE time < %ds AND time > %ds",
                begin.sec, begin.sec - end.sec);
        }
    } else if (query->frame_end.kind == CORTO_FRAME_DURATION) {
        clause = corto_asprintf(" WHERE time > (now() - %ds)", end.sec);
    } else {
        clause = corto_asprintf("");
    }

    clause = corto_asprintf("");

    return clause;
}

corto_string influxdb_mount_query_builder_order(
    influxdb_mount this,
    corto_query *query)
{
    corto_string order = NULL;

    order = corto_asprintf(" ORDER BY time DESC");

    return order;
}

corto_string influxdb_mount_query_builder_paginate(
    influxdb_mount this,
    corto_query *query,
    bool historical)
{
    corto_string paginate = NULL;

    /* InfluxDB pagination shall only be applied to historical queries. Else,
       only return latest sample (LIMIT 1). */
    if (historical) {
        corto_string limit = influxdb_mount_query_builder_limit(this, query);
        corto_string slimit = influxdb_mount_query_builder_slimit(this, query);
        corto_string offset = influxdb_mount_query_builder_offset(this, query);
        corto_string soffset = influxdb_mount_query_builder_soffset(this, query);

        paginate = corto_asprintf("%s%s%s%s", limit, offset, slimit, soffset);

        SAFE_DEALLOC(limit)
        SAFE_DEALLOC(slimit)
        SAFE_DEALLOC(offset)
        SAFE_DEALLOC(soffset)
    } else {
        paginate = corto_asprintf(" LIMIT 1");
    }

    return paginate;
}

/*
 * OFFSET <N> paginates N points in the query results.
 * INFLUXDB POINT: The part of InfluxDB’s data structure that consists of a
 * single collection of fields in a series. Each point is uniquely identified
 * by its series and timestamp.
 *
 * Mapping To Corto: History samples.
 */
corto_string influxdb_mount_query_builder_limit(
    influxdb_mount this,
    corto_query *query)
{
    corto_string limit = NULL;

    /* CORTO_FRAME_DEPTH = a number of samples. This is a relative sample
     * offset. So for example, give me the last 10 samples starting from NOW
     */
     if (query->slimit > 0) {
         limit = corto_asprintf(" LIMIT %llu", query->slimit);
     } else {
         limit = corto_asprintf("");
     }

    return limit;
}

/*
 * SOFFSET <N> paginates N series in the query results.
 * INFLUXDB SERIES: The collection of data in InfluxDB’s data structure that
 * share a measurement, tag set, and retention policy.
 *
 * Mapping To Corto: Object instances
 */
corto_string influxdb_mount_query_builder_slimit(
    influxdb_mount this,
    corto_query *query)
{
    corto_string limit = NULL;

    if (query->limit > 0) {
        limit = corto_asprintf(" SLIMIT %llu", query->limit);
    } else {
        limit = corto_asprintf("");
    }

    return limit;
}

/* History Samples */
corto_string influxdb_mount_query_builder_offset(
    influxdb_mount this,
    corto_query *query)
{
    corto_string offset = NULL;

    /* INFLUXDB: The OFFSET clause requires a LIMIT clause. Using the OFFSET clause
     * without a LIMIT clause can cause inconsistent query results.
     *
     * CORTO_FRAME_SAMPLE = an absolute sample identifier, starting from the
     * first sample written (first sample written for a series is # 0)
     */

     if ((query->soffset > 0) && (query->slimit > 0)) {
         offset = corto_asprintf(" OFFSET %llu", query->soffset);
     } else {
         offset = corto_asprintf("");
     }

    return offset;
}

/* Objects */
corto_string influxdb_mount_query_builder_soffset(
    influxdb_mount this,
    corto_query *query)
{
    corto_string offset = NULL;

    /* The OFFSET clause requires a LIMIT clause. Using the OFFSET clause
     * without a LIMIT clause can cause inconsistent query results.
     */

    if ((query->offset > 0) && (query->limit > 0)) {
        offset = corto_asprintf(" SOFFSET %llu", query->offset);
    } else {
        offset = corto_asprintf("");
    }

    return offset;
}

corto_string influxdb_mount_query_builder_regex(
    corto_string pattern)
{
    corto_string regex = NULL;
    if (strcmp(pattern, ".") == 0) {
        regex = corto_asprintf("^[^\\/]+$");
    }
    else {
        /* escape pattern (path) for regex */
        corto_string escaped = strreplace(pattern, "/", "\\/");
        regex = corto_asprintf("^(%s\\/)[^\\/]+$", escaped);
        corto_dealloc(escaped);
    }

    return regex;
}
