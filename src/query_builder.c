#include <driver/mnt/influxdb/query_builder.h>

#define SAFE_DEALLOC(s) if (s) { corto_dealloc(s); s = NULL; }

corto_string influxdb_Mount_query_builder_type(
    influxdb_Mount this,
    corto_query *query);

corto_string influxdb_Mount_query_builder_time(
    influxdb_Mount this,
    corto_query *query);

corto_string influxdb_Mount_query_builder_limit(
    influxdb_Mount this,
    corto_query *query);

corto_string influxdb_Mount_query_builder_slimit(
    influxdb_Mount this,
    corto_query *query);

corto_string influxdb_Mount_query_builder_offset(
    influxdb_Mount this,
    corto_query *query);

corto_string influxdb_Mount_query_builder_soffset(
    influxdb_Mount this,
    corto_query *query);

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
    corto_string where = NULL;

    corto_string type = influxdb_Mount_query_builder_type(this, query);
    corto_string time = influxdb_Mount_query_builder_time(this, query);

    if ((strlen(type) > 0) && (strlen(time) > 0)) {
        where = corto_asprintf("%s AND %s", type, time);
    } else {
        where = corto_asprintf("%s%s", type, time);
    }

    SAFE_DEALLOC(time)
    SAFE_DEALLOC(type)

    return where;
}

corto_string influxdb_Mount_query_builder_type(
    influxdb_Mount this,
    corto_query *query)
{
    corto_string type = NULL;

    return type;
}

corto_string influxdb_Mount_query_builder_time(
    influxdb_Mount this,
    corto_query *query)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    corto_string timeLimit = NULL;

    if (query->timeBegin.kind == CORTO_FRAME_NOW) {
        if (query->timeBegin.kind == CORTO_FRAME_TIME) {
            corto_time t = corto_frame_getTime(&query->timeBegin);
            timeLimit = corto_asprintf(" WHERE time > %ds", t.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
            corto_time t = corto_frame_getTime(&query->timeEnd);
            timeLimit = corto_asprintf(" WHERE time > (now() - %ds)", t.sec);
        }
    } else if (query->timeBegin.kind == CORTO_FRAME_TIME) {
        corto_time from = corto_frame_getTime(&query->timeBegin);
        if (query->timeEnd.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&query->timeEnd);
            timeLimit = corto_asprintf(" WHERE time < %ds AND time > %ds",
                from.sec, to.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
            corto_time to = corto_frame_getTime(&query->timeEnd);
            timeLimit = corto_asprintf(" WHERE time < %ds AND time > %ds",
                from.sec, from.sec - to.sec);
        }
    }

    if (timeLimit) {
        corto_buffer_appendstr(&buffer, timeLimit);
        corto_dealloc(timeLimit);
    }

    return corto_buffer_str(&buffer);
}

corto_string influxdb_Mount_query_builder_order(
    influxdb_Mount this,
    corto_query *query)
{
    corto_string order = NULL;

    order = corto_asprintf(" ORDER BY time DESC");

    return order;
}

corto_string influxdb_Mount_query_builder_paginate(
    influxdb_Mount this,
    corto_query *query)
{
    corto_string paginate = NULL;

    corto_string limit = influxdb_Mount_query_builder_limit(this, query);
    corto_string slimit = influxdb_Mount_query_builder_slimit(this, query);
    corto_string offset = influxdb_Mount_query_builder_offset(this, query);
    corto_string soffset = influxdb_Mount_query_builder_soffset(this, query);


    paginate = corto_asprintf("%s%s%s%s", limit, offset, slimit, soffset);

    SAFE_DEALLOC(limit)
    SAFE_DEALLOC(slimit)
    SAFE_DEALLOC(offset)
    SAFE_DEALLOC(soffset)

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
corto_string influxdb_Mount_query_builder_limit(
    influxdb_Mount this,
    corto_query *query)
{
    corto_string limit = NULL;

    /* CORTO_FRAME_DEPTH = a number of samples. This is a relative sample
     * offset. So for example, give me the last 10 samples starting from NOW
     */
     if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
         if (query->timeEnd.value > 0) {
             limit = corto_asprintf(" LIMIT %lld", query->timeEnd.value);
         } else {
             limit = corto_asprintf("");
         }
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
corto_string influxdb_Mount_query_builder_slimit(
    influxdb_Mount this,
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
corto_string influxdb_Mount_query_builder_offset(
    influxdb_Mount this,
    corto_query *query)
{
    corto_string offset = NULL;

    /* INFLUXDB: The OFFSET clause requires a LIMIT clause. Using the OFFSET clause
     * without a LIMIT clause can cause inconsistent query results.
     *
     * CORTO_FRAME_SAMPLE = an absolute sample identifier, starting from the
     * first sample written (first sample written for a series is # 0)
     */

     if (query->timeBegin.kind == CORTO_FRAME_DEPTH) {
         if (query->timeBegin.value > 0) {
             offset = corto_asprintf(" OFFSET %lld", query->timeBegin.value);
         } else {
             offset = corto_asprintf("");
         }
     }

    return offset;
}

/* Objects */
corto_string influxdb_Mount_query_builder_soffset(
    influxdb_Mount this,
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
