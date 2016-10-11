/* $CORTO_GENERATED
 *
 * Connector.c
 *
 * Only code written between the begin and end tags will be preserved
 * when the file is regenerated.
 */

#include <influxdb/influxdb.h>

corto_int16 _influxdb_Connector_construct(
    influxdb_Connector this)
{
/* $begin(influxdb/Connector/construct) */
    corto_string url, query;

    corto_mount(this)->kind = CORTO_HISTORIAN;
    corto_setstr(&corto_mount(this)->contentType, "text/json");
    corto_mount(this)->mask = CORTO_ON_TREE;

    /* Make sure that database exists */
    corto_asprintf(&url, "%s/query", this->host);
    corto_asprintf(&query, "q=CREATE DATABASE %s", this->db);
    web_client_post(url, query);
    corto_dealloc(url);
    corto_dealloc(query);

    return corto_mount_construct(this);
/* $end */
}

corto_resultIter _influxdb_Connector_onRequest(
    influxdb_Connector this,
    corto_request *r)
{
/* $begin(influxdb/Connector/onRequest) */
    /* TODO !! */
    // curl -GET 'http://localhost:8086/query?pretty=true'
    //    --data-urlencode "db=mydb" --data-urlencode "q=SELECT value FROM cpu_load_short WHERE region='us-west'"
    corto_id from;
    corto_id idFilter = {'\0'};
    corto_id timeLimit = {'\0'};
    corto_id depthLimit = {'\0'};
    corto_id sampleLimit = {'\0'};
    corto_string query;

    /* Create id filter */
    if (strcmp(r->expr, "*")) {
        sprintf(idFilter, "id = '%s'", r->expr);
    }

    /* Use path as measurement */
    sprintf(from, "%s/%s", corto_idof(corto_mount(this)->mount), r->parent);
    corto_cleanpath(from, from);

    if (r->from.kind == CORTO_FRAME_NOW) {
        if (r->to.kind == CORTO_FRAME_TIME) {
            corto_time t = corto_frame_getTime(&r->to);
            sprintf(timeLimit, "WHERE time > %ds", t.sec);
        } else if (r->to.kind == CORTO_FRAME_DURATION) {
            corto_time t = corto_frame_getTime(&r->to);
            sprintf(timeLimit, "WHERE time > (now() - %ds)", t.sec);
        } else if (r->to.kind == CORTO_FRAME_SAMPLE) {
            if (r->to.value) {
                sprintf(sampleLimit, "OFFSET %ld", r->to.value);
            }
        } else if (r->to.kind == CORTO_FRAME_DEPTH) {
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", r->to.value);
        }
    } else if (r->from.kind == CORTO_FRAME_TIME) {
        corto_time from = corto_frame_getTime(&r->from);
        if (r->to.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&r->to);
            sprintf(timeLimit, "WHERE time < %ds AND time > %ds", from.sec, to.sec);
        } else if (r->to.kind == CORTO_FRAME_DURATION) {
            corto_time to = corto_frame_getTime(&r->to);
            sprintf(timeLimit, "WHERE time < %ds AND time > %ds",
              from.sec,
              from.sec - to.sec);
        } else if (r->to.kind == CORTO_FRAME_SAMPLE) {
            sprintf(timeLimit, "WHERE time < %ds", from.sec);
            if (r->to.value) {
                sprintf(sampleLimit, "OFFSET %ld", r->to.value);
            }
        } else if (r->to.kind == CORTO_FRAME_DEPTH) {
            sprintf(timeLimit, "WHERE time < %ds", from.sec);
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", r->to.value);
        }
    } else if (r->from.kind == CORTO_FRAME_SAMPLE) {
        if (r->from.value) {
            sprintf(sampleLimit, "OFFSET %ld", r->from.value);
        }
        if (r->to.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&r->to);
            sprintf(timeLimit, "WHERE time < %ds", to.sec);
        } else if (r->to.kind == CORTO_FRAME_DURATION) {
            /* Unsupported */
        } else if (r->to.kind == CORTO_FRAME_SAMPLE) {
            if (r->to.value) {
                sprintf(depthLimit, "LIMIT %ld", r->to.value - r->from.value);
            }
        } else if (r->to.kind == CORTO_FRAME_DEPTH) {
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", r->to.value);
        }
    }

    /*corto_asprintf(&query, "SELECT * FROM %s %s %s %s",
        from,
        timeLimit,
        depthLimit,
        sampleLimit);*/

    return CORTO_ITERATOR_EMPTY;
/* $end */
}

corto_void _influxdb_Connector_onUpdate(
    influxdb_Connector this,
    corto_object observable)
{
/* $begin(influxdb/Connector/onUpdate) */
    corto_string url, content;

    corto_asprintf(&url, "%s/write?db=%s", this->host, this->db);
    content = corto_contentof(NULL, "text/influx", observable);
    corto_trace("influxdb: %s: POST %s", url, content);
    web_client_post(url, content);
    corto_dealloc(url);

/* $end */
}
