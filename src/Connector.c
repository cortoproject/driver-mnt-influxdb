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
    corto_observer(this)->mask = CORTO_ON_TREE;
    if (corto_mount_setContentTypeIn(this, "text/influx")) {
        goto error;
    }
    if (corto_mount_setContentTypeOut(this, "text/json")) {
        goto error;
    }

    /* Make sure that database exists */
    corto_asprintf(&url, "%s/query", this->host);
    corto_asprintf(&query, "q=CREATE DATABASE %s", this->db);
    web_client_post(url, query);
    corto_dealloc(url);
    corto_dealloc(query);

    return corto_mount_construct(this);
error:
    return -1;
/* $end */
}

corto_void _influxdb_Connector_onNotify(
    influxdb_Connector this,
    corto_eventMask event,
    corto_result *object)
{
/* $begin(influxdb/Connector/onNotify) */
    corto_string url;

    corto_asprintf(&url, "%s/write?db=%s", this->host, this->db);
    corto_trace("influxdb: %s: POST %s", url, (corto_string)object->value);
    web_client_post(url, (corto_string)object->value);
    corto_dealloc(url);

/* $end */
}

corto_resultIter _influxdb_Connector_onRequest(
    influxdb_Connector this,
    corto_request *request)
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
    if (strcmp(request->expr, "*")) {
        sprintf(idFilter, "id = '%s'", request->expr);
    }

    /* Use path as measurement */
    sprintf(from, "%s/%s", corto_idof(corto_mount(this)->mount), request->parent);
    corto_cleanpath(from, from);

    if (request->from.kind == CORTO_FRAME_NOW) {
        if (request->to.kind == CORTO_FRAME_TIME) {
            corto_time t = corto_frame_getTime(&request->to);
            sprintf(timeLimit, "WHERE time > %ds", t.sec);
        } else if (request->to.kind == CORTO_FRAME_DURATION) {
            corto_time t = corto_frame_getTime(&request->to);
            sprintf(timeLimit, "WHERE time > (now() - %ds)", t.sec);
        } else if (request->to.kind == CORTO_FRAME_SAMPLE) {
            if (request->to.value) {
                sprintf(sampleLimit, "OFFSET %ld", request->to.value);
            }
        } else if (request->to.kind == CORTO_FRAME_DEPTH) {
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", request->to.value);
        }
    } else if (request->from.kind == CORTO_FRAME_TIME) {
        corto_time from = corto_frame_getTime(&request->from);
        if (request->to.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&request->to);
            sprintf(timeLimit, "WHERE time < %ds AND time > %ds", from.sec, to.sec);
        } else if (request->to.kind == CORTO_FRAME_DURATION) {
            corto_time to = corto_frame_getTime(&request->to);
            sprintf(timeLimit, "WHERE time < %ds AND time > %ds",
              from.sec,
              from.sec - to.sec);
        } else if (request->to.kind == CORTO_FRAME_SAMPLE) {
            sprintf(timeLimit, "WHERE time < %ds", from.sec);
            if (request->to.value) {
                sprintf(sampleLimit, "OFFSET %ld", request->to.value);
            }
        } else if (request->to.kind == CORTO_FRAME_DEPTH) {
            sprintf(timeLimit, "WHERE time < %ds", from.sec);
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", request->to.value);
        }
    } else if (request->from.kind == CORTO_FRAME_SAMPLE) {
        if (request->from.value) {
            sprintf(sampleLimit, "OFFSET %ld", request->from.value);
        }
        if (request->to.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&request->to);
            sprintf(timeLimit, "WHERE time < %ds", to.sec);
        } else if (request->to.kind == CORTO_FRAME_DURATION) {
            /* Unsupported */
        } else if (request->to.kind == CORTO_FRAME_SAMPLE) {
            if (request->to.value) {
                sprintf(depthLimit, "LIMIT %ld", request->to.value - request->from.value);
            }
        } else if (request->to.kind == CORTO_FRAME_DEPTH) {
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", request->to.value);
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
