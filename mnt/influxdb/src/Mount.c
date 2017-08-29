
#include <driver/mnt/influxdb/influxdb.h>

int16_t influxdb_Mount_construct(
    influxdb_Mount this)
{
    corto_string url, query;

    corto_observer(this)->mask = CORTO_ON_TREE;
    if (corto_mount_setContentTypeIn(this, "text/influxdb")) {
        corto_error("Failed to set content type in.");
        goto error;
    }

    if (corto_mount_setContentTypeOut(this, "text/json")) {
        corto_error("Failed to set content type out.");
        goto error;
    }

    /* Make sure that database exists */
    corto_asprintf(&url, "%s/query", this->host);
    corto_asprintf(&query, "q=CREATE DATABASE %s", this->db);
    web_client_post(url, query);
    corto_dealloc(url);
    corto_dealloc(query);
    return corto_super_construct(this);
error:
    return -1;
}

void influxdb_Mount_onNotify(
    influxdb_Mount this,
    corto_subscriberEvent *event)
{
    corto_string url;

    corto_asprintf(&url, "%s/write?db=%s", this->host, this->db);
    corto_trace("influxdb: %s: POST %s", url, corto_result_getText(&event->data));
    web_client_post(url, corto_result_getText(&event->data));
    corto_dealloc(url);
}

corto_resultIter influxdb_Mount_onQuery(
    influxdb_Mount this,
    corto_query *query)
{
    /* TODO !! */
    // curl -GET 'http://localhost:8086/query?pretty=true'
    //    --data-urlencode "db=mydb" --data-urlencode "q=SELECT value FROM cpu_load_short WHERE region='us-west'"
    corto_id from;
    corto_id idFilter = {'\0'};
    corto_id timeLimit = {'\0'};
    corto_id depthLimit = {'\0'};
    corto_id sampleLimit = {'\0'};

    /* Create id filter */
    if (strcmp(query->select, "*")) {
        sprintf(idFilter, "id = '%s'", query->select);
    }

    /* Use path as measurement */
    sprintf(from, "%s/%s", corto_idof(corto_mount(this)->mount), query->from);
    corto_cleanpath(from, from);
    if (query->timeBegin.kind == CORTO_FRAME_NOW) {
        if (query->timeBegin.kind == CORTO_FRAME_TIME) {
            corto_time t = corto_frame_getTime(&query->timeBegin);
            sprintf(timeLimit, "WHERE time > %ds", t.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
            corto_time t = corto_frame_getTime(&query->timeEnd);
            sprintf(timeLimit, "WHERE time > (now() - %ds)", t.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_SAMPLE) {
            if (query->timeEnd.value) {
                sprintf(sampleLimit, "OFFSET %ld", query->timeEnd.value);
            }

        } else if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", query->timeEnd.value);
        }

    } else if (query->timeBegin.kind == CORTO_FRAME_TIME) {
        corto_time from = corto_frame_getTime(&query->timeBegin);
        if (query->timeEnd.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&query->timeEnd);
            sprintf(timeLimit, "WHERE time < %ds AND time > %ds", from.sec, to.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
            corto_time to = corto_frame_getTime(&query->timeEnd);
            sprintf(timeLimit, "WHERE time < %ds AND time > %ds",
              from.sec,
              from.sec - to.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_SAMPLE) {
            sprintf(timeLimit, "WHERE time < %ds", from.sec);
            if (query->timeEnd.value) {
                sprintf(sampleLimit, "OFFSET %ld", query->timeEnd.value);
            }

        } else if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
            sprintf(timeLimit, "WHERE time < %ds", from.sec);
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", query->timeEnd.value);
        }

    } else if (query->timeBegin.kind == CORTO_FRAME_SAMPLE) {
        if (query->timeBegin.value) {
            sprintf(sampleLimit, "OFFSET %ld", query->timeBegin.value);
        }

        if (query->timeEnd.kind == CORTO_FRAME_TIME) {
            corto_time to = corto_frame_getTime(&query->timeEnd);
            sprintf(timeLimit, "WHERE time < %ds", to.sec);
        } else if (query->timeEnd.kind == CORTO_FRAME_DURATION) {
            /* Unsupported */
        } else if (query->timeEnd.kind == CORTO_FRAME_SAMPLE) {
            if (query->timeEnd.value) {
                sprintf(depthLimit, "LIMIT %ld", query->timeEnd.value - query->timeBegin.value);
            }

        } else if (query->timeEnd.kind == CORTO_FRAME_DEPTH) {
            sprintf(depthLimit, "ORDER BY time DESC LIMIT %ld", query->timeEnd.value);
        }

    }

    /*corto_asprintf(&query, "SELECT * FROM %s %s %s %s",
        from,
        timeLimit,
        depthLimit,
        sampleLimit);*/
    return CORTO_ITER_EMPTY; /* Using corto_mount_return */
}

void influxdb_Mount_onBatchNotify(
    influxdb_Mount this,
    corto_subscriberEventIter data)
{
    corto_error("onBatchNotify not implemented.");
}
