#include <driver/mnt/influxdb/influxdb.h>

int16_t influxdb_Mount_construct(
    influxdb_Mount this)
{
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
    corto_string url = corto_asprintf("%s/query", this->host);
    corto_string query = corto_asprintf("q=CREATE DATABASE %s", this->db);
    httpclient_post(url, query);
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
    corto_string url = corto_asprintf("%s/write?db=%s", this->host, this->db);
    corto_trace("influxdb: %s: POST %s", url, corto_result_getText(&event->data));
    httpclient_post(url, corto_result_getText(&event->data));
    corto_dealloc(url);
}


// corto_string select;
// corto_string from;
// corto_string type;
// corto_string member;
// corto_string where;

// struct corto_mount_s {
//     struct corto_subscriber_s super;
//     corto_mountPolicy policy;
//     corto_object mount;
//     corto_attr attr;
//     corto_mountStats sent;
//     corto_mountStats received;
//     corto_mountStats sentDiscarded;
//     corto_mountSubscriptionList subscriptions;
//     corto_objectlist events;
//     corto_objectlist historicalEvents;
//     corto_time lastPoll;
//     corto_time lastPost;
//     corto_time lastSleep;
//     corto_time dueSleep;
//     uint32_t lastQueueSize;
//     bool passThrough;
//     bool explicitResume;
//     uintptr_t thread;
//     bool quit;
//     corto_string contentTypeOut;
//     uintptr_t contentTypeOutHandle;
// };

// struct corto_subscriber_s {
//     struct corto_observer_s super;
//     corto_query query;
//     corto_string contentType;
//     uintptr_t contentTypeHandle;
//     uintptr_t idmatch;
// };


corto_resultIter influxdb_Mount_onQuery(
    influxdb_Mount this,
    corto_query *query)
{
    corto_info("MOUNT_FROM [%s]", this->super.super.query.from);
    corto_info("SELECT [%s]", query->select);
    corto_info("FROM [%s]", query->from);
    corto_info("TYPE [%s]", query->type);
    corto_info("MEMBER [%s]", query->member);
    corto_info("WHERE [%s]", query->where);

    corto_buffer buffer = CORTO_BUFFER_INIT;

    /* TODO !! */
    // curl -GET 'http://localhost:8086/query?pretty=true'
    //    --data-urlencode "db=mydb" --data-urlencode "q=SELECT value FROM cpu_load_short WHERE region='us-west'"
    // corto_id timeLimit = {'\0'};
    // corto_id depthLimit = {'\0'};
    // corto_id sampleLimit = {'\0'};
    /* Create id filter */

    /* Build SELECT Data Fields (members) */
    corto_buffer_appendstr(&buffer, " ");
    if (strcmp(query->select, "*") != 0) {
        // sprintf(idFilter, "id = '%s'", query->select);
        //TODO Handle Select targets
        corto_buffer_appendstr(&buffer, query->select);
    }
    else {
        corto_buffer_appendstr(&buffer, query->select);
    }

    /* FROM */
    corto_string mountFrom = this->super.super.query.from;
    if (strcmp(query->from, ".") != 0) {
        corto_string from = corto_asprintf(" FROM \"%s\".\"autogen\".\"%s/%s\"",
            this->db, mountFrom, query->from);
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
        corto_string from = corto_asprintf(" FROM \"%s\".\"autogen\".\"%s\"",
            this->db, mountFrom);
        if (from) {
            corto_buffer_appendstr(&buffer, from);
            corto_dealloc(from);
        }
        else {
            corto_seterr("Failed to create InfluxDB FROM statement.");
            goto error;
        }
    }

    /* WHERE */
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

    /* Publish Query */
    corto_string bufferStr = corto_buffer_str(&buffer);
    char *encodedBuffer = httpclient_encode_fields(bufferStr);
    corto_string queryStr = corto_asprintf("q=SELECT%s", encodedBuffer);
    corto_dealloc(encodedBuffer);

    corto_string url = corto_asprintf("%s/query?db=%s", this->host, this->db);
    corto_trace("influxdb: %s: GET %s", url, queryStr);
    httpclient_Result result = httpclient_get(url, queryStr);

    corto_info("GET Result STATUS [%d] RESPONSE [%s]", result.status, result.response);

    corto_dealloc(url);
    corto_dealloc(bufferStr);
    corto_dealloc(queryStr);

    return CORTO_ITER_EMPTY; /* Using corto_mount_return */
error:
    corto_error("InfluxDB Query Failed. Corto Error: [%s]", corto_lasterr());
    return CORTO_ITER_EMPTY;
}

void influxdb_Mount_onBatchNotify(
    influxdb_Mount this,
    corto_subscriberEventIter events)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    while (corto_iter_hasNext(&events)) {
        corto_subscriberEvent *e = corto_iter_next(&events);

        corto_buffer_appendstr(&buffer, corto_result_getText(&e->data));
        if (corto_iter_hasNext(&events) != 0)
        {
            corto_buffer_appendstr(&buffer, "\n");
        }
    }

    corto_string bufferStr = corto_buffer_str(&buffer);

    corto_string url = corto_asprintf("%s/write?db=%s", this->host, this->db);
    corto_trace("influxdb: %s: POST %s", url, bufferStr);
    httpclient_post(url, bufferStr);

    corto_dealloc(url);
    corto_dealloc(bufferStr);
}

void influxdb_Mount_onHistoryBatchNotify(
    influxdb_Mount this,
    corto_subscriberEventIter events)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    while (corto_iter_hasNext(&events)) {
        corto_subscriberEvent *e = corto_iter_next(&events);

        corto_buffer_appendstr(&buffer, corto_result_getText(&e->data));
        if (corto_iter_hasNext(&events) != 0)
        {
            corto_buffer_appendstr(&buffer, "\n");
        }
    }

    corto_string bufferStr = corto_buffer_str(&buffer);

    corto_string url = corto_asprintf("%s/write?db=%s", this->host, this->db);
    corto_trace("influxdb: %s: POST %s", url, bufferStr);
    httpclient_post(url, bufferStr);

    corto_dealloc(url);
    corto_dealloc(bufferStr);
}
