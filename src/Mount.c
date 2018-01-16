#include <driver/mnt/influxdb/influxdb.h>
#include <driver/mnt/influxdb/query_builder.h>
#include <driver/mnt/influxdb/query_response.h>
#include <corto/string.h>

const corto_string INFLUXDB_QUERY_EPOCH = "ns";

#define SAFE_DEALLOC(p)if (p){ corto_dealloc(p); p = NULL; }

bool influxdb_Mount_filterEvent(corto_string type);
corto_string influxdb_Mount_notifySample(corto_subscriberEvent *event);
void influxdb_safeString(corto_buffer *b, corto_string source);

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

    if (this->udp) {
        /* Verify UDP Socket connection has been initialized. */
        if (this->udp->socket <= 0) {
            if (influxdb_UdpConn_construct(this->udp)) {
                corto_throw("Failed to Initialize UDP Connection.");
                goto error;
            }
        }
    }

    /* Make sure that database exists */
    corto_string url = corto_asprintf("http://%s:%d/query",
        this->host, this->port);
    corto_string query = corto_asprintf("q=CREATE DATABASE %s", this->db);
    httpclient_Result result = httpclient_post(url, query);
    SAFE_DEALLOC(url);
    SAFE_DEALLOC(query);

    if (result.status != 200) {
        corto_error("InfluxDB Create database Status [%d] Response [%s].",
            result.status, result.response);
        SAFE_DEALLOC(result.response)
        goto error;
    }

    SAFE_DEALLOC(result.response)
    return corto_super_construct(this);
error:
    return -1;
}

void influxdb_Mount_onNotify(
    influxdb_Mount this,
    corto_subscriberEvent *event)
{
    if (influxdb_Mount_filterEvent(event->data.type)) {
        return;
    }

    corto_string sample = influxdb_Mount_notifySample(event);
    if (sample == NULL) {
        corto_throw("Failed to build udpate sample.");
        return;
    }

    if (this->udp) {
        influxdb_UdpConn_send(this->udp, sample);
    } else {
        corto_string url = influxdb_Mount_query_builder_url(this);
        corto_trace("influxdb NOTIFY: %s: POST %s", url, sample);
        httpclient_Result result = httpclient_post(url, sample);
        if (result.status != 204) {
            corto_error("InfluxDB Update Failed. Status [%d] Response: %s",
                result.status, result.response);
        }
        corto_dealloc(url);
        SAFE_DEALLOC(result.response)
    }

    corto_dealloc(sample);
}

void influxdb_Mount_onBatchNotify(
    influxdb_Mount this,
    corto_subscriberEventIter events)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;
    size_t bufferSize = 0;

    while (corto_iter_hasNext(&events)) {
        corto_subscriberEvent *event = corto_iter_next(&events);

        if (influxdb_Mount_filterEvent(event->data.type)) {
            continue;
        }

        corto_string sample = influxdb_Mount_notifySample(event);
        if (sample == NULL) {
            corto_throw("Failed to build udpate sample.");
            continue;
        }

        if (this->udp) {
            /* UDP is enabled. */
            if (influxdb_UdpConn_write(
                this->udp,
                sample,
                (uintptr_t)&buffer,
                (uintptr_t)&bufferSize,
                corto_iter_hasNext(&events)
            )) {
                corto_throw("Failed to write UDP sample.");
            }
        } else {
            corto_buffer_appendstr(&buffer, sample);
            if (corto_iter_hasNext(&events) != 0) {
                corto_buffer_appendstr(&buffer, "\n");
            }
        }
        corto_dealloc(sample);
    }

    corto_string bufferStr = corto_buffer_str(&buffer);

    if (this->udp) {
        influxdb_UdpConn_send(this->udp, bufferStr);
    } else {
        corto_string url = influxdb_Mount_query_builder_url(this);
        corto_trace("influxdb BATCH NOTIFY: %s: POST %s", url, bufferStr);

        httpclient_Result result = httpclient_post(url, bufferStr);
        if (result.status != 204) {
            corto_throw("InfluxDB Update Failed. Status [%d] Response: %s",
                result.status, result.response);
        }

        SAFE_DEALLOC(url);
        SAFE_DEALLOC(result.response)
    }
    SAFE_DEALLOC(bufferStr);
}

void influxdb_Mount_onHistoryBatchNotify(
    influxdb_Mount this,
    corto_subscriberEventIter events)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;
    size_t bufferSize = 0;

    while (corto_iter_hasNext(&events)) {
        corto_subscriberEvent *event = corto_iter_next(&events);

        if (influxdb_Mount_filterEvent(event->data.type)) {
            continue;
        }
        corto_string sample = influxdb_Mount_notifySample(event);
        if (sample == NULL) {
            corto_throw("Failed to build udpate sample.");
            continue;
        }

        if (this->udp) {
            /* UDP is enabled. */
            if (influxdb_UdpConn_write(
                this->udp,
                sample,
                (uintptr_t)&buffer,
                (uintptr_t)&bufferSize,
                corto_iter_hasNext(&events)
            )) {
                corto_throw("Failed to write UDP sample.");
            }
        } else {
            corto_buffer_appendstr(&buffer, sample);
            if (corto_iter_hasNext(&events) != 0) {
                corto_buffer_appendstr(&buffer, "\n");
            }
        }
        corto_dealloc(sample);
    }

    corto_string bufferStr = corto_buffer_str(&buffer);

    if (this->udp) {
        influxdb_UdpConn_send(this->udp, bufferStr);
    } else {
        corto_string url = influxdb_Mount_query_builder_url(this);
        corto_trace("influxdb HISTORY BATCH NOTIFY: %s: POST %s", url, bufferStr);

        httpclient_Result result = httpclient_post(url, bufferStr);
        if (result.status != 204) {
            corto_error("InfluxDB HistoryBatchNotify [%d] Response [%s].",
                result.status, result.response);
        }

        SAFE_DEALLOC(url);
        SAFE_DEALLOC(result.response)
    }

    SAFE_DEALLOC(bufferStr);
}

corto_resultIter influxdb_Mount_onQueryExecute(
    influxdb_Mount this,
    corto_query *query,
    bool historical)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;
    corto_buffer_appendstr(&buffer, " ");

    /* Build SELECT Data Fields (members) */
    corto_string select = influxdb_Mount_query_builder_select(this, query);
    if (select) {
        corto_buffer_appendstr(&buffer, select);
        corto_dealloc(select);
    }

    /* FROM */
    corto_string from = influxdb_Mount_query_builder_from(this, query);
    if (from) {
        corto_buffer_appendstr(&buffer, from);
        corto_dealloc(from);
    }

    else {
        corto_error("Failed to create InfluxDB FROM statement.");
    }

    /* WHERE */
    corto_string where = influxdb_Mount_query_builder_where(this, query);
    if (where) {
        corto_buffer_appendstr(&buffer, where);
        corto_dealloc(where);
    }

    /* ORDER */
    corto_string order = influxdb_Mount_query_builder_order(this, query);
    if (order) {
        corto_buffer_appendstr(&buffer, order);
        corto_dealloc(order);
    }

    /* LIMITS and Offsets */
    corto_string paginate = influxdb_Mount_query_builder_paginate(
        this, query, historical);
    if (paginate) {
        corto_buffer_appendstr(&buffer, paginate);
        corto_dealloc(paginate);
    }


    influxdb_Mount_ResonseFilter filter =
        { historical, query->limit, query->offset };

    /* Publish Query */
    corto_string bufferStr = corto_buffer_str(&buffer);
    char *encodedBuffer = httpclient_encodeFields(bufferStr);
    corto_string queryStr = corto_asprintf("epoch=%s&q=SELECT%s",
        INFLUXDB_QUERY_EPOCH, encodedBuffer);
    corto_dealloc(encodedBuffer);
    corto_string url = corto_asprintf("http://%s:%d/query?db=%s",
        this->host, this->port, this->db);
    corto_trace("Fields to be decoded [%s]", bufferStr);
    corto_trace("influxdb: %s: GET %s", url, queryStr);
    httpclient_Result result = httpclient_get(url, queryStr);
    corto_dealloc(url);
    corto_dealloc(bufferStr);
    corto_dealloc(queryStr);
    influxdb_Mount_query_response_handler(this, &result, &filter);
    SAFE_DEALLOC(result.response)
    return CORTO_ITER_EMPTY; /* Using corto_mount_return */
}

corto_resultIter influxdb_Mount_onQuery(
    influxdb_Mount this,
    corto_query *query)
{
    return influxdb_Mount_onQueryExecute(this, query, false);
}

corto_resultIter influxdb_Mount_onHistoryQuery(
    influxdb_Mount this,
    corto_query *query)
{
    corto_info("TimeBegin [%d] [%lld]", query->timeBegin.kind, query->timeBegin.value);
    corto_info("TimeEnd [%d] [%lld]", query->timeEnd.kind, query->timeBegin.value);
    /* Uncomment to debug queries
    corto_info("MOUNT_FROM [%s]", this->super.super.query.from);
    corto_info("SELECT [%s]", query->select);
    corto_info("FROM [%s]", query->from);
    corto_info("TYPE [%s]", query->type);
    corto_info("MEMBER [%s]", query->member);
    corto_info("WHERE [%s]", query->where);
    corto_info("LIMIT [%llu]", query->limit);
    corto_info("OFFSET [%llu]", query->offset);
    */
    corto_info("Mount From [%s] Select [%s] From [%s] Type [%s] Member " \
        "[%s] Where [%s] LIMIT [%llu] OFFSET [%llu]",
        this->super.super.query.from,
        query->select,
        query->from,
        query->type,
        query->member,
        query->where,
        query->limit,
        query->offset);

    return influxdb_Mount_onQueryExecute(this, query, true);
}

bool influxdb_Mount_filterEvent(corto_string type)
{
    /* Ignore Void Objets */
    if (strcmp(type, "void") == 0) {
        return true;
    }

    return false;
}

corto_string influxdb_Mount_notifySample(corto_subscriberEvent *event)
{
    corto_buffer b = CORTO_BUFFER_INIT;
    /* Map measurement & tag to parent and id
     * Format: measurement(path),type dataFields
     */

    if (strcmp(".", event->data.parent) == 0) {
        /* Optimization for "%s,type=%s %s", id, t, r); */
        influxdb_safeString(&b, event->data.id);
        corto_buffer_appendstrn(&b, ",type=", 6);
        corto_buffer_appendstr(&b, event->data.type);
        corto_buffer_appendstrn(&b, " ", 1);
        corto_buffer_appendstr(&b, corto_result_getText(&event->data));
    }
    else {
        /* Optimization for "%s/%s,type=%s %s", parent, id, t, r); */
        influxdb_safeString(&b, event->data.parent);
        corto_buffer_appendstrn(&b, "/", 1);
        influxdb_safeString(&b, event->data.id);
        corto_buffer_appendstrn(&b, ",type=", 6);
        corto_buffer_appendstr(&b, event->data.type);
        corto_buffer_appendstrn(&b, " ", 1);
        corto_buffer_appendstr(&b, corto_result_getText(&event->data));
    }

    return corto_buffer_str(&b);
}

void influxdb_safeString(corto_buffer *b, corto_string source)
{
    /* Measurements and Tags names cannot contain non-espaced spaces */
    char *ptr, ch;
    for (ptr = source; (ch = *ptr); ptr++) {
        if (ch == ' ') {
            corto_buffer_append(b, "\\ ");
        } else {
            corto_buffer_appendstrn(b, ptr, 1);
        }

    }
}

corto_string influxdb_Mount_retentionPolicy(
    influxdb_Mount this)
{
    if (!this->rp) {
        return "autogen";
    }

    return this->rp->name;
}
