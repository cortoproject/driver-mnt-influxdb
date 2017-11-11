#include <driver/mnt/influxdb/influxdb.h>
#include <driver/mnt/influxdb/query_builder.h>
#include <driver/mnt/influxdb/query_response.h>

const corto_string INFLUXDB_QUERY_EPOCH = "ns";

#define SAFE_DEALLOC(p)if (p){ corto_dealloc(p); p = NULL; }

bool influxdb_Mount_filterEvent(corto_string type);
corto_string influxdb_Mount_notifySample(corto_subscriberEvent *event);
corto_string influxdb_safeString(corto_string source);
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
    if (influxdb_Mount_filterEvent(event->data.type)) {
        return;
    }

    corto_string sample = influxdb_Mount_notifySample(event);
    if (sample == NULL) {
        corto_seterr("Failed to build udpate sample. Error: %s",
            corto_lasterr());
        return;
    }

    corto_string url = influxdb_Mount_query_builder_url(this);
    corto_trace("influxdb: %s: POST %s", url, sample);
    httpclient_Result result = httpclient_post(url, sample);
    if (result.status != 204) {
        corto_seterr("InfluxDB Update Failed. Status [%d] Response:\n%s",
            result.status, result.response);
    }
    corto_dealloc(url);
    corto_dealloc(sample);
}

void influxdb_Mount_onBatchNotify(
    influxdb_Mount this,
    corto_subscriberEventIter events)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    while (corto_iter_hasNext(&events)) {
        corto_subscriberEvent *event = corto_iter_next(&events);

        if (influxdb_Mount_filterEvent(event->data.type)) {
            continue;
        }

        corto_string sample = influxdb_Mount_notifySample(event);
        if (sample == NULL) {
            corto_seterr("Failed to build udpate sample. Error: %s",
                corto_lasterr());
            continue;
        }

        corto_buffer_appendstr(&buffer, sample);
        corto_dealloc(sample);
        if (corto_iter_hasNext(&events) != 0) {
            corto_buffer_appendstr(&buffer, "\n");
        }

    }

    corto_string bufferStr = corto_buffer_str(&buffer);
    corto_string url = influxdb_Mount_query_builder_url(this);
    corto_trace("influxdb: %s: POST %s", url, bufferStr);
    httpclient_Result result = httpclient_post(url, bufferStr);
    if (result.status != 204) {
        corto_seterr("InfluxDB Update Failed. Status [%d] Response:\n%s",
            result.status, result.response);
    }

    corto_dealloc(url);
    corto_dealloc(bufferStr);
}

void influxdb_Mount_onHistoryBatchNotify(
    influxdb_Mount this,
    corto_subscriberEventIter events)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    while (corto_iter_hasNext(&events)) {
        corto_subscriberEvent *event = corto_iter_next(&events);

        if (influxdb_Mount_filterEvent(event->data.type)) {
            continue;
        }

        corto_string sample = influxdb_Mount_notifySample(event);
        if (sample == NULL) {
            corto_seterr("Failed to build udpate sample. Error: %s",
                corto_lasterr());
            continue;
        }

        corto_buffer_appendstr(&buffer, sample);
        corto_dealloc(sample);
        if (corto_iter_hasNext(&events) != 0) {
            corto_buffer_appendstr(&buffer, "\n");
        }

    }

    corto_string bufferStr = corto_buffer_str(&buffer);
    corto_string url = influxdb_Mount_query_builder_url(this);
    corto_trace("influxdb: %s: POST %s", url, bufferStr);
    httpclient_Result result = httpclient_post(url, bufferStr);
    if (result.status != 204) {
        corto_seterr("InfluxDB Update Failed. Status [%d] Response:\n%s",
            result.status, result.response);
    }

    corto_dealloc(url);
    corto_dealloc(bufferStr);
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
        corto_error("Failed to create InfluxDB FROM statement. Error %s",
            corto_lasterr());
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

    /* LIMITS (limit & slimit) + OFFSETS (offset + soffset) */
    corto_string paginate = influxdb_Mount_query_builder_paginate(this, query);
    if (paginate) {
        corto_buffer_appendstr(&buffer, paginate);
        corto_dealloc(paginate);
    }

    /* Publish Query */
    corto_string bufferStr = corto_buffer_str(&buffer);
    char *encodedBuffer = httpclient_encode_fields(bufferStr);
    corto_string queryStr = corto_asprintf("epoch=%s&q=SELECT%s",
        INFLUXDB_QUERY_EPOCH, encodedBuffer);
    corto_dealloc(encodedBuffer);
    corto_string url = corto_asprintf("%s/query?db=%s", this->host, this->db);
    corto_trace("Fields to be decoded [%s]", bufferStr);
    corto_trace("influxdb: %s: GET %s", url, queryStr);
    httpclient_Result result = httpclient_get(url, queryStr);
    corto_dealloc(url);
    corto_dealloc(bufferStr);
    corto_dealloc(queryStr);
    influxdb_Mount_query_response_handler(this, &result, historical);
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

    timeBegin
    timeEnd
    struct corto_frame {
    corto_frameKind kind;
    int64_t value;
};
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
    corto_string sample = NULL;
    /* Map measurement & tag to parent and id
     * Format: measurement(path),type dataFields
     */
    corto_string parent = NULL;
    corto_string id = influxdb_safeString(event->data.id);
    corto_string t = event->data.type;
    corto_string r = corto_result_getText(&event->data);

    if (strcmp(".", event->data.parent) == 0) {
        sample = corto_asprintf("%s,type=%s %s", id, t, r);
    }

    else {
        parent = influxdb_safeString(event->data.parent);
        sample = corto_asprintf("%s/%s,type=%s %s", parent, id, t, r);
        SAFE_DEALLOC(parent)
    }

    SAFE_DEALLOC(id)
    return sample;
}

corto_string influxdb_safeString(corto_string source)
{
    /* Measurements and Tags names cannot contain non-espaced spaces */
    return corto_replace(source, " ", "\\ ");
}

corto_string influxdb_Mount_retentionPolicy(
    influxdb_Mount this)
{
    if (!this->rp) {
        return "autogen";
    }

    return this->rp->name;
}
