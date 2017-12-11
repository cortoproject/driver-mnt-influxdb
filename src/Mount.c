#include <driver/mnt/influxdb/influxdb.h>
#include <driver/mnt/influxdb/query_builder.h>
#include <driver/mnt/influxdb/query_response.h>
#include <corto/string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

///TODO REMOVE
#include <time.h>

// call this function to start a nanosecond-resolution timer
struct timespec timer_start(void){
    struct timespec start_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    return start_time;
}

// call this function to end a timer, returning nanoseconds elapsed as a long
long timer_end(struct timespec start_time){
    struct timespec end_time;
    clock_gettime(CLOCK_MONOTONIC, &end_time);
    long diffInNanos = end_time.tv_nsec - start_time.tv_nsec;
    return diffInNanos/1000;
}
///TODO REMOVE

/* Local Thread Logging Support */
int16_t influxdb_UdpSend(corto_string buffer);
static void influxdb_UdpSocketFree(void *o);
int16_t influxdb_UdpSocketInitialize(corto_string host, int16_t port);
static corto_tls INFLUXDB_MOUNT_KEY_UDP;
typedef struct influxdb_UdpSocket_s {
    int     sfd;
} *influxdb_UdpSocket;

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

    if (this->udpEnable) {
        if (influxdb_UdpSocketInitialize(this->host, this->udpPort)) {
            corto_error("Failed to Initialize UDP Socket. %s", corto_lasterr());
            goto error;
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
        corto_error("InfluxDB Create database Status [%d] Response [%s].\n%s",
            result.status, result.response, corto_lasterr());
        SAFE_DEALLOC(result.response)
        goto error;
    }

    corto_info("InfluxDB created."); //TODO REMOVE

    SAFE_DEALLOC(result.response)
    // return corto_super_construct(this);

    ///TODO REMOVe
    int ret = corto_super_construct(this);

    corto_info("INFLUXDB SUPER CONSTRUCTED.");
    return ret;
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

    corto_string url = influxdb_Mount_query_builder_url(this);
    corto_trace("influxdb NOTIFY: %s: POST %s", url, sample);
    httpclient_Result result = httpclient_post(url, sample);
    if (result.status != 204) {
        corto_error("InfluxDB Update Failed. Status [%d] Response: %s",
            result.status, result.response);
    }
    corto_dealloc(url);
    corto_dealloc(sample);
    SAFE_DEALLOC(result.response)
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
            corto_throw("Failed to build udpate sample.");
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
    corto_trace("influxdb BATCH NOTIFY: %s: POST %s", url, bufferStr);

    httpclient_Result result = httpclient_post(url, bufferStr);
    if (result.status != 204) {
        corto_throw("InfluxDB Update Failed. Status [%d] Response: %s",
            result.status, result.response);
    }

    SAFE_DEALLOC(url);
    SAFE_DEALLOC(bufferStr);
    SAFE_DEALLOC(result.response)
}

void influxdb_Mount_onHistoryBatchNotify(
    influxdb_Mount this,
    corto_subscriberEventIter events)
{
    corto_buffer buffer = CORTO_BUFFER_INIT;

    corto_info("\n\nPROCESS HISTORICAL\n\n");
    ///TODO REMOVE
    struct timespec start = timer_start();
    int cnt = 0;
    ///TODO REMOVE
    while (corto_iter_hasNext(&events)) {
        cnt++;
        corto_subscriberEvent *event = corto_iter_next(&events);

        if (influxdb_Mount_filterEvent(event->data.type)) {
            continue;
        }
        corto_string sample = influxdb_Mount_notifySample(event);
        if (sample == NULL) {
            corto_throw("Failed to build udpate sample.");
            continue;
        }

        corto_buffer_appendstr(&buffer, sample);
        corto_dealloc(sample);
        if (corto_iter_hasNext(&events) != 0) {
            corto_buffer_appendstr(&buffer, "\n");
        }

    }

    long resolve = timer_end(start); //TODO REMOVe
    struct timespec startPost = timer_start(); ///TODO REMOVE

    corto_string bufferStr = corto_buffer_str(&buffer);

    if (this->udpEnable) {
        corto_info("Send Buffer [%s] Size [%zu]", bufferStr, sizeof(bufferStr));
        influxdb_UdpSend(bufferStr);
    } else {
        corto_string url = influxdb_Mount_query_builder_url(this);
        corto_info("influxdb HISTORY BATCH NOTIFY: %s: POST %s", url, bufferStr); //TODO TRACE

        httpclient_Result result = httpclient_post(url, bufferStr);
        if (result.status != 204) {
            corto_error("InfluxDB HistoryBatchNotify [%d] Response [%s].",
                result.status, result.response);
        }

        SAFE_DEALLOC(url);
        SAFE_DEALLOC(result.response)
    }
    SAFE_DEALLOC(bufferStr);

    //TODO REMOVE
    long post = timer_end(startPost);
    long total = timer_end(start);

    long timePer = resolve/cnt;

    corto_info("Total [%ld]us Buffer [%d][%ld]us Per [%ld]us Post [%ld]us",
        total, cnt, resolve, timePer, post);

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
    ///TODO TEST
    // corto_info("LIMIT [%llu] offset [%llu]", filter.limit, filter.offset);

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
    return strreplace(source, " ", "\\ ");
}

corto_string influxdb_Mount_retentionPolicy(
    influxdb_Mount this)
{
    if (!this->rp) {
        return "autogen";
    }

    return this->rp->name;
}

int16_t influxdb_UdpSend(corto_string buffer) {
    influxdb_UdpSocket s = (influxdb_UdpSocket)corto_tls_get(
        INFLUXDB_MOUNT_KEY_UDP);
    if (!s) {
        corto_throw("Failed to resolve UDP socket.");
        goto error;
    }

    size_t len = strlen(buffer) + 1;
    if (write(s->sfd, buffer, len) != len) {
        corto_throw("UDP Write failed.");
        goto error;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_UdpSocketInitialize(corto_string host, int16_t port) {
    if (port <= 0) {
        corto_throw("Invalid port [%d]", port);
        goto error;
    }

    if (corto_tls_new(&INFLUXDB_MOUNT_KEY_UDP, influxdb_UdpSocketFree)) {
        corto_throw("Failed to initialize UDP Socket key.");
        goto error;
    }

    size_t udpSockLen = sizeof(struct influxdb_UdpSocket_s);
    influxdb_UdpSocket s = (influxdb_UdpSocket)malloc(udpSockLen);

    struct addrinfo hints;
    struct addrinfo *result, *rp;

    /* Obtain address(es) matching host/port */

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;        /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_DGRAM;     /* Datagram socket */
    hints.ai_flags = 0;
    hints.ai_protocol = IPPROTO_UDP;    /* UDP protocol */

    corto_string portStr = corto_asprintf("%d", port);
    int ret = getaddrinfo(host, portStr, &hints, &result);
    if (ret) {
        corto_throw("getaddrinfo [%s:%s] Error: %s",
            host, portStr, gai_strerror(ret));
        corto_dealloc(portStr);
        goto error;
    }
    corto_dealloc(portStr);

    /* getaddrinfo() returns a list of address structures.
      Try each address until we successfully connect(2).
      If socket(2) (or connect(2)) fails, we (close the socket
      and) try the next address. */

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        s->sfd = socket(rp->ai_family, rp->ai_socktype,
                    rp->ai_protocol);
        if (s->sfd == -1)
           continue;

        if (connect(s->sfd, rp->ai_addr, rp->ai_addrlen) != -1)
           break;                  /* Success */

        close(s->sfd);
    }

    if (rp == NULL) {               /* No address succeeded */
       corto_error("Could not connect to [http://%s:%d]", host, port);
       goto error;
    }

    freeaddrinfo(result);           /* No longer needed */

    if (corto_tls_set(INFLUXDB_MOUNT_KEY_UDP, (void *)s)) {
        corto_throw("Failed to set TLS UDP connect data.");
        goto error;
    }

    return 0;
error:
    return -1;
}

static void influxdb_UdpSocketFree(void *o) {
    influxdb_UdpSocket s = (influxdb_UdpSocket)o;
    if (s) {
        close(s->sfd);
        free(s);
    }
}
