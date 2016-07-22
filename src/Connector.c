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

    return CORTO_ITERATOR_EMPTY;
/* $end */
}

corto_void _influxdb_Connector_onUpdate(
    influxdb_Connector this,
    corto_object observable)
{
/* $begin(influxdb/Connector/onUpdate) */
    corto_string url;

    corto_asprintf(&url, "%s/write?db=%s", this->host, this->db);
    web_client_post(url, corto_contentof(NULL, "text/influx", observable));
    corto_dealloc(url);

/* $end */
}
