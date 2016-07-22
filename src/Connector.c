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

    corto_mount(this)->kind = CORTO_HISTORIAN;
    corto_setstr(&corto_mount(this)->contentType, "text/json");

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

    web_client_post(
        this->host,
        corto_contentof(NULL, observable, "text/influx"));

/* $end */
}
