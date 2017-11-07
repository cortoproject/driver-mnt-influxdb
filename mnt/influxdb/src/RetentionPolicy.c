/* This is a managed file. Do not delete this comment. */

#include <driver/mnt/influxdb/influxdb.h>

int16_t influxdb_RetentionPolicy_construct(
    influxdb_RetentionPolicy this)
{
    corto_string shard = NULL;
    corto_string request = NULL;

    if (!this->name) {
        corto_seterr("[name] is required.");
        goto error;
    }

    if (!this->host) {
        corto_seterr("[host] is required.");
        goto error;
    }

    if (!this->db) {
        corto_seterr("[db] is required.");
        goto error;
    }

    if (!this->duration) {
        corto_seterr("[duration] is required.");
        goto error;
    }

    if (this->shardDuration) {
        shard = corto_asprintf(" SHARD DURATION %s", this->shardDuration);
    }
    else {
        shard = corto_asprintf(" ");
    }


    request = corto_asprintf("CREATE RETENTION POLICY %s ON %s " \
        "DURATION %s REPLICATION %d%s",
        this->name, this->db, this->duration, this->replication, shard);
    char *encodedBuffer = httpclient_encode_fields(request);
    corto_string url = corto_asprintf("%s/query?db=%s", this->host, this->db);
    corto_string queryStr = corto_asprintf("q=%s", encodedBuffer);
    httpclient_Result r = httpclient_get(url, queryStr);
    corto_dealloc(queryStr);
    corto_dealloc(url);
    corto_dealloc(encodedBuffer);
    corto_dealloc(request);
    corto_dealloc(shard);

    if (r.status != 200) {
        corto_error("Create Retention Policy failed. Status [%d] Response [%s]",
            r.status, r.response);
        goto error;
    }

    return 0;
error:
    corto_error("Failed to create Retention Policy. Error: %s", corto_lasterr());
    return -1;
}
