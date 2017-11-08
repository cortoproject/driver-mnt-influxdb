/* This is a managed file. Do not delete this comment. */

#include <driver/mnt/influxdb/influxdb.h>

/* Execute checks to ensure a conficting policy does not exist.
 * @return 1 Matching retention policy already exists.
 * @return 0 Retention policy does not exist - Create it
 * @return -1 Conflicting retention policy exists, do not create.
 */
int16_t influxdb_RetentionPolicy_verify_create(
    influxdb_RetentionPolicy this)
{
    corto_ll rpList = corto_ll_new();
    influxdb_Mount_show_retentionPolicies(this->host, this->db, rpList);

    bool matching = false;
    bool exists = false;

    int i;
    for (i = 0; i < corto_ll_size(rpList); i++) {
        influxdb_Query_RetentionPolicyResult *rp =
            (influxdb_Query_RetentionPolicyResult*)corto_ll_get(rpList, i);
        if (strcmp(rp->name, this->name) == 0) {
            exists = true;
            /* Matching policy found - verify it is equivalent. If not fail */
            if (rp->replication != this->replication) {
                corto_seterr("Requested replication [%s] conflicts with [%s]",
                    this->replication, rp->replication);
                break;
            }
            if (strcmp(rp->duration, this->duration) != 0) {
                corto_seterr("Requested Duration [%s] conflicts with [%s]",
                    this->duration, rp->duration);
                break;
            }
            /* Ensure shard group duration is not NULL */
            if (rp->sgDuration && this->shardDuration) {
                if (strcmp(rp->sgDuration, this->shardDuration) != 0) {
                    corto_seterr("Shard Duration [%s] conflicts with [%s]",
                        this->shardDuration, rp->sgDuration);
                    break;
                }
            }
            else if (!rp->sgDuration) {
                corto_seterr("Error determining existing shard group duration.");
                break;
            }

            matching = true;
        }
    }

    influxdb_Mount_show_retentionPolicies_free(rpList);
    corto_ll_free(rpList);

    if (exists) {
        if (matching) {
            /* Exists and properties match - do not update */
            return 1;
        }
        /* Exists but properties do not match - do not update & fail */
        corto_error("Policy [%s] exists for DB [%s]. Error: %s",
            this->db, this->name, corto_lasterr());
        return -1;
    }

    /* Does not exist - create */
    return 0;
}

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

    if (influxdb_Mount_create_database(this->host, this->db)) {
        corto_seterr("Failed to create database.");
        goto error;
    }

    int exists = influxdb_RetentionPolicy_verify_create(this);
    if (exists > 0) {
        return 0;
    }
    else if (exists < 0) {
        goto error;
    }
    request = corto_asprintf("CREATE RETENTION POLICY %s ON %s " \
        "DURATION %s REPLICATION %d%s",
        this->name, this->db, this->duration, this->replication, shard);
    char *encodedBuffer = httpclient_encode_fields(request);
    corto_string url = corto_asprintf("%s/query?db=%s", this->host, this->db);
    corto_string queryStr = corto_asprintf("q=%s", encodedBuffer);
    httpclient_Result r = httpclient_post(url, queryStr);
    corto_dealloc(queryStr);
    corto_dealloc(url);
    corto_dealloc(encodedBuffer);
    corto_dealloc(request);
    corto_dealloc(shard);

    if (r.status != 200) {
        corto_seterr("Status [%d] Response [%s]", r.status, r.response);
        goto error;
    }

    return 0;
error:
    corto_error("Failed to create Retention Policy. Error: %s", corto_lasterr());
    return -1;
}
