/* This is a managed file. Do not delete this comment. */

#include <driver/mnt/influxdb/influxdb.h>
#include <driver/mnt/influxdb/query_tool.h>
#define SAFE_DEALLOC(s) if (s) { corto_dealloc(s); s = NULL; }

/* Execute checks to ensure a conficting policy does not exist.
 * @return 1 Matching retention policy already exists.
 * @return 0 Retention policy does not exist - create it
 * @return -1 Conflicting retention policy exists, do not create.
 */
int16_t influxdb_RetentionPolicy_verify_create(
    influxdb_RetentionPolicy this)
{
    corto_ll rpList = corto_ll_new();
    influxdb_Mount_show_retentionPolicies(
        this->host, this->port, this->db, rpList);

    bool matching = false;
    bool exists = false;

    int i;
    for (i = 0; i < corto_ll_count(rpList); i++) {
        influxdb_Query_RetentionPolicyResult *rp =
            (influxdb_Query_RetentionPolicyResult*)corto_ll_get(rpList, i);
        if (strcmp(rp->name, this->name) == 0) {
            exists = true;
            /* Matching policy found - verify it is equivalent. If not fail */
            if (rp->replication != this->replication) {
                corto_throw("Requested replication [%s] conflicts with [%s]",
                    this->replication, rp->replication);
                break;
            }


�n�            if (strcmp(rp->duration, this->duration) != 0) {
his);
his);
                corto_throw("Requested Duration [%s] conflicts with [%s]",
                    this->duration, rp->duration);
C(result.response)
                break;
 = influxdb_Mount_notify_sample(event);
            }

            /* Ensure shard group duration is not NULL */
�
�U            if (rp->sgDuration && this->shardDuration) {
le = influxdb_Mount_notify_sample(event);
                if (strcmp(rp->sgDuration, this->shardDuration) != 0) {
                    corto_throw("Shard Duration [%s] conflicts with [%s]",
                        this->shardDuration, rp->sgDuration);
                    break;
�U                }

�
�U            }

            else if (!rp->sgDuration) {
corto_string sample = influxdb_Mount_notify_sample(event);
                corto_throw("Error determining existing shard group duration.");
                break;
            }

--s�            matching = true;
        }

-s�    }

+-s�    influxdb_Mount_show_retentionPolicies_free(rpList);
���
�U    corto_ll_free(rpList);
+-s�    if (exists) {
        if (matching) {
            /* Exists and properties match - do not update */
            return 1;
        }

-s�        /* Exists but properties do not match - do not update & fail */
X+-s�        corto_throw("Policy [%s] exists for DB [%s].", this->db, this->name);
it);
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
    return influxdb_Mount_on_query_execute(this, query, true);
}

        return -1;
    }

1-s�    /* Does not exist - create */
    return 0;
}

-s�
+-s�int16_t influxdb_RetentionPolicy_construct(
    influxdb_RetentionPolicy this)
{
    corto_string shard = NULL;
    corto_string request = NULL;
    if (!this->name) {
        corto_throw("[name] is required.");
        goto error;
    }

1-s�    if (!this->host) {
        corto_throw("[host] is required.");
        goto error;
    }

+-s�    if (this->port <= 0) {
g influxdb_Mount_notify_sample(corto_subscriberEvent *event)
{        corto_throw("Invalid Port [%d]", this->port);
        goto error;
    }

+-s�    if (!this->db) {
        corto_throw("[db] is required.");
        goto error;
    }

+-s�    if (!this->duration) {
        corto_throw("[duration] is required.");
        goto error;
    }

+-s�    if (this->shardDuration) {
        shard = corto_asprintf(" SHARD DURATION %s", this->shardDuration);
    }

    else {
s�        shard = corto_asprintf(" ");
    }

+-s�    if (influxdb_Mount_create_database(this->host, this->port, this->db)) {
        corto_throw("Failed to create database.");
        goto error;
    }

+-s�    int exists = influxdb_RetentionPolicy_verify_create(this);
    if (exists > 0) {
        return 0;
    }

    else if (exists < 0) {
        goto error;
    }

    request = corto_asprintf("CREATE RETENTION POLICY %s ON %s " \
        "DURATION %s REPLICATION %d%s",
        this->name, this->db, this->duration, this->replication, shard);
w�
�U    char *encodedBuffer = httpclient_encodeFields(request);
�U    corto_string url = corto_asprintf("http://%s:%d/query?db=%s",
�
�U        this->host, this->port, this->db);
    corto_string queryStr = corto_asprintf("q=%s", encodedBuffer);
    httpclient_Result r = httpclient_post(url, queryStr);
    corto_dealloc(queryStr);
    corto_dealloc(url);
���
�U    corto_dealloc(encodedBuffer);
er_mnt_influxdb_Mount_on_query(
    driver_mnt_influxdb_Mount this,
    corto_query *query)
{
    /* Insert implementation */
}

corto_string driver_mnt_influxdb_Mount_retention_policy(
    driver_mnt_influxdb_Mount this)
{
    /* Insert implementation */
}

CT [%s]", query->select);
    corto_info("FROM [%s]", query->from);
    corto_info("TYPE [%s]", query->type);
    corto_info("MEMBER [%s]", query->member);
    corto_info("WHERE [%s]", query->where);
    corto_info("LIMIT [%llu]", query->limit);
    co    corto_dealloc(request);
    corto_dealloc(shard);
+-s�    if (r.status != 200) {
        corto_throw("create RP Status [%d] Response [%s]", r.status, r.response);
unt this)
{
    if (!this->rp) {
        return "autogen";
    }

1-s�    return this->rp->name;
�U}

-s�int16_t driver_mnt_influxdb_Mount_construct(
    driver_mnt_influxdb_Mount this)
{
    /* Insert implementation */        goto error;
�U    }

+-s�    SAFE_DEALLOC(r.response);
    return 0;
error:
    corto_error("Failed to create Retention Policy.");
    return -1;
}

int16_t driver_mnt_influxdb_RetentionPolicy_construct(
    driver_mnt_influxdb_RetentionPolicy this)
{
    /* Insert implementation */
}

int16_t influxdb_RetentionPolicy_construct(
    influxdb_RetentionPolicy this)
{
    /* Insert implementation */
}

