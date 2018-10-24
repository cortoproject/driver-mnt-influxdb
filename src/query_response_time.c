#include <driver/mnt/influxdb/query_response_time.h>
#include <driver/mnt/influxdb/query_response_parser.h>

int16_t influxdb_mount_time_rfc3339(const char* timeStr, struct timespec *ts);
int16_t influxdb_mount_time_epochNano(double nanoseconds, struct timespec *ts);

/* InfluxDB returns timestamps as "Time"
 * Format 2017-10-17T02:25:41.60012734Z
 */
int16_t influxdb_mount_response_time(JSON_Object *output, JSON_Value *value)
{
    struct timespec ts;
    JSON_Value *v = NULL;

    JSON_Value_Type timeFormat = json_value_get_type(value);
    if (timeFormat == JSONString) {
        const char *timeStr = json_value_get_string(value);
        if (!timeStr) {
            corto_throw("Failed to parse timestamp string.");
            goto error;
        }
        if (influxdb_mount_time_rfc3339(timeStr, &ts)) {
            corto_error("Failed to parse timestamp.");
            goto error;
        }

        // corto_info("Format [%s] Output Sec [%d] Nano [%d]",
            // timeStr, ts.tv_sec, ts.tv_nsec);
        // corto_info("EPOCH [%lld.%.9ld]", (long long)ts.tv_sec, ts.tv_nsec);
    } else if (timeFormat == JSONNumber) {
        ///TODO Need to check precision - hardcoded to nanoseconds
        double epochNano = json_value_get_number(value);
        if (influxdb_mount_time_epochNano(epochNano, &ts)) {
            corto_error("Failed to parse timestamp.");
            goto error;
        }
    } else {
        corto_throw("Unsupported time format. JSON Type = [%d]", timeFormat);
        goto error;
    }

    v = json_value_init_object();
    JSON_PTR_VERIFY(v, "Failed to create timestamp JSON Value.")
    JSON_Object *o = json_value_get_object(v);
    JSON_PTR_VERIFY(o, "Failed to retrieve timestamp JSON object.")

    JSON_Status s;

    s = json_object_set_number(o, "sec", ts.tv_sec);
    if (s != JSONSuccess) {
        corto_throw("Failed to set timestamp object seconds");
        goto error;
    }

    s = json_object_set_number(o, "nanosec", ts.tv_nsec);
    if (s != JSONSuccess) {
        corto_throw("Failed to set timestamp object nanoseconds");
        goto error;
    }

    s = json_object_set_value(output, "timestamp", json_value_deep_copy(v));
    if (s != JSONSuccess) {
        corto_throw("Failed to set JSON result value.");
        goto error;
    }

    json_value_free(v);

    return 0;
error:
    if (v) {
        json_value_free(v);
    }
    return -1;
}

/* https://stackoverflow.com/questions/7114690/how-to-parse-syslog-timestamp */
int16_t influxdb_mount_time_rfc3339(const char* timeStr, struct timespec *ts)
{
    struct tm tm;
    time_t t;
    char *extra = strptime(timeStr, "%C%y-%m-%dT%H:%M:%S", &tm );
    tm.tm_isdst = -1;
    t = mktime(&tm);

    if (extra && extra[0] == '.') {
        char *endptr;
        double fraction = strtod( extra, &endptr );
        extra = endptr;

        /* use timespec if fractional seconds required */
        ts->tv_sec = t;
        ts->tv_nsec = fraction * 1000000000;
    }
    else {
        goto error;
    }

    return 0;
error:
    return -1;
// invalid:
//     ///TODO improve format checks
//     // corto_throw("RFC3339 String Format Invalid [%s]", timeStr);
//     ts->tv_sec = 0;
//     ts->tv_nsec = 0;
//     return 0;
}

int16_t influxdb_mount_time_epochNano(double nanoseconds, struct timespec *ts)
{
    uint64_t ns = (uint64_t)nanoseconds;
    ts->tv_nsec = ns % (1000 * 1000 * 1000);

    uint64_t us = ns / 1000;
    uint64_t ms = us / 1000;
    ts->tv_sec = ms / 1000;

    return 0;
}
