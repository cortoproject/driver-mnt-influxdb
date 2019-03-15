/* This is a managed file. Do not delete this comment. */
#include <driver/mnt/influxdb/influxdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#define INFLUXDB_UDPCONN_UDP_MAX_BUFFER 65255
#define INFLUXDB_UDPCONN_MAX_BUFFER 500
int16_t influxdb_UdpConn_write(
    influxdb_UdpConn this,
    const char *line,
    uintptr_t buffer,
    uintptr_t bufferSize,
    bool hasNext)
{
    corto_buffer *b = (corto_buffer*)buffer;
    size_t *size = (size_t*)bufferSize;
    size_t len = strlen(line);

    /* Set Buffer Max */
    size_t max = INFLUXDB_UDPCONN_MAX_BUFFER;
    if (this->bufferMax > 0) {
        if (this->bufferMax > INFLUXDB_UDPCONN_UDP_MAX_BUFFER) {
            max = INFLUXDB_UDPCONN_UDP_MAX_BUFFER;
        } else {
            max = this->bufferMax;
        }

    }

    if ((len + *size + 2) >= max) { // Account for newline and null term
        /* Send Current Buffer before UDP max is exceeded */
        corto_buffer_appendstr(b, "\n\0");
        corto_string str = corto_buffer_str(b);
        *size = 0;
        if (influxdb_UdpConn_send(this, str)) {
            goto error;
        }

    }

    corto_buffer_appendstr(b, (char*)line);
    *size += len;
    if (hasNext) {
        corto_buffer_appendstr(b, "\n");
        *size += 1;
    }

    return 0;
error:
    return -1;
}

int16_t influxdb_UdpConn_send(
    influxdb_UdpConn this,
    const char *buffer)
{
    if (buffer == NULL) {
        corto_warning("UDP Conn: Cannot package empty buffer");
        goto error;
    }

    /* Verify UDP Socket has been initialized. */
    if (this->socket <= 0) {
        if (influxdb_UdpConn_construct(this)) {
            corto_throw("Unable to send UDP message. uninitialized Socket.");
            goto error;
        }

    }

    size_t len = strlen(buffer);
    int sent = write(this->socket, buffer, len);
    if (sent != len) {
        if (sent > 0) {
            corto_throw("UDP Write Failed. Sent [%d] of [%zu]", sent, len);
            corto_raise();
            goto error;
        } else if (sent < 0) {
            corto_throw("Write Error [%d]: %s", errno, strerror(errno));
            corto_raise();
            goto error;
        }

    }

    return 0;
error:
    return -1;
}

int16_t influxdb_UdpConn_construct(
    influxdb_UdpConn this)
{
    if (!this->port) {
        corto_throw("Invalid UDP [port]");
        goto error;
    }

    if (!this->host) {
        corto_throw("Invalid UDP [host]");
        goto error;
    }

    struct addrinfo hints;
    struct addrinfo *result, *rp;
    /* Obtain address(es) matching host/port */
    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = AF_UNSPEC;        /* Allow IPv4 or IPv6 */
    hints.ai_socktype = SOCK_DGRAM;     /* Datagram socket */
    hints.ai_flags = 0;
    hints.ai_protocol = IPPROTO_UDP;    /* UDP protocol */
    int ret = getaddrinfo(this->host, this->port, &hints, &result);
    if (ret) {
        corto_throw("getaddrinfo [%s:%s] Error: %s",
            this->host, this->port, gai_strerror(ret));
        goto error;
    }

    /* getaddrinfo() returns a list of address structures.
      Try each address until we successfully connect(2).
      If socket(2) (or connect(2)) fails, we (close the socket
      and) try the next address. */
    for (rp = result; rp != NULL; rp = rp->ai_next) {
        this->socket = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (this->socket == -1)
           continue;
        if (connect(this->socket, rp->ai_addr, rp->ai_addrlen) != -1)
           break;                  /* Success */
        close(this->socket);
    }

    if (rp == NULL) {               /* No address succeeded */
       corto_error("Could not connect to [http://%s:%d]", this->host, this->port);
       goto error;
    }

    freeaddrinfo(result);           /* No longer needed */
    return 0;
error:
    return -1;
}

void influxdb_UdpConn_destruct(
    influxdb_UdpConn this)
{
    if (this) {
        close(this->socket);
    }

}
