/* This is a managed file. Do not delete this comment. */
#include <driver/mnt/influxdb/influxdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

int16_t influxdb_UdpConn_send(
    influxdb_UdpConn this,
    const char *buffer)
{
    /* Verify UDP Socket has been initialized. */
    if (this->socket <= 0) {
        if (influxdb_UdpConn_construct(this)) {
            corto_throw("Unable to send UDP message. uninitialized Socket.");
            goto error;
        }

    }

    size_t len = strlen(buffer) + 2;
    corto_string msg = corto_asprintf("%s\n\0", buffer);
    if (write(this->socket, msg, len) != len) {
        corto_throw("UDP Write failed.");
        goto error;
    }
    corto_dealloc(msg);

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
