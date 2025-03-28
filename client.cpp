#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>

static void msg(const char *msg)
{
    fprintf(stderr, "%s\n", msg);
}

static void die(const char *msg)
{
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

static int32_t read_full(int fd, char *buf, size_t n)
{
    while (n > 0)
    {
        ssize_t rv = read(fd, buf, n);
        if (rv <= 0)
        {
            return -1; // error, or unexpected EOF
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

static int32_t write_all(int fd, const char *buf, size_t n)
{
    while (n > 0)
    {
        ssize_t rv = write(fd, buf, n);
        if (rv <= 0)
        {
            return -1; // error
        }
        assert((size_t)rv <= n);
        n -= (size_t)rv;
        buf += rv;
    }
    return 0;
}

const size_t k_max_msg = 32 << 20; // likely larger than the kernel buffer

// Modified send_req to handle HTTP requests (GET/POST)
static int32_t send_req(int fd, const std::string &method, const std::string &path, const std::string &body = "")
{
    std::string request;

    // Build the HTTP request based on method (GET/POST)
    if (method == "POST")
    {
        request = "POST " + path + " HTTP/1.1\r\n";
        request += "Content-Type: application/json\r\n";
        request += "Content-Length: " + std::to_string(body.size()) + "\r\n";
        request += "\r\n";
        request += body;
    }
    else if (method == "GET")
    {
        request = "GET " + path + " HTTP/1.1\r\n";
        request += "\r\n";
    }
    else
    {
        return -1; // Unsupported HTTP method
    }

    return write_all(fd, request.c_str(), request.size());
}

// Modified read_res to handle HTTP responses
static int32_t read_res(int fd)
{
    // Read response header (we assume HTTP/1.1 response)
    char rbuf[4 + k_max_msg + 1];
    int32_t err = read_full(fd, rbuf, 4);
    if (err)
    {
        if (errno == 0)
        {
            msg("EOF");
        }
        else
        {
            msg("read() error");
        }
        return err;
    }

    // Read HTTP response body
    uint32_t len = 0;
    memcpy(&len, rbuf, 4); // Assume little endian
    printf("len = %u\n", len);
    if (len > k_max_msg)
    {
        msg("too long");
        return -1;
    }

    err = read_full(fd, &rbuf[4], len);
    if (err)
    {
        msg("read() error");
        return err;
    }

    // Print the result (parse the response)
    uint32_t rescode = 0;
    if (len < 4)
    {
        msg("bad response");
        return -1;
    }
    memcpy(&rescode, &rbuf[4], 4);
    printf("server says: [%u] %.*s\n", rescode, len - 4, &rbuf[8]);
    return 0;
}

int main(int argc, char **argv)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        die("socket()");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(1234);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK); // 127.0.0.1
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv)
    {
        die("connect");
    }

    std::string method, path, body;

    if (argc < 2)
    {
        msg("Usage: client <method> <key> [value]");
        close(fd);
        return 1;
    }

    method = argv[1];

    if (method == "POST" && argc == 4)
    {
        path = "/put";
        body = "{\"key\": \"" + std::string(argv[2]) + "\", \"value\": \"" + std::string(argv[3]) + "\"}";
    }
    else if (method == "GET" && argc == 3)
    {
        path = "/get?key=" + std::string(argv[2]);
    }
    else
    {
        msg("Invalid arguments");
        close(fd);
        return 1;
    }

    int32_t err = send_req(fd, method, path, body);
    if (err)
    {
        goto L_DONE;
    }
    err = read_res(fd);
    if (err)
    {
        goto L_DONE;
    }

L_DONE:
    close(fd);
    return 0;
}
