// stdlib
#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <math.h> // isnan
// system
#include <time.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <netinet/tcp.h>
#include <signal.h> // added for SIGPIPE
#include <sys/resource.h>
#include <iostream>
// C++
#include <string>
#include <vector>
#include <sstream>
#include <algorithm>
#include <stddef.h> // added for offsetof
// proj
#include "common.h"
#include "hashtable.h"
#include "zset.h"
#include "list.h"
#include "heap.h"
#include "thread_pool.h"

// Add container_of definition if not defined
#ifndef container_of
#define container_of(ptr, type, member) \
    ((type *)((char *)(ptr) - offsetof(type, member)))
#endif

// Serialization enums and related definitions
enum
{
    TAG_NIL = 0, // nil
    TAG_ERR = 1, // error code + msg
    TAG_STR = 2, // string
    TAG_INT = 3, // int64
    TAG_DBL = 4, // double
    TAG_ARR = 5, // array
};

// Value types
enum
{
    T_INIT = 0,
    T_STR = 1,  // string
    T_ZSET = 2, // sorted set
};

// KV pair for the top-level hashtable
struct Entry
{
    struct HNode node; // hashtable node
    std::string key;
    // for TTL
    size_t heap_idx = -1; // array index to the heap item
    // value
    uint32_t type = 0;
    // one of the following
    std::string str;
    ZSet zset;

    uint64_t last_access = 0; // new: timestamp of last access for eviction
};

static Entry *entry_new(uint32_t type)
{
    Entry *ent = new Entry();
    ent->type = type;
    return ent;
}

// Add the LookupKey definition and helper functions
struct LookupKey
{
    HNode node;
    std::string key;
};

static bool entry_eq(HNode *node, HNode *key)
{
    Entry *ent = container_of(node, Entry, node);
    LookupKey *lk = container_of(key, LookupKey, node);
    return ent->key == lk->key;
}

static void entry_del(Entry *ent)
{
    // Simple deletion; adjust as needed
    delete ent;
}

// Serialization helper functions
typedef std::vector<uint8_t> Buffer;

// append to the back
static void buf_append(Buffer &buf, const uint8_t *data, size_t len)
{
    buf.insert(buf.end(), data, data + len);
}
static void buf_append_u8(Buffer &buf, uint8_t data)
{
    buf.push_back(data);
}
static void buf_append_u32(Buffer &buf, uint32_t data)
{
    buf_append(buf, (const uint8_t *)&data, 4);
}
static void buf_append_i64(Buffer &buf, int64_t data)
{
    buf_append(buf, (const uint8_t *)&data, 8);
}
static void buf_append_dbl(Buffer &buf, double data)
{
    buf_append(buf, (const uint8_t *)&data, 8);
}
static void out_nil(Buffer &out)
{
    buf_append_u8(out, TAG_NIL);
}
static void out_str(Buffer &out, const char *s, size_t size)
{
    buf_append_u8(out, TAG_STR);
    buf_append_u32(out, (uint32_t)size);
    buf_append(out, (const uint8_t *)s, size);
}
static void out_int(Buffer &out, int64_t val)
{
    buf_append_u8(out, TAG_INT);
    buf_append_i64(out, val);
}
static void out_dbl(Buffer &out, double val)
{
    buf_append_u8(out, TAG_DBL);
    buf_append_dbl(out, val);
}
static void out_err(Buffer &out, uint32_t code, const std::string &msg)
{
    buf_append_u8(out, TAG_ERR);
    buf_append_u32(out, code);
    buf_append_u32(out, (uint32_t)msg.size());
    buf_append(out, (const uint8_t *)msg.data(), msg.size());
}
static void out_arr(Buffer &out, uint32_t n)
{
    buf_append_u8(out, TAG_ARR);
    buf_append_u32(out, n);
}
static size_t out_begin_arr(Buffer &out)
{
    out.push_back(TAG_ARR);
    buf_append_u32(out, 0); // filled by out_end_arr()
    return out.size() - 4;  // the `ctx` arg
}
static void out_end_arr(Buffer &out, size_t ctx, uint32_t n)
{
    assert(out[ctx - 1] == TAG_ARR);
    memcpy(&out[ctx], &n, 4);
}

static void msg(const char *msg)
{
    fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char *msg)
{
    fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

static void die(const char *msg)
{
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

static uint64_t get_monotonic_msec()
{
    struct timespec tv = {0, 0};
    clock_gettime(CLOCK_MONOTONIC, &tv);
    return uint64_t(tv.tv_sec) * 1000 + tv.tv_nsec / 1000 / 1000;
}

static void fd_set_nb(int fd)
{
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno)
    {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    (void)fcntl(fd, F_SETFL, flags);
    if (errno)
    {
        die("fcntl error");
    }
}

const size_t k_max_msg = 32 << 20; // likely larger than the kernel buffer

// Define maximum allowed keys before eviction.
static const size_t MAX_KEYS = 25000;
static const double MEM_EVICTION_THRESHOLD = 0.70; // Evict at 70% memory usage
static const double MEM_HIGH_WATER = 0.85;         // Aggressive eviction at 85%
static const double MEM_CRITICAL = 0.95;           // Emergency eviction at 95%

// Add function to get current memory usage
static double get_memory_usage()
{
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0)
    {
        // Get maxrss (resident set size) in KB, convert to fraction of system memory
        long total_memory = sysconf(_SC_PHYS_PAGES) * sysconf(_SC_PAGESIZE) / 1024;
        double usage_fraction = static_cast<double>(usage.ru_maxrss) / total_memory;
        return usage_fraction;
    }
    return 0.0; // On error, assume low memory usage
}

// Set idle timeout and connection limits
const uint64_t k_idle_timeout_ms = 60 * 1000; // 60 seconds (was 5 seconds)
const size_t MAX_ACTIVE_CONNS = 8000;         // Limit maximum active connections
const int MAX_CONN_PER_SEC = 1000;            // Maximum new connections per second

// remove from the front
static void buf_consume(Buffer &buf, size_t n)
{
    buf.erase(buf.begin(), buf.begin() + n);
}

struct Conn
{
    int fd = -1;
    // application's intention, for the event loop
    bool want_read = false;
    bool want_write = false;
    bool want_close = false;
    // buffered input and output
    Buffer incoming; // data to be parsed by the application
    Buffer outgoing; // responses generated by the application
    // timer
    uint64_t last_active_ms = 0;
    DList idle_node;
};

// global states
static struct
{
    HMap db;
    // a map of all client connections, keyed by fd
    std::vector<Conn *> fd2conn;
    // timers for idle connections
    DList idle_list;
    // timers for TTLs
    std::vector<HeapItem> heap;
    // the thread pool
    TheadPool thread_pool;
} g_data;

// Add forward declaration for hnode_same() so it can be used in evict_keys().
static bool hnode_same(HNode *node, HNode *key);

static void evict_keys()
{
    size_t current = hm_size(&g_data.db);
    double mem_usage = get_memory_usage();

    // Skip if we're under all thresholds
    if (current <= MAX_KEYS && mem_usage < MEM_EVICTION_THRESHOLD)
    {
        return;
    }

    // Determine percentage to evict based on memory pressure
    double evict_percentage = 0.10; // Default: evict 10%

    if (mem_usage >= MEM_CRITICAL)
    {
        evict_percentage = 0.40; // Emergency: evict 40%
        fprintf(stderr, "CRITICAL MEMORY USAGE (%.1f%%): Evicting %.1f%% of keys\n",
                mem_usage * 100, evict_percentage * 100);
    }
    else if (mem_usage >= MEM_HIGH_WATER)
    {
        evict_percentage = 0.25; // Heavy pressure: evict 25%
        fprintf(stderr, "HIGH MEMORY USAGE (%.1f%%): Evicting %.1f%% of keys\n",
                mem_usage * 100, evict_percentage * 100);
    }
    else if (mem_usage >= MEM_EVICTION_THRESHOLD)
    {
        evict_percentage = 0.15; // Moderate pressure: evict 15%
        fprintf(stderr, "MEMORY THRESHOLD EXCEEDED (%.1f%%): Evicting %.1f%% of keys\n",
                mem_usage * 100, evict_percentage * 100);
    }

    size_t num_to_evict = std::max(size_t(current * evict_percentage), size_t(1));
    std::vector<Entry *> candidates;
    candidates.reserve(current);

    // Callback to collect all entries.
    auto collect_cb = [](HNode *node, void *arg) -> bool
    {
        std::vector<Entry *> *vec = (std::vector<Entry *> *)arg;
        Entry *ent = container_of(node, Entry, node);
        vec->push_back(ent);
        return true;
    };
    hm_foreach(&g_data.db, collect_cb, &candidates);

    // Sort candidates - enhance sorting to favor evicting larger entries first when under memory pressure
    if (mem_usage >= MEM_HIGH_WATER)
    {
        // Under high memory pressure, prioritize evicting larger entries (strings)
        std::sort(candidates.begin(), candidates.end(), [](Entry *a, Entry *b)
                  {
            // First prioritize by type - string entries can be large
            if (a->type == T_STR && b->type != T_STR) return true;
            if (a->type != T_STR && b->type == T_STR) return false;
            
            // For strings, prioritize larger strings
            if (a->type == T_STR && b->type == T_STR) {
                if (a->str.size() != b->str.size())
                    return a->str.size() > b->str.size();
            }
            
            // Fall back to LRU for ties or non-string types
            return a->last_access < b->last_access; });
    }
    else
    {
        // Standard LRU eviction for normal conditions
        std::sort(candidates.begin(), candidates.end(), [](Entry *a, Entry *b)
                  { return a->last_access < b->last_access; });
    }

    for (size_t i = 0; i < num_to_evict && i < candidates.size(); i++)
    {
        Entry *ent = candidates[i];
        HNode *node = hm_delete(&g_data.db, &ent->node, &hnode_same);
        if (node)
        {
            entry_del(ent);
        }
    }

    // Report current memory status after eviction
    if (mem_usage >= MEM_EVICTION_THRESHOLD)
    {
        double new_usage = get_memory_usage();
        fprintf(stderr, "After eviction: Memory usage %.1f%%, Keys: %zu\n",
                new_usage * 100, hm_size(&g_data.db));
    }
}

// Ensure hnode_same is defined
static bool hnode_same(HNode *node, HNode *key)
{
    return node == key;
}

// Forward declarations for functions used by HTTP handlers.
static void do_get(std::vector<std::string> &cmd, Buffer &out);
static void do_set(std::vector<std::string> &cmd, Buffer &out);

static std::string http_response(int code, const std::string &json_body)
{
    std::string status_text = (code == 200) ? "OK" : "ERROR";
    std::ostringstream oss;
    oss << "HTTP/1.1 " << code << " " << status_text << "\r\n"
        << "Content-Type: application/json\r\n"
        << "Content-Length: " << json_body.size() << "\r\n"
        << "\r\n"
        << json_body;
    return oss.str();
}

static std::string handle_http_request(const std::string &request)
{
    fprintf(stderr, "Received HTTP request:\n%s\n", request.c_str());
    // Split header and body
    size_t header_end = request.find("\r\n\r\n");
    if (header_end == std::string::npos)
    {
        return http_response(400, "{\"status\": \"ERROR\", \"message\": \"Bad Request: Incomplete header\"}");
    }
    std::string header = request.substr(0, header_end);
    std::string body = "";
    if (request.size() > header_end + 4)
    {
        body = request.substr(header_end + 4);
    }

    // Parse the request line.
    std::istringstream header_stream(header);
    std::string method, path, version;
    header_stream >> method >> path >> version;

    if (method == "GET")
    {
        // Example URL: /get?key=exampleKey
        if (path.find("/get") == 0)
        {
            size_t pos = path.find("key=");
            std::string key = (pos != std::string::npos) ? path.substr(pos + 4) : "";
            Buffer out;
            std::vector<std::string> cmd = {"get", key};
            do_get(cmd, out);
            std::string value(out.begin(), out.end());
            if (value.empty())
            {
                return http_response(404, "{\"status\": \"ERROR\", \"message\": \"Key not found.\"}");
            }
            std::ostringstream oss;
            oss << "{\"status\": \"OK\", \"key\": \"" << key << "\", \"value\": \"" << value << "\"}";
            return http_response(200, oss.str());
        }
    }
    else if (method == "POST")
    {
        if (path == "/put")
        {
            // For simplicity, we use naive parsing for JSON.
            // Expect a body like: {"key": "someKey", "value": "someValue"}
            std::string key, value;
            size_t key_pos = body.find("\"key\"");
            if (key_pos != std::string::npos)
            {
                size_t colon = body.find(":", key_pos);
                size_t quote1 = body.find("\"", colon);
                size_t quote2 = body.find("\"", quote1 + 1);
                if (quote1 != std::string::npos && quote2 != std::string::npos)
                {
                    key = body.substr(quote1 + 1, quote2 - quote1 - 1);
                }
            }
            size_t value_pos = body.find("\"value\"");
            if (value_pos != std::string::npos)
            {
                size_t colon = body.find(":", value_pos);
                size_t quote1 = body.find("\"", colon);
                size_t quote2 = body.find("\"", quote1 + 1);
                if (quote1 != std::string::npos && quote2 != std::string::npos)
                {
                    value = body.substr(quote1 + 1, quote2 - quote1 - 1);
                }
            }
            if (key.empty() || value.empty())
            {
                return http_response(400, "{\"status\": \"ERROR\", \"message\": \"Invalid JSON payload.\"}");
            }
            Buffer out;
            std::vector<std::string> cmd = {"set", key, value};
            do_set(cmd, out);
            return http_response(200, "{\"status\": \"OK\", \"message\": \"Key inserted/updated successfully.\"}");
        }
    }
    return http_response(404, "{\"status\": \"ERROR\", \"message\": \"Not Found\"}");
}

// Attempt to parse one complete HTTP request from conn->incoming.
// Returns true if a complete request was found, parsed, and a response added to conn->outgoing.
static bool try_one_http_request(Conn *conn)
{
    // Look for the end of the header ("\r\n\r\n")
    auto header_end_it = std::search(conn->incoming.begin(), conn->incoming.end(),
                                     (const uint8_t *)"\r\n\r\n", (const uint8_t *)"\r\n\r\n" + 4);
    if (header_end_it == conn->incoming.end())
    {
        // Check for potential overrun - if incoming buffer is too large but no header end found
        if (conn->incoming.size() > 8192)
        { // 8KB is reasonable for HTTP headers
            conn->want_close = true;
            return false; // Invalid request, force close
        }
        return false; // Incomplete header
    }
    size_t header_end = std::distance(conn->incoming.begin(), header_end_it) + 4;

    // Convert header to string to check for Content-Length (for POST)
    std::string header_str(conn->incoming.begin(), conn->incoming.begin() + header_end);
    size_t content_length = 0;
    if (header_str.find("Content-Length:") != std::string::npos)
    {
        std::istringstream hs(header_str);
        std::string line;
        while (std::getline(hs, line))
        {
            // Remove carriage return if present
            if (!line.empty() && line.back() == '\r')
            {
                line.pop_back();
            }

            if (line.find("Content-Length:") == 0)
            {
                try
                {
                    content_length = std::stoul(line.substr(16)); // Skip "Content-Length: "
                }
                catch (...)
                {
                    content_length = 0;
                }
            }
        }
    }

    // If POST, ensure the entire body is available.
    if (header_str.find("POST") == 0)
    {
        if (conn->incoming.size() < header_end + content_length)
        {
            // Check for potential abuse - if content length is too large
            if (content_length > 1024 * 1024)
            { // 1MB max
                conn->want_close = true;
                return false; // Invalid request, force close
            }
            return false; // wait for the full body
        }
    }

    // We have a complete HTTP request.
    size_t total_request_size = header_end + content_length;
    std::string request_str(conn->incoming.begin(), conn->incoming.begin() + total_request_size);

    std::string response = handle_http_request(request_str);
    // Append response to outgoing (as bytes)
    conn->outgoing.insert(conn->outgoing.end(), response.begin(), response.end());

    // Remove the processed request from the incoming buffer.
    buf_consume(conn->incoming, total_request_size);
    return true;
}

static void do_get(std::vector<std::string> &cmd, Buffer &out)
{
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node)
    {
        return;
    }
    Entry *ent = container_of(node, Entry, node);
    if (ent->type != T_STR)
    {
        return;
    }
    // Update access time.
    ent->last_access = get_monotonic_msec();
    buf_append(out, (const uint8_t *)ent->str.data(), ent->str.size());
}

static void do_set(std::vector<std::string> &cmd, Buffer &out)
{
    // Check memory usage at beginning of operation
    double mem_usage = get_memory_usage();
    if (mem_usage >= MEM_CRITICAL)
    {
        // Critical memory state, force eviction before adding any keys
        evict_keys();
    }

    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (node)
    {
        Entry *ent = container_of(node, Entry, node);
        if (ent->type != T_STR)
        {
            out.push_back(TAG_ERR);
            return;
        }
        // Update access time and value.
        ent->last_access = get_monotonic_msec();
        ent->str.swap(cmd[2]);
    }
    else
    {
        // For large values, check if we're near threshold and proactively evict
        if (cmd[2].size() > 1024 && mem_usage >= MEM_EVICTION_THRESHOLD * 0.9)
        {
            evict_keys(); // Proactive eviction for large values
        }

        Entry *ent = entry_new(T_STR);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        ent->str.swap(cmd[2]);
        ent->last_access = get_monotonic_msec();
        hm_insert(&g_data.db, &ent->node);
        // Trigger eviction if needed.
        evict_keys();
    }
    out.push_back(TAG_NIL);
}

// application callback when the listening socket is ready
static int32_t handle_accept(int fd)
{
    // Implement connection rate limiting
    static uint64_t last_check_time = 0;
    static int connections_since_last_check = 0;

    uint64_t now = get_monotonic_msec();

    // Reset counter every second
    if (now - last_check_time > 1000)
    {
        last_check_time = now;
        connections_since_last_check = 0;
    }

    connections_since_last_check++;

    // Count active connections
    size_t active_conns = 0;
    for (Conn *conn : g_data.fd2conn)
    {
        if (conn)
            active_conns++;
    }

    // Apply rate limiting
    if (connections_since_last_check > MAX_CONN_PER_SEC || active_conns >= MAX_ACTIVE_CONNS)
    {
        // Too many connections - silently refuse but don't report as error
        struct sockaddr_in client_addr = {};
        socklen_t addrlen = sizeof(client_addr);
        int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
        if (connfd >= 0)
        {
            close(connfd); // Close immediately
        }
        return 0;
    }

    // accept
    struct sockaddr_in client_addr = {};
    socklen_t addrlen = sizeof(client_addr);
    int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
    if (connfd < 0)
    {
        msg_errno("accept() error");
        return -1;
    }
    uint32_t ip = client_addr.sin_addr.s_addr;
    fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
            ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
            ntohs(client_addr.sin_port));

    // set the new connection fd to nonblocking mode
    fd_set_nb(connfd);

    // Add socket options for better connection handling
    int val = 1;
    setsockopt(connfd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val));
    setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, &val, sizeof(val));
    // Set TCP keepalive parameters
    int idle = 60;
    int intvl = 10;
    int cnt = 5;
    setsockopt(connfd, IPPROTO_TCP, TCP_KEEPIDLE, &idle, sizeof(idle));
    setsockopt(connfd, IPPROTO_TCP, TCP_KEEPINTVL, &intvl, sizeof(intvl));
    setsockopt(connfd, IPPROTO_TCP, TCP_KEEPCNT, &cnt, sizeof(cnt));
    struct linger lg = {1, 0}; // Zero timeout - don't wait
    setsockopt(connfd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));

    // create a `struct Conn`
    Conn *conn = new Conn();
    conn->fd = connfd;
    conn->want_read = true;
    conn->last_active_ms = get_monotonic_msec();
    dlist_insert_before(&g_data.idle_list, &conn->idle_node);

    // put it into the map
    if (g_data.fd2conn.size() <= (size_t)conn->fd)
    {
        g_data.fd2conn.resize(conn->fd + 1);
    }
    assert(!g_data.fd2conn[conn->fd]);
    g_data.fd2conn[conn->fd] = conn;
    return 0;
}

static void conn_destroy(Conn *conn)
{
    (void)close(conn->fd);
    g_data.fd2conn[conn->fd] = NULL;
    dlist_detach(&conn->idle_node);

    // Clear buffers to free memory immediately
    Buffer().swap(conn->incoming);
    Buffer().swap(conn->outgoing);

    delete conn;
}

const size_t k_max_args = 200 * 1000;

static bool read_u32(const uint8_t *&cur, const uint8_t *end, uint32_t &out)
{
    if (cur + 4 > end)
    {
        return false;
    }
    memcpy(&out, cur, 4);
    cur += 4;
    return true;
}

static bool
read_str(const uint8_t *&cur, const uint8_t *end, size_t n, std::string &out)
{
    if (cur + n > end)
    {
        return false;
    }
    out.assign(cur, cur + n);
    cur += n;
    return true;
}

// +------+-----+------+-----+------+-----+-----+------+
// | nstr | len | str1 | len | str2 | ... | len | strn |
// +------+-----+------+-----+------+-----+-----+------+

static int32_t
parse_req(const uint8_t *data, size_t size, std::vector<std::string> &out)
{
    const uint8_t *end = data + size;
    uint32_t nstr = 0;
    if (!read_u32(data, end, nstr))
    {
        return -1;
    }
    if (nstr > k_max_args)
    {
        return -1; // safety limit
    }

    while (out.size() < nstr)
    {
        uint32_t len = 0;
        if (!read_u32(data, end, len))
        {
            return -1;
        }
        out.push_back(std::string());
        if (!read_str(data, end, len, out.back()))
        {
            return -1;
        }
    }
    if (data != end)
    {
        return -1; // trailing garbage
    }
    return 0;
}

// error code for TAG_ERR
enum
{
    ERR_UNKNOWN = 1, // unknown command
    ERR_TOO_BIG = 2, // response too big
    ERR_BAD_TYP = 3, // unexpected value type
    ERR_BAD_ARG = 4, // bad arguments
};

static void do_del(std::vector<std::string> &cmd, Buffer &out)
{
    // a dummy struct just for the lookup
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    // hashtable delete
    HNode *node = hm_delete(&g_data.db, &key.node, &entry_eq);
    if (node)
    { // deallocate the pair
        entry_del(container_of(node, Entry, node));
    }
    return out_int(out, node ? 1 : 0);
}

static void heap_delete(std::vector<HeapItem> &a, size_t pos)
{
    // swap the erased item with the last item
    a[pos] = a.back();
    a.pop_back();
    // update the swapped item
    if (pos < a.size())
    {
        heap_update(a.data(), pos, a.size());
    }
}

static void heap_upsert(std::vector<HeapItem> &a, size_t pos, HeapItem t)
{
    if (pos < a.size())
    {
        a[pos] = t; // update an existing item
    }
    else
    {
        pos = a.size();
        a.push_back(t); // or add a new item
    }
    heap_update(a.data(), pos, a.size());
}

// set or remove the TTL
static void entry_set_ttl(Entry *ent, int64_t ttl_ms)
{
    if (ttl_ms < 0 && ent->heap_idx != (size_t)-1)
    {
        // setting a negative TTL means removing the TTL
        heap_delete(g_data.heap, ent->heap_idx);
        ent->heap_idx = -1;
    }
    else if (ttl_ms >= 0)
    {
        // add or update the heap data structure
        uint64_t expire_at = get_monotonic_msec() + (uint64_t)ttl_ms;
        HeapItem item = {expire_at, &ent->heap_idx};
        heap_upsert(g_data.heap, ent->heap_idx, item);
    }
}

static bool str2int(const std::string &s, int64_t &out)
{
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

// PEXPIRE key ttl_ms
static void do_expire(std::vector<std::string> &cmd, Buffer &out)
{
    int64_t ttl_ms = 0;
    if (!str2int(cmd[2], ttl_ms))
    {
        return out_err(out, ERR_BAD_ARG, "expect int64");
    }

    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (node)
    {
        Entry *ent = container_of(node, Entry, node);
        entry_set_ttl(ent, ttl_ms);
    }
    return out_int(out, node ? 1 : 0);
}

// PTTL key
static void do_ttl(std::vector<std::string> &cmd, Buffer &out)
{
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());

    HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!node)
    {
        return out_int(out, -2); // not found
    }

    Entry *ent = container_of(node, Entry, node);
    if (ent->heap_idx == (size_t)-1)
    {
        return out_int(out, -1); // no TTL
    }

    uint64_t expire_at = g_data.heap[ent->heap_idx].val;
    uint64_t now_ms = get_monotonic_msec();
    return out_int(out, expire_at > now_ms ? (expire_at - now_ms) : 0);
}

static bool cb_keys(HNode *node, void *arg)
{
    Buffer &out = *(Buffer *)arg;
    const std::string &key = container_of(node, Entry, node)->key;
    out_str(out, key.data(), key.size());
    return true;
}

static void do_keys(std::vector<std::string> &, Buffer &out)
{
    out_arr(out, (uint32_t)hm_size(&g_data.db));
    hm_foreach(&g_data.db, &cb_keys, (void *)&out);
}

static bool str2dbl(const std::string &s, double &out)
{
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}

// zadd zset score name
static void do_zadd(std::vector<std::string> &cmd, Buffer &out)
{
    double score = 0;
    if (!str2dbl(cmd[2], score))
    {
        return out_err(out, ERR_BAD_ARG, "expect float");
    }

    // look up or create the zset
    LookupKey key;
    key.key.swap(cmd[1]);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);

    Entry *ent = NULL;
    if (!hnode)
    { // insert a new key
        ent = entry_new(T_ZSET);
        ent->key.swap(key.key);
        ent->node.hcode = key.node.hcode;
        hm_insert(&g_data.db, &ent->node);
    }
    else
    { // check the existing key
        ent = container_of(hnode, Entry, node);
        if (ent->type != T_ZSET)
        {
            return out_err(out, ERR_BAD_TYP, "expect zset");
        }
    }

    // add or update the tuple
    const std::string &name = cmd[3];
    bool added = zset_insert(&ent->zset, name.data(), name.size(), score);
    return out_int(out, (int64_t)added);
}

static const ZSet k_empty_zset;

static ZSet *expect_zset(std::string &s)
{
    LookupKey key;
    key.key.swap(s);
    key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
    HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
    if (!hnode)
    { // a non-existent key is treated as an empty zset
        return (ZSet *)&k_empty_zset;
    }
    Entry *ent = container_of(hnode, Entry, node);
    return ent->type == T_ZSET ? &ent->zset : NULL;
}

// zrem zset name
static void do_zrem(std::vector<std::string> &cmd, Buffer &out)
{
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
    {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(zset, name.data(), name.size());
    if (znode)
    {
        zset_delete(zset, znode);
    }
    return out_int(out, znode ? 1 : 0);
}

// zscore zset name
static void do_zscore(std::vector<std::string> &cmd, Buffer &out)
{
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
    {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    const std::string &name = cmd[2];
    ZNode *znode = zset_lookup(zset, name.data(), name.size());
    return znode ? out_dbl(out, znode->score) : out_nil(out);
}

// zquery zset score name offset limit
static void do_zquery(std::vector<std::string> &cmd, Buffer &out)
{
    // parse args
    double score = 0;
    if (!str2dbl(cmd[2], score))
    {
        return out_err(out, ERR_BAD_ARG, "expect fp number");
    }
    const std::string &name = cmd[3];
    int64_t offset = 0, limit = 0;
    if (!str2int(cmd[4], offset) || !str2int(cmd[5], limit))
    {
        return out_err(out, ERR_BAD_ARG, "expect int");
    }

    // get the zset
    ZSet *zset = expect_zset(cmd[1]);
    if (!zset)
    {
        return out_err(out, ERR_BAD_TYP, "expect zset");
    }

    // seek to the key
    if (limit <= 0)
    {
        return out_arr(out, 0);
    }
    ZNode *znode = zset_seekge(zset, score, name.data(), name.size());
    znode = znode_offset(znode, offset);

    // output
    size_t ctx = out_begin_arr(out);
    int64_t n = 0;
    while (znode && n < limit)
    {
        out_str(out, znode->name, znode->len);
        out_dbl(out, znode->score);
        znode = znode_offset(znode, +1);
        n += 2;
    }
    out_end_arr(out, ctx, (uint32_t)n);
}

static void do_request(std::vector<std::string> &cmd, Buffer &out)
{
    if (cmd.size() == 2 && cmd[0] == "get")
    {
        return do_get(cmd, out);
    }
    else if (cmd.size() == 3 && cmd[0] == "set")
    {
        return do_set(cmd, out);
    }
    else if (cmd.size() == 2 && cmd[0] == "del")
    {
        return do_del(cmd, out);
    }
    else if (cmd.size() == 3 && cmd[0] == "pexpire")
    {
        return do_expire(cmd, out);
    }
    else if (cmd.size() == 2 && cmd[0] == "pttl")
    {
        return do_ttl(cmd, out);
    }
    else if (cmd.size() == 1 && cmd[0] == "keys")
    {
        return do_keys(cmd, out);
    }
    else if (cmd.size() == 4 && cmd[0] == "zadd")
    {
        return do_zadd(cmd, out);
    }
    else if (cmd.size() == 3 && cmd[0] == "zrem")
    {
        return do_zrem(cmd, out);
    }
    else if (cmd.size() == 3 && cmd[0] == "zscore")
    {
        return do_zscore(cmd, out);
    }
    else if (cmd.size() == 6 && cmd[0] == "zquery")
    {
        return do_zquery(cmd, out);
    }
    else
    {
        return out_err(out, ERR_UNKNOWN, "unknown command.");
    }
}

static void response_begin(Buffer &out, size_t *header)
{
    *header = out.size();   // messege header position
    buf_append_u32(out, 0); // reserve space
}
static size_t response_size(Buffer &out, size_t header)
{
    return out.size() - header - 4;
}
static void response_end(Buffer &out, size_t header)
{
    size_t msg_size = response_size(out, header);
    if (msg_size > k_max_msg)
    {
        out.resize(header + 4);
        out_err(out, ERR_TOO_BIG, "response is too big.");
        msg_size = response_size(out, header);
    }
    // message header
    uint32_t len = (uint32_t)msg_size;
    memcpy(&out[header], &len, 4);
}

// application callback when the socket is writable
static void handle_write(Conn *conn)
{
    assert(conn->outgoing.size() > 0);
    ssize_t rv = send(conn->fd, &conn->outgoing[0], conn->outgoing.size(), MSG_NOSIGNAL);
    if (rv < 0 && errno == EAGAIN)
    {
        return; // actually not ready
    }
    if (rv < 0)
    {
        msg_errno("send() error");
        conn->want_close = true; // error handling
        return;
    }

    // remove written data from `outgoing`
    buf_consume(conn->outgoing, (size_t)rv);

    // update the readiness intention
    if (conn->outgoing.size() == 0)
    { // all data written
        conn->want_read = true;
        conn->want_write = false;
    } // else: want write
}

// application callback when the socket is readable
static void handle_read(Conn *conn)
{
    // read some data
    uint8_t buf[64 * 1024];
    ssize_t rv = read(conn->fd, buf, sizeof(buf));
    if (rv < 0 && errno == EAGAIN)
    {
        return; // actually not ready
    }
    // handle IO error
    if (rv < 0)
    {
        msg_errno("read() error");
        conn->want_close = true;
        return; // want close
    }
    // handle EOF
    if (rv == 0)
    {
        if (conn->incoming.size() == 0)
        {
            msg("client closed");
        }
        else
        {
            msg("unexpected EOF");
        }
        conn->want_close = true;
        return; // want close
    }
    // got some new data
    buf_append(conn->incoming, buf, (size_t)rv);

    // parse requests and generate responses
    while (try_one_http_request(conn))
    {
    }
    // Q: Why calling this in a loop? See the explanation of "pipelining".

    // update the readiness intention
    if (conn->outgoing.size() > 0)
    { // has a response
        conn->want_read = false;
        conn->want_write = true;
        // The socket is likely ready to write in a request-response protocol,
        // try to write it without waiting for the next iteration.
        return handle_write(conn);
    } // else: want read
}

static uint32_t next_timer_ms()
{
    uint64_t now_ms = get_monotonic_msec();
    uint64_t next_ms = (uint64_t)-1;
    // idle timers using a linked list
    if (!dlist_empty(&g_data.idle_list))
    {
        Conn *conn = container_of(g_data.idle_list.next, Conn, idle_node);
        next_ms = conn->last_active_ms + k_idle_timeout_ms;
    }
    // TTL timers using a heap
    if (!g_data.heap.empty() && g_data.heap[0].val < next_ms)
    {
        next_ms = g_data.heap[0].val;
    }
    // timeout value
    if (next_ms == (uint64_t)-1)
    {
        return -1; // no timers, no timeouts
    }
    if (next_ms <= now_ms)
    {
        return 0; // missed?
    }
    return (int32_t)(next_ms - now_ms);
}

static void process_timers()
{
    uint64_t now_ms = get_monotonic_msec();

    // Count active connections first
    size_t active_conns = 0;
    for (Conn *conn : g_data.fd2conn)
    {
        if (conn)
            active_conns++;
    }

    // idle timers using a linked list - limit how many we close at once
    const size_t max_close_per_cycle = 50; // Limit to 50 connections closed per event loop cycle
    size_t closed_count = 0;

    while (!dlist_empty(&g_data.idle_list) && closed_count < max_close_per_cycle)
    {
        Conn *conn = container_of(g_data.idle_list.next, Conn, idle_node);
        uint64_t next_ms = conn->last_active_ms + k_idle_timeout_ms;
        if (next_ms >= now_ms)
        {
            break; // not expired
        }

        // Only remove truly idle connections
        if (conn->incoming.size() == 0 && conn->outgoing.size() == 0)
        {
            fprintf(stderr, "removing idle connection: %d (active: %zu)\n", conn->fd, active_conns);
            conn_destroy(conn);
            closed_count++;
            active_conns--;
        }
        else
        {
            // Connection has pending data, move to end of list and give it more time
            dlist_detach(&conn->idle_node);
            dlist_insert_before(&g_data.idle_list, &conn->idle_node);
        }
    }

    // TTL timers using a heap
    const size_t k_max_works = 2000;
    size_t nworks = 0;
    while (!g_data.heap.empty() && g_data.heap[0].val < now_ms)
    {
        Entry *ent = container_of(g_data.heap[0].ref, Entry, heap_idx);
        HNode *node = hm_delete(&g_data.db, &ent->node, &hnode_same);
        assert(node == &ent->node);

        // Delete the heap entry properly
        heap_delete(g_data.heap, 0);

        // Delete the key
        entry_del(ent);

        if (nworks++ >= k_max_works)
        {
            // don't stall the server if too many keys are expiring at once
            break;
        }
    }
}

int main()
{
    signal(SIGPIPE, SIG_IGN); // ignore SIGPIPE to prevent connection reset errors

    // initialization
    dlist_init(&g_data.idle_list);
    thread_pool_init(&g_data.thread_pool, 4);

    // the listening socket
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        die("socket()");
    }
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    // bind
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(7171);     // changed port: was 1234
    addr.sin_addr.s_addr = ntohl(0); // wildcard address 0.0.0.0
    int rv = bind(fd, (const sockaddr *)&addr, sizeof(addr));
    if (rv)
    {
        die("bind()");
    }

    // set the listen fd to nonblocking mode
    fd_set_nb(fd);

    // listen
    rv = listen(fd, SOMAXCONN);
    if (rv)
    {
        die("listen()");
    }

    // Add a counter for memory check frequency
    size_t loop_counter = 0;

    // the event loop
    std::vector<struct pollfd> poll_args;
    while (true)
    {
        // prepare the arguments of the poll()
        poll_args.clear();
        // put the listening sockets in the first position
        struct pollfd pfd = {fd, POLLIN, 0};
        poll_args.push_back(pfd);
        // the rest are connection sockets
        for (Conn *conn : g_data.fd2conn)
        {
            if (!conn)
            {
                continue;
            }
            // always poll() for error
            struct pollfd pfd = {conn->fd, POLLERR, 0};
            // poll() flags from the application's intent
            if (conn->want_read)
            {
                pfd.events |= POLLIN;
            }
            if (conn->want_write)
            {
                pfd.events |= POLLOUT;
            }
            poll_args.push_back(pfd);
        }

        // wait for readiness
        int32_t timeout_ms = next_timer_ms();
        int rv = poll(poll_args.data(), (nfds_t)poll_args.size(), timeout_ms);
        if (rv < 0 && errno == EINTR)
        {
            continue; // not an error
        }
        if (rv < 0)
        {
            die("poll");
        }

        // handle the listening socket
        if (poll_args[0].revents)
        {
            handle_accept(fd);
        }

        // handle connection sockets
        for (size_t i = 1; i < poll_args.size(); ++i)
        {
            uint32_t ready = poll_args[i].revents;
            if (ready == 0)
            {
                continue;
            }

            int fd = poll_args[i].fd;
            if (fd >= (int)g_data.fd2conn.size() || !g_data.fd2conn[fd])
            {
                // Skip invalid fd (this can happen if a connection was closed)
                continue;
            }

            Conn *conn = g_data.fd2conn[fd];

            // update the idle timer by moving conn to the end of the list
            conn->last_active_ms = get_monotonic_msec();
            dlist_detach(&conn->idle_node);
            dlist_insert_before(&g_data.idle_list, &conn->idle_node);

            // Check for errors first to avoid processing bad connections
            if (ready & (POLLERR | POLLHUP | POLLNVAL))
            {
                conn_destroy(conn);
                continue;
            }

            // handle IO
            if (ready & POLLIN)
            {
                if (conn->want_read)
                {
                    handle_read(conn); // application logic
                }
            }
            if (ready & POLLOUT)
            {
                if (conn->want_write)
                {
                    handle_write(conn); // application logic
                }
            }

            // Check if the connection should be closed
            if (conn->want_close)
            {
                conn_destroy(conn);
            }
        }

        // Periodically check memory usage (every 100 iterations)
        if (++loop_counter % 100 == 0)
        {
            double mem_usage = get_memory_usage();
            if (mem_usage >= MEM_EVICTION_THRESHOLD)
            {
                evict_keys(); // Proactive eviction based on memory usage
            }
            // Reset counter to prevent overflow
            if (loop_counter > 10000)
            {
                loop_counter = 0;
            }
        }

        // handle timers
        process_timers();
    }
    return 0;
}
