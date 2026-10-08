/**
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"runtime/cgo"
	"strconv"
	"sync/atomic"
	"unsafe"
)

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#endif
#include "select_rdkafka.h"

typedef struct ckgo_resolve_entry_s {
        char *host;    // host and port as librdkafka looks them up
        char *port;
        char *to_host; // numeric address they resolve to
        char *to_port;
} ckgo_resolve_entry_t;

// Address resolution state of a client, set as the librdkafka opaque.
// It must outlive rd_kafka_destroy().
typedef struct ckgo_resolve_s {
        ckgo_resolve_entry_t *entries; // go.resolve.map
        size_t entry_cnt;
        uintptr_t go_cb;               // cgo.Handle of go.resolve.cb, or 0
        int64_t calls;                 // lookups
        int64_t results;               // addrinfo lists handed to librdkafka
        int64_t frees;                 // addrinfo lists librdkafka freed
} ckgo_resolve_t;

extern int ckgoResolveGo(uintptr_t h, char *node, char *service,
                         char **to_node, char **to_service, int *use_default);

static int ckgo_strcaseeq(const char *a, const char *b) {
        for (; *a && *b; a++, b++) {
                char ca = *a, cb = *b;
                if (ca >= 'A' && ca <= 'Z')
                        ca += 'a' - 'A';
                if (cb >= 'A' && cb <= 'Z')
                        cb += 'a' - 'A';
                if (ca != cb)
                        return 0;
        }
        return *a == *b;
}

// Resolves a numeric address, keeping librdkafka's family, socket type and
// protocol hints. AI_ADDRCONFIG is dropped: the application chose the
// address, which is typically a loopback one that AI_ADDRCONFIG may filter.
static int ckgo_getaddrinfo_numeric(const char *node, const char *service,
                                    const struct addrinfo *hints,
                                    struct addrinfo **res) {
        struct addrinfo h;
        memset(&h, 0, sizeof(h));
        if (hints) {
                h.ai_family   = hints->ai_family;
                h.ai_socktype = hints->ai_socktype;
                h.ai_protocol = hints->ai_protocol;
        }
        h.ai_flags = AI_NUMERICHOST;
#ifdef AI_NUMERICSERV
        h.ai_flags |= AI_NUMERICSERV;
#endif
        return getaddrinfo(node, service, &h, res);
}

// librdkafka resolve_cb: go.resolve.map first, then go.resolve.cb, then the
// system resolver. Called from librdkafka's broker and main threads.
static int ckgo_resolve_cb(const char *node, const char *service,
                           const struct addrinfo *hints,
                           struct addrinfo **res, void *opaque) {
        ckgo_resolve_t *r = (ckgo_resolve_t *)opaque;
        size_t i;
        int err;

        if (!node && !service && !hints) {
                if (*res) {
                        freeaddrinfo(*res);
                        *res = NULL;
                        __atomic_fetch_add(&r->frees, 1, __ATOMIC_RELAXED);
                }
                return 0;
        }

        __atomic_fetch_add(&r->calls, 1, __ATOMIC_RELAXED);

        for (i = 0; i < r->entry_cnt; i++) {
                const ckgo_resolve_entry_t *e = &r->entries[i];
                if (ckgo_strcaseeq(node, e->host) && service &&
                    !strcmp(service, e->port)) {
                        err = ckgo_getaddrinfo_numeric(e->to_host, e->to_port,
                                                       hints, res);
                        goto done;
                }
        }

        if (r->go_cb) {
                char *to_node = NULL, *to_service = NULL;
                int use_default = 0;
                err = ckgoResolveGo(r->go_cb, (char *)node,
                                    (char *)(service ? service : ""),
                                    &to_node, &to_service, &use_default);
                if (!err && !use_default)
                        err = ckgo_getaddrinfo_numeric(to_node, to_service,
                                                       hints, res);
                free(to_node);
                free(to_service);
                if (err || !use_default)
                        goto done;
        }

        err = getaddrinfo(node, service, hints, res);

done:
        if (!err)
                __atomic_fetch_add(&r->results, 1, __ATOMIC_RELAXED);
        return err;
}

static void ckgo_conf_set_resolve(rd_kafka_conf_t *conf, ckgo_resolve_t *r) {
        rd_kafka_conf_set_opaque(conf, r);
        rd_kafka_conf_set_resolve_cb(conf, ckgo_resolve_cb);
}

static void ckgo_resolve_stats(ckgo_resolve_t *r, int64_t *calls,
                               int64_t *results, int64_t *frees) {
        *calls   = __atomic_load_n(&r->calls, __ATOMIC_RELAXED);
        *results = __atomic_load_n(&r->results, __ATOMIC_RELAXED);
        *frees   = __atomic_load_n(&r->frees, __ATOMIC_RELAXED);
}

static void ckgo_resolve_destroy(ckgo_resolve_t *r) {
        size_t i;
        for (i = 0; i < r->entry_cnt; i++) {
                free(r->entries[i].host);
                free(r->entries[i].port);
                free(r->entries[i].to_host);
                free(r->entries[i].to_port);
        }
        free(r->entries);
        free(r);
}
*/
import "C"

// ResolveCallback resolves the host and port of a Kafka broker to the
// numeric address librdkafka connects to, in place of the system resolver.
// It is set with the go.resolve.cb configuration property.
//
// host and port are as librdkafka looks them up: from bootstrap.servers,
// and then as advertised by the brokers. addrHost must be a numeric IPv4 or
// IPv6 address; an empty addrPort keeps port. Returning an empty addrHost and
// a nil error leaves the lookup to the system resolver.
//
// TLS still uses the broker's host name, for both SNI and the
// verification of the broker's certificate, whatever address the callback
// returns.
//
// The callback is called from librdkafka's internal threads, concurrently
// for different brokers. It blocks connecting to the broker until it
// returns, so it should return quickly, and it must not call the client,
// in particular not Close(). It is called again whenever librdkafka
// connects to a broker once broker.address.ttl has expired. librdkafka
// reports an error returned by the callback as a failure to resolve the
// broker, without the error's message: an error satisfying
// *net.DNSError's IsNotFound is reported as EAI_NONAME, IsTemporary or
// IsTimeout as EAI_AGAIN, and any other error as EAI_FAIL. A panic in the
// callback is recovered and reported as EAI_FAIL.
//
// The go.resolve.map configuration property, a map[string]string of
// "host:port" to numeric "ip:port", maps fixed addresses without calling Go
// code. Its entries take precedence over the callback, and a broker that is
// in neither is resolved by the system resolver.
//
// client.dns.lookup=resolve_canonical_bootstrap_servers_only must not be
// used along with these properties, as it would replace the bootstrap
// servers' host names with the reverse lookup of the resolved addresses.
type ResolveCallback func(host, port string) (addrHost, addrPort string, err error)

// resolver is the address resolution configured for a client with the
// go.resolve.cb and go.resolve.map properties.
type resolver struct {
	c      *C.ckgo_resolve_t
	handle cgo.Handle // of the ResolveCallback, 0 if none

	// Counters, as of destroy(), for tests.
	finalCalls, finalResults, finalFrees int64
}

// liveResolveHandles counts the cgo.Handles held by resolvers, for tests.
var liveResolveHandles atomic.Int64

// errResolvePanic stands for a panic in a ResolveCallback.
var errResolvePanic = errors.New("go.resolve.cb panicked")

// extractResolveConfig extracts go.resolve.cb and go.resolve.map, returning
// nil if neither is set. The resolver must be destroyed once the client
// created with it is destroyed, or if creating it fails.
func (m ConfigMap) extractResolveConfig() (*resolver, error) {
	cbv, err := m.extract("go.resolve.cb", nil)
	if err != nil {
		return nil, err
	}
	mapv, err := m.extract("go.resolve.map", nil)
	if err != nil {
		return nil, err
	}

	var cb ResolveCallback
	switch x := cbv.(type) {
	case nil:
	case ResolveCallback:
		cb = x
	case func(string, string) (string, string, error):
		cb = x
	default:
		return nil, newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("go.resolve.cb expects type kafka.ResolveCallback, not %T", cbv))
	}

	var addrs map[string]string
	switch x := mapv.(type) {
	case nil:
	case map[string]string:
		addrs = x
	default:
		return nil, newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("go.resolve.map expects type map[string]string, not %T", mapv))
	}

	if cb == nil && len(addrs) == 0 {
		return nil, nil
	}
	return newResolver(cb, addrs)
}

func splitResolveAddr(key, addr string, numeric bool) (host, port string, err error) {
	host, port, err = net.SplitHostPort(addr)
	if err == nil && host == "" {
		err = errors.New("missing host")
	}
	if err == nil {
		_, err = strconv.ParseUint(port, 10, 16)
	}
	if err == nil && numeric {
		_, err = netip.ParseAddr(host)
	}
	if err != nil {
		return "", "", newErrorFromString(ErrInvalidArg,
			fmt.Sprintf("go.resolve.map: invalid %s %q: %s", key, addr, err))
	}
	return host, port, nil
}

func newResolver(cb ResolveCallback, addrs map[string]string) (*resolver, error) {
	type entry struct{ host, port, toHost, toPort string }
	entries := make([]entry, 0, len(addrs))
	for from, to := range addrs {
		var e entry
		var err error
		if e.host, e.port, err = splitResolveAddr("host:port", from, false); err != nil {
			return nil, err
		}
		if e.toHost, e.toPort, err = splitResolveAddr("numeric ip:port", to, true); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}

	r := &resolver{}
	r.c = (*C.ckgo_resolve_t)(C.calloc(1, C.sizeof_ckgo_resolve_t))
	if len(entries) > 0 {
		r.c.entries = (*C.ckgo_resolve_entry_t)(C.calloc(C.size_t(len(entries)), C.sizeof_ckgo_resolve_entry_t))
		cEntries := unsafe.Slice(r.c.entries, len(entries))
		for i, e := range entries {
			cEntries[i].host = C.CString(e.host)
			cEntries[i].port = C.CString(e.port)
			cEntries[i].to_host = C.CString(e.toHost)
			cEntries[i].to_port = C.CString(e.toPort)
		}
		r.c.entry_cnt = C.size_t(len(entries))
	}
	if cb != nil {
		r.handle = cgo.NewHandle(cb)
		liveResolveHandles.Add(1)
		r.c.go_cb = C.uintptr_t(r.handle)
	}
	return r, nil
}

// apply sets the resolver as cConf's resolve_cb, and as its opaque.
func (r *resolver) apply(cConf *C.rd_kafka_conf_t) {
	C.ckgo_conf_set_resolve(cConf, r.c)
}

// stats returns the number of lookups, of results handed to librdkafka and
// of results librdkafka freed.
func (r *resolver) stats() (calls, results, frees int64) {
	var cCalls, cResults, cFrees C.int64_t
	C.ckgo_resolve_stats(r.c, &cCalls, &cResults, &cFrees)
	return int64(cCalls), int64(cResults), int64(cFrees)
}

// destroy releases the resolver. It must not be called before the client
// using it is destroyed, as librdkafka's threads may still call it.
func (r *resolver) destroy() {
	if r == nil {
		return
	}
	r.finalCalls, r.finalResults, r.finalFrees = r.stats()
	C.ckgo_resolve_destroy(r.c)
	r.c = nil
	if r.handle != 0 {
		r.handle.Delete()
		r.handle = 0
		liveResolveHandles.Add(-1)
	}
}

// resolveErrorCode maps an error returned by a ResolveCallback to a
// getaddrinfo() error code.
func resolveErrorCode(err error) C.int {
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		if dnsErr.IsNotFound {
			return C.EAI_NONAME
		}
		if dnsErr.IsTemporary || dnsErr.IsTimeout {
			return C.EAI_AGAIN
		}
	}
	return C.EAI_FAIL
}
