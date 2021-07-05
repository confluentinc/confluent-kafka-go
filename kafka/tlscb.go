package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"strings"
	"time"
	"unsafe"
)

/*
#include "rdkafka_select.h"
#include <stdlib.h>
#include <string.h>

// Helper to do pointer arithmetic to find the correct pointer to pass to
// __str_memcpy
static char* __char_ptr_add(char *base, size_t offset) {
	return base + offset;
}


// helper to cast the void* result of rd_kafka_mem_malloc to char*
static char* __str_rd_kafka_mem_malloc(rd_kafka_t *rk, size_t len) {
	return (char*)rd_kafka_mem_malloc(rk, len);
}

static size_t* __size_t_rd_kafka_mem_malloc(rd_kafka_t *rk, size_t len) {
	return (size_t*)rd_kafka_mem_malloc(rk, len);
}

// helper to do memcpy on char* dests
static void* __str_memcpy(char *dest, void *src, size_t len) {
	return memcpy(dest, src, len);
}

// helper to assign to array
static void __set_size_t_arr_element(size_t *arr, size_t i, size_t value) {
	arr[i] = value;
}
*/
import "C"

//export goSSLCertVerifyCB
func goSSLCertVerifyCB(
	rk *C.rd_kafka_t, brokerName *C.char, brokerID C.int32_t, x509Error *C.int, depth C.int, buf *C.char,
	size C.size_t, errstr *C.char, errstrSize C.size_t, opaque unsafe.Pointer) C.int {

	globalCgoMapLock.Lock()
	h := globalCgoMap[opaque]
	globalCgoMapLock.Unlock()

	cert, err := x509.ParseCertificate(C.GoBytes(unsafe.Pointer(buf), C.int(size)))
	if err != nil {
		e := C.CString(fmt.Sprintf("ParseCertificate failed: %s", err.Error()))
		C.strncpy(errstr, e, errstrSize)
		C.free(unsafe.Pointer(e))
		return 0
	}
	h.tlsLock.RLock()
	verifyOpts := x509.VerifyOptions{
		Intermediates: h.intermediates,
		Roots:         h.tlsConfig.RootCAs,
		CurrentTime:   time.Now(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if int(depth) == 0 && h.verifyBrokerDNS {
		// peer certificate - need to validate CN as well
		// broker name can have host:port format, so split to just get host.
		verifyOpts.DNSName = strings.Split(C.GoString(brokerName), ":")[0]
	}
	chains, err := cert.Verify(verifyOpts)
	h.tlsLock.RUnlock()
	if len(chains) == 0 && err == nil {
		err = errors.New("no path to root")
	}
	if err != nil {
		e := C.CString(fmt.Sprintf("Verify failed: %s", err.Error()))
		C.strncpy(errstr, e, errstrSize)
		C.free(unsafe.Pointer(e))
		return 0
	}
	if cert.IsCA {
		h.tlsLock.Lock()
		h.intermediates.AddCert(cert)
		h.tlsLock.Unlock()
	}
	*x509Error = 0
	return 1
}

//export goSSLCertFetchCB
func goSSLCertFetchCB(
	rk *C.rd_kafka_t, brokerName *C.char, brokerID C.int32_t,
	certsp *C.rd_kafka_ssl_cert_fetch_cb_certs_t, errstr *C.char, errstrSize C.size_t,
	opaque unsafe.Pointer) C.rd_kafka_cert_fetch_cb_res_t {

	globalCgoMapLock.Lock()
	h := globalCgoMap[opaque]
	globalCgoMapLock.Unlock()

	if h.tlsConfig.GetClientCertificate == nil {
		e := C.CString("no GetClientCertificate callback specified")
		C.strncpy(errstr, e, errstrSize)
		C.free(unsafe.Pointer(e))
		return C.RD_KAFKA_CERT_FETCH_ERR
	}
	cert, err := h.tlsConfig.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		e := C.CString(fmt.Sprintf("call to GetClientCertificate failed: %s", err.Error()))
		C.strncpy(errstr, e, errstrSize)
		C.free(unsafe.Pointer(e))
		return C.RD_KAFKA_CERT_FETCH_ERR
	}
	leafBytes := cert.Certificate[0]
	keyBytes, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		e := C.CString(fmt.Sprintf("call to MarshalPKCS8PrivateKey failed: %s", err.Error()))
		C.strncpy(errstr, e, errstrSize)
		C.free(unsafe.Pointer(e))
		return C.RD_KAFKA_CERT_FETCH_ERR
	}
	intermediateCerts := cert.Certificate[1:]

	// For each of these unfortunately we need to copy out of go into C with C.CBytes,
	// then copy it into the actual buffer allocated with rd_kafka_mem_malloc, since we don't
	// know for sure what memory allocator librdkafka is using undedr the hood (in theory).
	certsp.leaf_cert = C.__str_rd_kafka_mem_malloc(rk, C.size_t(len(leafBytes)))
	certsp.leaf_cert_len = C.size_t(len(leafBytes))
	leafBytesC := C.CBytes(leafBytes)
	C.__str_memcpy(certsp.leaf_cert, leafBytesC, C.size_t(len(leafBytes)))
	C.free(leafBytesC)

	certsp.pkey = C.__str_rd_kafka_mem_malloc(rk, C.size_t(len(keyBytes)))
	certsp.pkey_len = C.size_t(len(keyBytes))
	keyBytesC := C.CBytes(keyBytes)
	C.__str_memcpy(certsp.pkey, keyBytesC, C.size_t(len(keyBytes)))
	C.free(keyBytesC)

	if len(intermediateCerts) > 0 {
		chainCertsBufferSize := 0
		for _, chainBytes := range intermediateCerts {
			chainCertsBufferSize += len(chainBytes)
		}

		certsp.chain_certs_cnt = C.int(len(intermediateCerts))
		certsp.chain_certs_buf = C.__str_rd_kafka_mem_malloc(rk, C.size_t(chainCertsBufferSize))
		certsp.chain_cert_lens = C.__size_t_rd_kafka_mem_malloc(rk, C.size_t(C.sizeof_size_t*len(intermediateCerts)))
		bufferIx := 0
		for i, chainBytes := range intermediateCerts {
			chainBytesC := C.CBytes(chainBytes)
			C.__str_memcpy(
				C.__char_ptr_add(certsp.chain_certs_buf, C.size_t(bufferIx)),
				chainBytesC, C.size_t(len(chainBytes)),
			)
			C.free(chainBytesC)
			bufferIx += len(chainBytes)
			C.__set_size_t_arr_element(certsp.chain_cert_lens, C.size_t(i), C.size_t(len(chainBytes)))
		}
	}

	certsp.format = C.RD_KAFKA_CERT_ENC_DER
	return C.RD_KAFKA_CERT_FETCH_OK
}
