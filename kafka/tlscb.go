package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"time"
	"unsafe"
)

/*
#include "rdkafka_select.h"
#include <stdlib.h>
#include <string.h>

static char* char_ptr_add(char *base, size_t offset) {
	return base + offset;
}

static char** charptr_ptr_add(char **base, size_t offset) {
	return base + offset;
}

static size_t* sizet_ptr_add(size_t *base, size_t offset) {
	return base + offset;
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
	chains, err := cert.Verify(x509.VerifyOptions{
		Intermediates: h.intermediates,
		Roots:         h.tlsConfig.RootCAs,
		CurrentTime:   time.Now(),
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	if len(chains) == 0 {
		err = errors.New("no path to root")
	}
	if err != nil {
		e := C.CString(fmt.Sprintf("Verify failed: %s", err.Error()))
		C.strncpy(errstr, e, errstrSize)
		C.free(unsafe.Pointer(e))
		return 0
	}
	if cert.IsCA {
		h.intermediates.AddCert(cert)
	}
	*x509Error = 0
	return 1
}

//export goSSLCertFetchCB
func goSSLCertFetchCB(
	rk *C.rd_kafka_t, brokerName *C.char, brokerID C.int32_t, buf *C.char, bufSize *C.size_t,
	leafCert **C.char, leafCertSize *C.size_t, pkey **C.char, pkeySize *C.size_t,
	chainCerts **C.char, chainCertSizes *C.size_t, format *C.rd_kafka_cert_enc_t,
	opaque unsafe.Pointer) C.int {

	globalCgoMapLock.Lock()
	h := globalCgoMap[opaque]
	globalCgoMapLock.Unlock()

	if h.tlsConfig.GetClientCertificate == nil {
		return C.RD_KAFKA_CERT_FETCH_ERR
	}
	cert, err := h.tlsConfig.GetClientCertificate(&tls.CertificateRequestInfo{})
	if err != nil {
		return C.RD_KAFKA_CERT_FETCH_ERR
	}
	leafBytes := cert.Certificate[0]
	keyBytes, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		return C.RD_KAFKA_CERT_FETCH_ERR
	}
	intermediateCerts := cert.Certificate[1:]

	desiredBufferSize := len(leafBytes) + len(keyBytes)
	for _, c := range intermediateCerts {
		desiredBufferSize += len(c)
	}

	if desiredBufferSize > int(*bufSize) {
		*bufSize = C.size_t(desiredBufferSize)
		return C.RD_KAFKA_CERT_FETCH_MORE_BUFFER
	}

	*leafCert = buf
	*leafCertSize = C.size_t(len(leafBytes))
	C.memcpy(unsafe.Pointer(*leafCert), unsafe.Pointer(&leafBytes[0]), *leafCertSize)

	*pkey = C.char_ptr_add(*leafCert, *leafCertSize)
	*pkeySize = C.size_t(len(keyBytes))
	C.memcpy(unsafe.Pointer(*pkey), unsafe.Pointer(&keyBytes[0]), *pkeySize)

	curIntCertPtr := C.char_ptr_add(*pkey, *pkeySize)
	for i := 0; i < 16 && i < len(intermediateCerts); i++ {
		thisIntCertPointerLoc := C.charptr_ptr_add(chainCerts, C.size_t(i))
		thisIntCertSizeLoc := C.sizet_ptr_add(chainCertSizes, C.size_t(i))

		*thisIntCertPointerLoc = curIntCertPtr
		*thisIntCertSizeLoc = C.size_t(len(intermediateCerts[i]))
		C.memcpy(unsafe.Pointer(*thisIntCertPointerLoc), unsafe.Pointer(&intermediateCerts[i][0]), *thisIntCertSizeLoc)
		curIntCertPtr = C.char_ptr_add(*thisIntCertPointerLoc, *thisIntCertSizeLoc)
	}

	*format = C.RD_KAFKA_CERT_ENC_DER

	return 0
}
