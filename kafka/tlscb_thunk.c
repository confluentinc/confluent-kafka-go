#include "rdkafka_select.h"
#include "tlscb_thunk.h"

int goSSLCertVerifyCB(rd_kafka_t *rk, char *broker_name, int32_t broker_id,
                          int *x509_error, int depth, char *buf, size_t size,
                          char *errstr, size_t errstr_size, void *opaque);

int cgoSSLCertVerifyCB(rd_kafka_t *rk, const char *broker_name, int32_t broker_id,
						   int *x509_error, int depth, const char *buf, size_t size,
                           char *errstr, size_t errstr_size, void *opaque) {
	return goSSLCertVerifyCB(rk, (char*)broker_name, broker_id, x509_error, depth,
                                 (char*)buf, size, errstr, errstr_size, opaque);
}

int goSSLCertFetchCB(rd_kafka_t *rk, char *broker_name, int32_t broker_id,
                         char *buf, size_t *buf_size, char **leaf_cert, size_t *leaf_cert_size,
                         char **pkey, size_t *pkey_size, char **chain_certs,
				         size_t *chain_cert_sizes, rd_kafka_cert_enc_t *format,
						 void *opaque);

int cgoSSLCertFetchCB(rd_kafka_t *rk, const char *broker_name, int32_t broker_id,
                          char *buf, size_t *buf_size, char **leaf_cert, size_t *leaf_cert_size,
                          char **pkey, size_t *pkey_size, char *chain_certs[16],
				          size_t chain_cert_sizes[16], rd_kafka_cert_enc_t *format,
						  void *opaque) {
	return goSSLCertFetchCB(rk, (char*)broker_name, broker_id, buf, buf_size, leaf_cert, leaf_cert_size,
							    pkey, pkey_size, chain_certs, chain_cert_sizes, format, opaque);
}

int cgo_rd_kafka_conf_set_tls_callbacks(rd_kafka_conf_t *conf) {
    int r;
    r = rd_kafka_conf_set_ssl_cert_verify_cb(conf, cgoSSLCertVerifyCB);
    if (r != RD_KAFKA_CONF_OK) return r;
    r = rd_kafka_conf_set_ssl_cert_fetch_cb(conf, cgoSSLCertFetchCB);
    return r;
}
