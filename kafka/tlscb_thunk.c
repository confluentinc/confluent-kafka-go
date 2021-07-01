#include "rdkafka_select.h"
#include "tlscb_thunk.h"

int cgoSSLCertVerifyCB(rd_kafka_t *rk, const char *broker_name, int32_t broker_id,
						   int *x509_error, int depth, const char *buf, size_t size,
                           char *errstr, size_t errstr_size, void *opaque) {
	return goSSLCertVerifyCB(rk, (char*)broker_name, broker_id, x509_error, depth,
                                 (char*)buf, size, errstr, errstr_size, opaque);
}

rd_kafka_cert_fetch_cb_res_t cgoSSLCertFetchCB(rd_kafka_t *rk, const char *broker_name, int32_t broker_id,
                                              rd_kafka_ssl_cert_fetch_cb_certs_t *certsp, char *errstr,
                                              size_t errstr_size, void *opaque) {
	return goSSLCertFetchCB(rk, (char*)broker_name, broker_id, certsp, errstr, errstr_size, opaque);
}

int cgo_rd_kafka_conf_set_tls_callbacks(rd_kafka_conf_t *conf) {
    int r;
    r = rd_kafka_conf_set_ssl_cert_verify_cb(conf, cgoSSLCertVerifyCB);
    if (r != RD_KAFKA_CONF_OK) return r;
    r = rd_kafka_conf_set_ssl_cert_fetch_cb(conf, cgoSSLCertFetchCB);
    return r;
}
