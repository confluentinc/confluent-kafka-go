#include "rdkafka_select.h"

int goSSLCertVerifyCB(rd_kafka_t *rk, char *broker_name, int32_t broker_id,
                          int *x509_error, int depth, char *buf, size_t size,
                          char *errstr, size_t errstr_size, void *opaque);


rd_kafka_cert_fetch_cb_res_t goSSLCertFetchCB(rd_kafka_t *rk, char *broker_name, int32_t broker_id,
                                               rd_kafka_ssl_cert_fetch_cb_certs_t *certsp, char *errstr,
                                               size_t errstr_size, void *opaque);

int cgo_rd_kafka_conf_set_tls_callbacks(rd_kafka_conf_t *conf);
