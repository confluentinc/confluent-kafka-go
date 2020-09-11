#include "rdkafka_select.h"

int cgoSSLCertVerifyCB(rd_kafka_t *rk, const char *broker_name, int32_t broker_id,
						   int *x509_error, int depth, const char *buf, size_t size,
                           char *errstr, size_t errstr_size, void *opaque);


int cgoSSLCertFetchCB(rd_kafka_t *rk, const char *broker_name, int32_t broker_id,
                          char *buf, size_t *buf_size, char **leaf_cert, size_t *leaf_cert_size,
                          char **pkey, size_t *pkey_size, char *chain_certs[16],
				          size_t chain_cert_sizes[16], rd_kafka_cert_enc_t *format,
						  void *opaque);

int cgo_rd_kafka_conf_set_tls_callbacks(rd_kafka_conf_t *conf);
