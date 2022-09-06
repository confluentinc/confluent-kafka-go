package kafka

/*
#include <stdlib.h>
#include <string.h>
#include "select_rdkafka.h"


// Copies the string in src to dest. If it does not fit, sets *dest_size to the required
// size and *rc to -1. If dest is NULL this function does nothing.
static void copy(char *dest, char *src, size_t *dest_size, int *rc) {
  if (!dest) {
    return;
  }

  int src_size = strlen(src) + 1;

  if (src_size > *dest_size) {
    *dest_size = src_size;
    *rc = -1;
  } else {
    strcpy(dest, src);
  }
}


// This is the librdkafka SASL/PLAIN credentials callback used for all Go Kafka clients. Since all
// Go clients use this as their registered callback (using C.rd_kafka_conf_set_plain_creds_cb) this
// implementation disambiguates between the client by their address and calls the correct Go callback
// method that was passed in through the "plain.creds.cb" field in the ConfigMap.
static int plain_creds_cb(rd_kafka_t *rk, char *username, size_t *username_size, char *password, size_t *password_size) {
  // This function (in Go) calls the right Go callback for this client. The username and password fields are allocated
  // by the function and need to be freed by the caller (i.c: us).
  extern void callPlainCredsCallback(int *rk, char **username, char **password, char **errmsg);
  char *go_username;
  char *go_password;
  char *errmsg;


  // Calls the Go callback (through this intermediary function).
  callPlainCredsCallback((int *)rk, &go_username, &go_password, &errmsg);

  if (errmsg) {
    // TODO: Find a better way to log this error message.
    fprintf(stderr,"Go callback error: %s\n", errmsg);
    free(errmsg);
    return -1;
  }

 // The return code for this function. 0 means a username and/or password was returned. -1 means that the
  // username or password did not fit in the buffer provided by the caller. In that case *username_size and/or
  // *password_size are set to the number of bytes required (including termininating '\0').
  int rc = 0;

  // If the Go callback succeeded, copies the username and password to the caller
  // provided areas and frees the buffers allocated by the intermediary.
  copy(username, go_username, username_size, &rc);
  free(go_username);
  copy(password, go_password, password_size, &rc);
  free(go_password);

  return rc;
}

// Sets the one-callback-function-to-rule-them-all into the Kafka configuration map.
void set_plain_creds_cb(rd_kafka_conf_t *conf) {
  rd_kafka_conf_set_plain_creds_cb(conf, plain_creds_cb);
}
*/
import "C"
