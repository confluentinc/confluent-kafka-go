package kafka

import (
	"testing"
	"net/url"
	"encoding/base64"
)

var authTests = []struct {
	input ConfigMap
	expected string
}{
	{ ConfigMap{
		"basic.auth.credentials.source": "url",
		"url": "https://user_url:password_url@schema_registry1:8083"},
		"user_url:password_url"},
	{ ConfigMap{
		"basic.auth.credentials.source": "user_info",
		"url": "https://user_url:password_url@schema_registry1:8083",
		"basic.auth.user.info": "user_user_info:password_user_info"},
		"user_user_info:password_user_info"},
	{ ConfigMap{
		"basic.auth.credentials.source": "sasl_inherit",
		"url": "https://user_url:password_url@schema_registry1:8083",
		"sasl.username": "user_sasl_inherit",
		"sasl.password": "password_sasl_inherit",
		"sasl.mechanism": "PLAIN"},
		"user_sasl_inherit:password_sasl_inherit"},
}

func TestConfigureBasicAuth(t *testing.T) {
	for _, test := range authTests {
		u, _ := url.Parse(test.input.GetString("url", ""))
		ui, _ := configureAuth(u, test.input)
		if ui != base64.StdEncoding.EncodeToString([]byte(test.expected)) {
			actual, _ := base64.StdEncoding.DecodeString(ui)
			t.Logf("FAILED: Got %s expected %s", actual, test.expected)
			t.Fail()
		}
	}
}

