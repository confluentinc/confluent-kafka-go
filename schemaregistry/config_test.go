package schemaregistry

import (
	"testing"
)

func TestConfigWithAuthentication(t *testing.T) {
	maybeFail = initFailFunc(t)

	c := NewConfigWithAuthentication("mock://", "username", "password")

	maybeFail("BasicAuthCredentialsSource", expect(c.BasicAuthCredentialsSource, "USER_INFO"))
	maybeFail("BasicAuthUserInfo", expect(c.BasicAuthUserInfo, "username:password"))
}
