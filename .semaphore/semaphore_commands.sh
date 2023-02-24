if [ "$EXPECT_LINK_INFO" = "dynamic" ]; then export GO_TAGS="-tags dynamic"; bash mk/bootstrap-librdkafka.sh ${LIBRDKAFKA_VERSION} tmp-build; fi
for dir in kafka examples ; do (cd $dir && go install $GO_TAGS ./...) ; done
if [[ -f .do_lint ]]; then golint -set_exit_status ./examples/... ./kafka/... ./kafkatest/... ./soaktest/... ./schemaregistry/...; fi
for dir in kafka schemaregistry ; do (cd $dir && go test -timeout 180s -v $GO_TAGS ./...) ; done
(cd kafka/testresources && docker-compose up && cd .. && sleep 30 && go test -v $GO_TAGS -timeout 3600s -run  ^TestIntegration$ --onSemaphore true ; cd ..)
go-kafkacat --help
library-version
(library-version | grep "$EXPECT_LINK_INFO") || (echo "Incorrect linkage, expected $EXPECT_LINK_INFO" ; false)
