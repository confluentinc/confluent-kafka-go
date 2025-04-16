set -e
coverage_profile="static_integration_coverage.txt"
if [ "$EXPECT_LINK_INFO" = "dynamic" ]; then export GO_TAGS="-tags dynamic" && coverage_profile="dynamic_integration_coverage.txt"; bash mk/bootstrap-librdkafka.sh ${LIBRDKAFKA_VERSION} tmp-build; fi
for dir in kafka examples ; do (cd $dir && go install $GO_TAGS ./...) ; done
if [[ -f .do_lint ]]; then golint -set_exit_status ./examples/... ./kafka/... ./kafkatest/... ./soaktest/... ./schemaregistry/...; fi
for dir in kafka schemaregistry ; do (cd $dir && go test -coverprofile="$coverage_profile" -timeout 180s -v $GO_TAGS ./...) ; done
(cd kafka && go test -v $GO_TAGS -timeout 3600s -run  ^TestIntegration$ -docker.needed=true ; cd ..)
go-kafkacat --help
library-version
(library-version | grep "$EXPECT_LINK_INFO") || (echo "Incorrect linkage, expected $EXPECT_LINK_INFO" ; false)

(gocovmerge $(find . -type f -iname "*coverage.txt") > ${coverage_profile}) || (echo "Failed to merge coverage files" && exit 0)
artifact push workflow ${coverage_profile} || true