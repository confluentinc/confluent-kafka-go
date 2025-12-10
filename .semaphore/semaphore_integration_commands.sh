set -e
coverage_profile="static_integration_coverage.txt"
if [ "$EXPECT_LINK_INFO" = "dynamic" ]; then export GO_TAGS="-tags dynamic" && coverage_profile="dynamic_integration_coverage.txt"; bash mk/bootstrap-librdkafka.sh ${LIBRDKAFKA_VERSION} tmp-build; fi
for dir in kafka examples ; do (cd $dir && go install $GO_TAGS ./...) ; done
if [[ -f .do_lint ]]; then golint -set_exit_status ./examples/... ./kafka/... ./kafkatest/... ./soaktest/... ./schemaregistry/...; fi
for dir in kafka schemaregistry ; do (cd $dir && go test -coverprofile="$coverage_profile" -timeout 180s -v $GO_TAGS ./...) ; done
(cd kafka && go test -v $GO_TAGS -timeout 3600s -run  ^TestIntegration$ -docker.needed=true ; cd ..)

# If we're on newer macOS, avoid running binaries that are not code-signed, rather, use go run.
# Running go-kafkacat with `go run` needs some `go get` commands, so just check existence instead.

if [[ $(command -v codesign) ]]; then
    # macOS 15+ with code-signing requirements
    which go-kafkacat
    
    # Run library-version and capture output for validation
    output=$(go run $GO_TAGS examples/library-version/library-version.go)
    echo "$output"
    
    # Validate linkage information
    if ! echo "$output" | grep -q "$EXPECT_LINK_INFO"; then
        echo "Incorrect linkage, expected $EXPECT_LINK_INFO"
        exit 1
    fi
else
    # macOS 13 and older - use pre-built binaries
    go-kafkacat --help
    library-version
    
    # Validate linkage information
    if ! library-version | grep -q "$EXPECT_LINK_INFO"; then
        echo "Incorrect linkage, expected $EXPECT_LINK_INFO"
        exit 1
    fi
fi

(gocovmerge $(find . -type f -iname "*coverage.txt") > ${coverage_profile}) || (echo "Failed to merge coverage files" && exit 0)
artifact push workflow ${coverage_profile} || true
