# Information for confluent-kafka-go developers

Whenever librdkafka error codes are updated make sure to run generate
before building:

```
  $ make -f mk/Makefile generr
  $ go build ./...
```




## Testing

Some of the tests included in this directory, the benchmark and integration tests in particular,
require an existing Kafka cluster and a testconf.json configuration file to
provide tests with bootstrap brokers, topic name, etc.

The format of testconf.json is a JSON object:
```
{
  "Brokers": "<bootstrap-brokers>",
  "Topic": "<test-topic-name>"
}
```

See testconf-example.json for an example and full set of available options.


To run unit-tests:
```
$ go test
```

To run benchmark tests:
```
$ go test -bench .
```

For the code coverage:
```
$ go test -coverprofile=coverage.out -bench=.
$ go tool cover -func=coverage.out
```

## Build tags (static linking)


Different build types are supported through Go build tags (`-tags ..`),
these tags should be specified on the **application** build command.

 * `static` - Build with librdkafka linked statically (but librdkafka
              dependencies linked dynamically).
 * `static_all` - Build with all libraries linked statically.
 * neither - Build with librdkafka (and its dependencies) linked dynamically.



## Generating HTML documentation

To generate one-page HTML documentation run the mk/doc-gen.py script from the
top-level directory. This script requires the beautifulsoup4 Python package.

```
$ source .../your/virtualenv/bin/activate
$ pip install beautifulsoup4
...
$ make -f mk/Makefile docs
```


## Release process

For each release candidate and final release, perform the following steps:

### Install librdkafka

Make sure the librdkafka version installed is the version this release
is aimed to be used with.

E.g., confluent-kafka-go v1.0.0 will require librdkafka v1.0.0.


### Update librdkafka version requirement

Update the minimum required librdkafka version in `kafka/00version.go`
and `README.md`.


### Update error codes

Error codes can be automatically generated from the current librdkafka version.


Update generated error codes:

    $ make -f mk/Makefile generr
    # Verify by building


### Rebuild everything

    $ go clean -i ./...
    $ go build ./...


### Run full test suite

Set up a test cluster using whatever mechanism you typically use
(docker, trivup, ccloud, ..).

Make sure to update `kafka/testconf.json` as needed (broker list, $BROKERS)

Run test suite:

    $ go test ./...


### Verify examples

Manually verify that the examples/ applications work.

Also make sure the examples in README.md work.


### Commit any changes

Make sure to push to github before creating the tag to have CI tests pass.


### Create and push tag

    $ git tag v1.0.0-RC3
    $ git push --dry-run origin v1.0.0-RC3
    # Remove --dry-run and re-execute if it looks ok.


### Create release notes page on github
