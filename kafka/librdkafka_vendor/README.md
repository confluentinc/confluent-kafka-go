# Bundling prebuilt librdkafka

confluent-kafka-go bundles prebuilt statically linked
versions of librdkafka for the following platforms:

 * MacOSX x64      (aka Darwin)
 * Linux glibc x64 (Ubuntu, CentOS, etc)
 * Linux musl x64  (Alpine)

## Import static librdkafka bundle

First create the static librdkafka bundle following the instructions in
librdkafka's packaging/nuget/README.md.

Then import the new version by using the import.sh script here, this script
will create a branch, import the bundle, create a commit and push the
branch to Github for PR review. This PR must be manually opened, reviewed
and then finally merged (make sure to merge it, DO NOT squash or rebase).

    $ ./import.sh ~/path/to/librdkafka-static-bundle-v1.4.0.tgz

This will copy the static library and the rdkafka.h header file
to this directory, as well as generate a new ../build_..go file
for this platform + variant.
