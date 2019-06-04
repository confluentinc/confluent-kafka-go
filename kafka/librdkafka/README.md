# Bundling prebuilt librdkafka

confluent-kafka-go bundles prebuilt statically linked
versions of librdkafka for the following platforms:

 * MacOSX x64      (aka Darwin)
 * Linux glibc x64 (Ubuntu, CentOS, etc)
 * Linux musl x64  (Alpine)

## Update bundled libraries

On each platform where a native static librdkafka has been built,
run setup.sh from this directory, passing the librdkafka build directory
as well as the platform (linux or darwin (osx)), and in the case of linux
also if the build was using glibc or musl (alpine).

    # On OSX:
    $ ./setup.sh ~/src/librdkafka darwin

    # On Ubuntu, CentOS, et.al:
    $ ./setup.sh ~/src/librdkafka linux glibc

    # On Alpine:
    $ ./setup.sh ~/src/librdkafka linux musl


This will copy the static library and the rdkafka.h header file
to this directory, as well as generate a new ../build_..go file
for this platform + variant.

Repeat the process for darwin, linux glibc and linux alpine.

**IMPORTANT**: The librdkafka version (tag) must be identical for
               all platforms and variants.

