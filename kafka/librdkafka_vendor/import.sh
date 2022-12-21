#!/bin/bash
#
#
# Import a new version of librdkafka based on a librdkafka static bundle.
# This will create a separate branch, import librdkafka, make a commit,
# and then ask you to push the branch to github, have it reviewed,
# and then later merged (NOT squashed or rebased).
# Having a merge per import allows future shallow clones to skip and ignore
# older imports, hopefully reducing the amount of git history data 'go get'
# needs to download.

set -e

usage() {
    echo "Usage: $0 [--devel] path/to/librdkafka-static-bundle-<VERSION>.tgz"
    echo ""
    echo "This tool must be run from the TOPDIR/kafka/librdkafka directory"
    echo ""
    echo "Options:"
    echo "  --devel  - Development use: No branch checks and does not push to github"
    exit 1
}

error_cleanup() {
    echo "Error occurred, cleaning up"
    git checkout $currbranch
    git branch -D $import_branch
    exit 1
}

devel=0
if [[ $1 == --devel ]]; then
    devel=1
    shift
fi

bundle="$1"
[[ -f $bundle ]] || usage

# Parse the librdkafka version from the bundle
bundlename=$(basename $bundle)
version=${bundlename#librdkafka-static-bundle-}
version=${version%.tgz}

if [[ -z $version ]]; then
    echo "Error: Could not parse version from bundle $bundle"
    exit 1
fi

# Verify branch state
curr_branch=$(git symbolic-ref HEAD 2>/dev/null | cut -d"/" -f 3-)
uncommitted=$(git status --untracked-files=no --porcelain)

if [[ ! -z $uncommitted ]]; then
    echo "Error: This script must be run on a clean branch with no uncommitted changes"
    echo "Uncommitted files:"
    echo "$uncommitted"
    exit 1
fi

if [[ $devel != 1 ]] && [[ $curr_branch != master ]] ; then
    echo "Error: This script must be run on an up-to-date, clean, master branch"
    exit 1
fi


# Create import branch, import bundle, commit.
import_branch="import_$version"

exists=$(git branch -rlq | grep "/$import_branch\$" || true)
if [[ ! -z $exists ]]; then
    echo "Error: This version branch already seems to exist: $exists: already imorted?"
    [[ $devel != 1 ]] && exit 1
fi

echo "Checking for existing commits that match this version (should be none)"
git log --oneline | grep "^librdkafka static bundle $version\$" && exit 1


echo "Creating import branch $import_branch"
git checkout -b $import_branch

echo "Importing bundle $bundle"
./bundle-import.sh "$bundle" || error_cleanup

echo "Committing $version"
git commit -a -m "librdkafka static bundle $version" || error_cleanup

echo "Updating error codes and docs"
pushd ../../
make -f mk/Makefile docs || error_cleanup
git commit -a -m "Documentation and error code update for librdkafka $version" \
    || error_cleanup
popd

if [[ $devel != 1 ]]; then
    echo "Pushing branch"
    git push origin $import_branch || error_cleanup
fi

git checkout $curr_branch

if [[ $devel != 1 ]]; then
    git branch -D $import_branch
fi

echo ""
echo "############## IMPORT OF $version COMPLETE ##############"
if [[ $devel != 1 ]]; then
    echo "Branch $import_branch has been pushed."
    echo "Create a PR, have it reviewed and then merge it (do NOT squash or rebase)."
fi

