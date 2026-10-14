#!/usr/bin/env bash
# This file is part of VoltDB.
# Copyright (C) 2026 Volt Active Data Inc.
#
# Build, test and publish steps shared by jenkins/test/Jenkinsfile and
# jenkins/release/Jenkinsfile, and runnable on a laptop. Every step runs in an
# official python:<version>-slim container, so the host needs only Docker: the
# build agents' own python3 is 3.6, older than the package supports.
#
#   jenkins/ci.sh build                  sdist + wheel into dist/, twine check
#   jenkins/ci.sh test 3.9 3.13 ...      per version: install dist/*.whl into a
#                                        clean venv, run the offline suites
#   jenkins/ci.sh set-version 16.1.0     stamp the version into pyproject.toml
#   jenkins/ci.sh unpublished testpypi|pypi
#                                        fail if the version is already on the index
#   jenkins/ci.sh checksum               dist/SHA256SUMS
#   jenkins/ci.sh upload testpypi|pypi   twine upload dist/* (token in $TWINE_PASSWORD)
#   jenkins/ci.sh verify testpypi|pypi   the index serves exactly the files in dist/
#
# Images come from mirror.gcr.io, Google's Docker Hub mirror, so pulls need no
# Docker Hub login and no credential is ever present while PR code runs.

set -euo pipefail

cd "$(dirname "$0")/.."

IMAGE_REGISTRY="${IMAGE_REGISTRY:-mirror.gcr.io/library}"
BUILD_PYTHON="${BUILD_PYTHON:-3.13}"
PROJECT=voltdbclient

die() { echo "ERROR: $*" >&2; exit 1; }

# in_python <version> <script>: run a bash script in python:<version>-slim with
# the checkout at /src, as the calling user so the workspace stays cleanable.
in_python() {
    local version=$1 script=$2
    docker run --rm \
        --user "$(id -u):$(id -g)" \
        -e HOME=/tmp \
        -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
        -e PYTHONDONTWRITEBYTECODE=1 \
        -e SOURCE_DATE_EPOCH \
        -e TWINE_USERNAME \
        -e TWINE_PASSWORD \
        -v "$PWD:/src" -w /src \
        "${IMAGE_REGISTRY}/python:${version}-slim" \
        bash -euo pipefail -c "$script"
}

project_version() {
    sed -n 's/^version *= *"\(.*\)"$/\1/p' pyproject.toml
}

# index_url <testpypi|pypi> <json|upload>
index_url() {
    case "$1:$2" in
        testpypi:json)   echo "https://test.pypi.org/pypi" ;;
        testpypi:upload) echo "https://test.pypi.org/legacy/" ;;
        pypi:json)       echo "https://pypi.org/pypi" ;;
        pypi:upload)     echo "https://upload.pypi.org/legacy/" ;;
        *)               die "unknown index '$1' (testpypi or pypi)" ;;
    esac
}

cmd_build() {
    rm -rf dist build src/*.egg-info
    # Zip entry timestamps from the commit, not the clock, so a rebuild of the
    # same commit with the same tools produces the same wheel.
    SOURCE_DATE_EPOCH=$(git log -1 --format=%ct)
    export SOURCE_DATE_EPOCH
    local version
    version=$(project_version)
    in_python "$BUILD_PYTHON" "
        python -m venv /tmp/venv
        /tmp/venv/bin/pip install -q build twine
        /tmp/venv/bin/python -m build
        /tmp/venv/bin/twine check --strict dist/*
        python - <<'EOF'
import os, sys, zipfile

wheel = 'dist/${PROJECT}-${version}-py3-none-any.whl'
sdist = 'dist/${PROJECT}-${version}.tar.gz'
built = sorted(os.listdir('dist'))
if built != sorted([os.path.basename(wheel), os.path.basename(sdist)]):
    sys.exit('ERROR: expected exactly %s and %s in dist/, found %s' % (wheel, sdist, built))
# The wheel is what VoltDB's build unpacks into lib/python: both modules, at
# the top level, and nothing else importable.
modules = sorted(n for n in zipfile.ZipFile(wheel).namelist() if '.dist-info/' not in n)
if modules != ['voltclient.py', 'voltdbclient.py']:
    sys.exit('ERROR: unexpected wheel contents: %s' % modules)
print('Built %s and %s' % (wheel, sdist))
EOF"
}

cmd_test() {
    [ $# -gt 0 ] || die "usage: ci.sh test <python-version>..."
    local wheel
    wheel=$(ls dist/*.whl 2>/dev/null) || die "no wheel in dist/: run 'ci.sh build' first"
    mkdir -p reports
    local version
    for version in "$@"; do
        echo "=== Python ${version}: offline suites against ${wheel}"
        # Tests run from tests/ against the installed wheel, never against src/:
        # the import check fails if the modules come from anywhere but the venv.
        in_python "$version" "
            python -m venv /tmp/venv
            /tmp/venv/bin/pip install -q pytest /src/${wheel}
            cd tests
            /tmp/venv/bin/python -c '
import voltclient, voltdbclient
for m in (voltdbclient, voltclient):
    assert m.__file__.startswith(\"/tmp/venv/\"), m.__file__
'
            /tmp/venv/bin/python -m pytest -p no:cacheprovider -q \
                --junitxml=/src/reports/python-${version}.xml \
                -o junit_suite_name=python-${version}"
    done
}

cmd_set_version() {
    local version=${1:-}
    [ -n "$version" ] || die "usage: ci.sh set-version <version>"
    # Release versions only: X.Y.Z, optionally aN/bN/rcN. PEP 440 normalizes
    # other spellings, which would make the tag and the files disagree.
    [[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+((a|b|rc)[0-9]+)?$ ]] \
        || die "'$version' is not X.Y.Z or X.Y.Z{a,b,rc}N"
    sed -i.bak "s/^version *= *\".*\"$/version = \"${version}\"/" pyproject.toml
    rm -f pyproject.toml.bak
    [ "$(project_version)" = "$version" ] || die "pyproject.toml version is $(project_version) after setting $version"
    echo "pyproject.toml version: $version"
}

cmd_unpublished() {
    local index=${1:-}
    local url version
    url="$(index_url "$index" json)" || exit 1
    version=$(project_version)
    in_python "$BUILD_PYTHON" "
        python - <<'EOF'
import sys, urllib.error, urllib.request

try:
    urllib.request.urlopen('${url}/${PROJECT}/${version}/json', timeout=30)
except urllib.error.HTTPError as e:
    if e.code == 404:
        print('${PROJECT} ${version} is not on ${index} yet')
        sys.exit(0)
    raise
sys.exit('ERROR: ${PROJECT} ${version} is already on ${index}. An index never accepts '
         'the same file twice, so release a new version.')
EOF"
}

cmd_checksum() {
    in_python "$BUILD_PYTHON" '
        cd dist
        sha256sum -- *.whl *.tar.gz > SHA256SUMS
        sha256sum -c --quiet SHA256SUMS
        cat SHA256SUMS'
}

cmd_upload() {
    local index=${1:-}
    local url
    url="$(index_url "$index" upload)" || exit 1
    [ -n "${TWINE_PASSWORD:-}" ] || die "TWINE_PASSWORD (the ${index} API token) is not set"
    export TWINE_USERNAME=__token__
    in_python "$BUILD_PYTHON" "
        python -m venv /tmp/venv
        /tmp/venv/bin/pip install -q twine
        /tmp/venv/bin/twine upload --non-interactive --repository-url ${url} \
            dist/*.whl dist/*.tar.gz"
}

cmd_verify() {
    local index=${1:-}
    local url version
    url="$(index_url "$index" json)" || exit 1
    version=$(project_version)
    [ -f dist/SHA256SUMS ] || die "no dist/SHA256SUMS: run 'ci.sh checksum' first"
    # The index can take a little while to list a new upload.
    in_python "$BUILD_PYTHON" "
        python - <<'EOF'
import json, sys, time, urllib.error, urllib.request

url = '${url}/${PROJECT}/${version}/json'
local = {}
with open('dist/SHA256SUMS') as f:
    for line in f:
        digest, name = line.split()
        local[name.lstrip('*')] = digest

for attempt in range(30):
    try:
        with urllib.request.urlopen(url, timeout=30) as r:
            served = {u['filename']: u['digests']['sha256'] for u in json.load(r)['urls']}
        if set(served) >= set(local):
            break
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise
    time.sleep(10)
else:
    sys.exit('ERROR: %s does not list all of %s after 5 minutes' % (url, sorted(local)))

bad = [n for n in local if served[n] != local[n]]
extra = sorted(set(served) - set(local))
for n in sorted(local):
    print('%s  %s  %s' % (served[n], n, 'OK' if n not in bad else 'MISMATCH, built ' + local[n]))
if extra:
    print('ERROR: the index also serves files this build did not make: %s' % extra)
if bad or extra:
    sys.exit(1)
EOF"
}

command=${1:-}
[ $# -gt 0 ] && shift
case "$command" in
    build)       cmd_build "$@" ;;
    test)        cmd_test "$@" ;;
    set-version) cmd_set_version "$@" ;;
    unpublished) cmd_unpublished "$@" ;;
    checksum)    cmd_checksum "$@" ;;
    upload)      cmd_upload "$@" ;;
    verify)      cmd_verify "$@" ;;
    *)           sed -n '/^# Build, test and publish/,/^$/s/^# \{0,1\}//p' "$0" >&2; exit 2 ;;
esac
