# Contributing

Thank you for your interest in contributing. VoltDB uses GitHub to manage reviews of pull requests. We welcome your contributions to VoltDB or to any of the related libraries and tools.

VoltDB uses the standard github workflow, meaning we make use of forking and pull requests.

* If you have a trivial fix or improvement, go ahead and create a pull request.

* If you are interested in contributing something more involved, feel free to discuss your ideas in [VoltDB Public](http://chat.voltdb.com/) on Slack.

## Development

The client is two pure-Python modules, `src/voltdbclient.py` and `src/voltclient.py`, with no third-party dependencies. Install the checkout in editable mode and run the offline tests, which need no server:

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -e .
python -m unittest discover -s tests -v
```

The end-to-end tests against a live cluster, and the benchmark, run in the VoltDB server's own build. They use the client release that the VoltDB distribution ships, so a new release is tested against the server when the server build moves to it.

To build the distributions that get published to PyPI:

```bash
pip install build twine
python -m build
twine check dist/*
```

CI runs the same steps through `jenkins/ci.sh`, in a `python:<version>-slim` container per step, so you can reproduce a CI run with only Docker installed:

```bash
jenkins/ci.sh build
jenkins/ci.sh test 3.9 3.14
```

## Continuous integration and releases

Two Jenkins pipelines live under `jenkins/`:

* `jenkins/test/Jenkinsfile`, the job `VoltDB/Python Client/Tests`, builds every branch of this repository and runs the offline suites against the built wheel on each supported Python version. Pull requests from forks are not built; a maintainer pushes the branch here to test one. The job uses no credentials.
* `jenkins/release/Jenkinsfile`, the job `VoltDB/Python Client/Release`, releases a version, starting from `RELEASE_VERSION` (for example `16.1.0rc1` or `16.1.0`). It stamps that version into `pyproject.toml` on a release commit, then builds and tests it. Only after that does it tag the commit `v<version>` and publish to TestPyPI. With `PUBLISH_TO=pypi` it then waits for a person to approve before publishing to PyPI. After each upload it checks that the index serves exactly the files it built. `DRY_RUN`, on by default, stops after the tests.

`MODE=promote` publishes a version that is already tagged and on TestPyPI to PyPI without rebuilding it. It downloads the wheel and the sdist from TestPyPI, checks them against TestPyPI's hashes, and runs the tests on that wheel. After a person approves, it uploads the same files to PyPI.

Neither index ever accepts a file twice, so a release that fails after uploading to TestPyPI is finished with promote, or continues with the next version. `master` keeps a `.devN` version between releases; moving it on is an ordinary pull request.

The VoltDB distribution bundles a pinned release of this package. Its build checks the wheel against the sha256 the release job prints in its build description, so use that value when updating the pin, not one read back from PyPI.

## Contributor License Agreement

In order to contribute code to VoltDB, you must first sign the [VoltDB Contributor License Agreement (CLA)](https://www.voltdb.com/contributor-license-agreement/) and email an image or PDF of the document including your hand-written signature to [support@voltdb.com](mailto:support@voltdb.com). VoltDB will sign and return the final copy of the agreement for your records.

## How to submit code

The workflow is essentially the following:

1. Fork the VoltDB project
2. Make a branch. Commit your changes to this branch. (See note below)
3. Issue a pull request on the VoltDB repository.

Once you have signed the CLA, a VoltDB engineer will review your pull request.

Note:

It will be easier to keep your work merge-able (conflict-free) if you don't work directly on your master branch in your VoltDB fork. Rather, keep your master branch in sync with the VoltDB repository and apply your changes on a branch of your fork.

For further reading:

* [How to fork a GitHub repository](https://help.github.com/articles/fork-a-repo)
* [Using pull requests](https://help.github.com/articles/using-pull-requests/)

## Additional Resources

* [VoltDB Wiki](https://github.com/VoltDB/voltdb/wiki) on Github
* [VoltDB Public](http://chat.voltdb.com/) on Slack
* [VoltDB on Stack Overflow](https://stackoverflow.com/questions/tagged/voltdb)
