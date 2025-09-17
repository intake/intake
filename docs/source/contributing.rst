Contributor guide
=================

``intake`` is an open-source project (see the LICENSE). We welcome contributions from
the public, for code, including fixes and new features, documentation and anything else
that will help make this repo better. Even posting issues can be very useful, as they
will help others to make the necessary changes to alleviate the issue.

Development process
-------------------

Development of ``intake`` happens on `github`_. There you will find options for
creating issues and commenting on existing issues and pull-requests (PRs). You may
wish to "watch" the repo, to be notified of changes as they occur. You must have an
account on github to be able to interact here, but this is free. By default, you
will be notified of changes (e.g., new comments) on any issue or PR you have interacted
with.

In order to propose changes to the repo itself, you will need to create a PR. This is
done by following these steps:

1. clone the repo. There are many ways to do this, but most common is the following command,
   which will create a local directory ``intake/`` containing the code, metadata, docs and
   version control information.

.. code-block:: shell

   $ git clone https://github.com/intake/intake


2. create a fork or the repo using the github web interface. Your fork will probably live in
   your private github namespace. Set this as a remote inside your local copy of the repo

.. code-block:: shell

   $ git remote add fork https://github.com/<username>/intake

3. make changes locally in a new branch. First you create the branch, and then add commits
   to that branch. Here are suggested ways to do this. Note that git is _very_ flexible and
   there are many ways to achieve each step.

.. code-block:: shell

   $ git checkout -b <new branch name>
   $ git commit -a

4. When your branch is an a suitable state, `push` your work to your branch. github will prompt
   you with a URL to create the PR, or navigate to your fork and branch in the web interface to
   create the PR there.

.. code-block:: shell

   $ git push fork

5. After review from a maintainer, you may wish to push more commits to your branch as required,
   and your PR may be accepted ("merged") or rejected ("closed").

.. _github: https://github.com/intake/intake

Guidelines
----------

To make contributing as smooth as possible, we recommend the following.

1. Always follow the project's Code of Conduct when interacting with other humans.

2. Please describe as clearly as possible what your intent is. In the case of issues, this
   might include pasting the whole traceback your have seen following an error, listing the
   versions of ``intake`` and its dependencies that you have installed, describing the
   circumstances when you saw a problem or would like better behaviour. Ideally, you would
   include code that allows maintainers to fully reproduce your steps.

3. When submitting changes, make sure that you describe what the changes achieve and how.
   Ideally, all code should be covered by tests included in the same PR, and that run to
   completion as part of CI (see below).

4. New functions and classes should include reasonable
   `style`, e.g., appropriate labels and hierarchy, indentation and other code formatting
   matching the rest of the docs, and docstrings and comments as appropriate. A "precommit"
   set of linters is available to run against your code, and runs as part of CI to enforce
   a minimal set of style rules. To run these locally on every commit, you can run this in the
   repo root:

.. code-block:: shell

   $ pre-commit install

5. Additions to the prose documentation (under docs/source/) should be included for new
   or altered features. After the initial full release, we will be maintaining a changelog.

Testing
-------

This repo uses ``pytest`` for testing. You can install test dependencies, for example with
this command run in the repo root. There are many optional dependencies for specific tests,
and we recommend that you use ``pytest.importorskip`` to tests that need these or additional
packages, so that they will not fail for developers without those dependencies. **Do**, however,
edit one or more files in scripts/ci/, to ensure that added tests will execute in at least one
of the CI runs.

The easiest way to boostrap a development environment in order to run tests as they will in
CI is to use conda-env, e.g.:

.. code-block:: shell

   $ conda env create -y -f scripts/ci/environment-py313.yml
   $ conda activate test_env

To run the tests:

.. code-block:: shell

   $ pytest -v

Note that ensuring coverage is optional, but recommended.

Adding docs
-----------

Docstrings, prose text and examples/tutorials are eagerly accepted! We, as coders, often
are late to fully document our work, and all contributions are welcome. Separate instructions
can be found in the docs/README.md file.

In addition, full notebook examples may be added in the examples/ directory, but you
should be sure to add instructions on the appropriate environment or other preparation
required to run them.
