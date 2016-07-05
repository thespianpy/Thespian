Tests are found in this directory and also scattered throughout the
source paths.  All tests are designed to be run easily by nosetests.

The tests in this directory tend to be functional tests
and the scattered tests are typically unittests and are in the "test"
subdirectory of the source directory where the unit they are testing
exists.

Tests are designed to be run by pytest (https://pytest.org) and
utilize pytest fixtures and other features.  The names of the test
classes should begin with "TestUnit" for unit tests and "TestFunc" for
functional tests.  Most tests are parameterized with a test Actor
System that will iterate through the defined system bases.  The pytest
`-k' argument may be used to select a specific subset of tests.  For
example:


  $ py.test           # runs all tests
  $ py.test -k Func   # runs all functional tests
  $ py.test -k "Unit and multiprocTCPBase"    # runs unit tests for this base


Not all tests can be run for all system bases; the pytest skip
directive is used to skip tests not appropriate for a system base.
