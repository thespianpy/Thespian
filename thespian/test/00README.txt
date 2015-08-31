Tests are found in this directory and also scattered throughout the
source paths.  All tests are designed to be run easily by nosetests.

The tests in this directory tend to be functional tests
and the scattered tests are typically unittests and are in the "test"
subdirectory of the source directory where the unit they are testing
exists.

There are several attributes assigned to tests to make it easier to
run a subset of tests:

    * testbase -- specifies which system base is used for this test.
      Many test files contain a set of identical tests that are run
      inder different ActorSystem implementations (bases) to verify
      the behavior is the same for all implementations.  The testbase
      attribute indicates which implementation is being tested.

      The following testbase values are defined:

        Simple          -- the simple base system
        Multiprocess    -- older multi-process TCP implementation
        MultiprocUDP    -- multi-process with UDP transport
        MultiprocTCP    -- multi-process with TCP transport
        MultiprocQueue  -- multi-process with multiprocess.Queue transport

      Testing can be limited to specific bases using the -a or -A nosetest argument:

      $ nosetests -a testbase=MultiprocQueue
      $ nosetests -a testbase=MultiprocQueue  -a testbase=MultiprocUDP
      $ nosetests -A "testbase in ['MultiprocQueue', 'MultiprocUDP']"

    * scope -- specifies the scope of the test.  Value values are:

       unit -- simple unit test of a single component; no ActorSystem started
       func -- functional test of entire system, run for one or more specific ActorSystems

    * unstable -- specifies that a particular test is unstable.

      For example, the MultiproceQueueSystem appears to deadlock
      internally under high stress.  Please do not mark tests as
      unstable unless they really appear to be due to external issues
      and not Thespian implementation problems: normal build testing
      will skip unstable tests.

      Tests marked in this way may be skipped for normal test runs:

      $ nosetests -a '!unstable' ...

