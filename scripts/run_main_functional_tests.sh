nosetests --verbosity=2 -A 'unstable != 1 and scope=="func" and testbase in ["Simple", "MultiprocQueue", "MultiprocTCP", "MultiprocUDP"]'
