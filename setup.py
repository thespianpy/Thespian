from setuptools import setup, find_packages

setup(
    name = 'thespian',
    version = '2.1.6',
    description = 'Python Actor concurrency library',
    author = 'Kevin Quick, GoDaddy',
    author_email = 'kquick@godaddy.com',
    url = 'http://thespianpy.com',
    license = 'MIT',
    scripts = [ 'thespianShell.py' ],
    packages = find_packages(exclude=['thespian/test']),
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: System :: Distributed Computing',
    ],
    long_description = '''

    Thespian is a Python library providing a framework for developing
    concurrent, distributed, fault tolerant applications.

    Thespian is built on the Actor Model which allows applications to
    be written as a group of independently executing but cooperating
    "Actors" which communicate via messages.  These Actors run within
    the Actor System provided by the Thespian library.

      * Concurrent
      * Distributed
      * Fault Tolerant
      * Scalable
      * Location independent

    Actor programming is broadly applicable and it is ideally suited
    for Cloud-based applications as well, where compute nodes are
    added and removed from the environment dynamically.

  '''
)
