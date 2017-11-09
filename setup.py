from setuptools import setup, find_packages

setup(
    name = 'thespian',
    version = '3.8.3',
    description = 'Python Actor concurrency library',
    author = 'Kevin Quick',
    author_email = 'quick@sparq.org',
    url = 'http://thespianpy.com',
    license = 'MIT',
    packages = find_packages(exclude=['thespian/test']),
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: System :: Distributed Computing',
    ],
    long_description = '''

Thespian is a Python library providing a framework for developing
concurrent, distributed, fault tolerant applications.

Thespian is built on the Actor Model which allows applications to be
written as a group of independently executing but cooperating
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

   * More Information: http://thespianpy.com
   * Release Notes: http://thespianpy.com/doc/releases.html

    ''',
    keywords = ['actors', 'concurrent', 'concurrency', 'asynchronous',
                'message passing', 'distributed', 'distributed systems',
                'fault tolerant']
)
