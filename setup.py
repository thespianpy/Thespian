from setuptools import setup, find_packages

setup(
    name = 'thespian',
    version = '2.1.4',
    description = 'Python Actor concurrency library',
    author = 'Kevin Quick',
    author_email = 'kquick@godaddy.com',
    license = 'MIT',
    scripts = [ 'thespianShell.py' ],
    packages = find_packages(exclude=['thespian/test']),
    classifiers = [
        'Environment :: Library',
        'Intended Audience :: Developers',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ]
)
