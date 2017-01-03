from thespian.test import *
from thespian.director import GroupLoadableFiles

foo_ver = lambda s: [P() for P in GroupLoadableFiles.versionExtract('foo')(s)]


class Test_GroupVersion(object):

    def test_extract_alpha(self):
        assert ['-a'] == foo_ver('foo-a.tls')
        assert ['-b'] == foo_ver('foo-b.tls')
        assert ['-abc'] == foo_ver('foo-abc.tls')
        assert ['-a-a'] == foo_ver('foo-a-a.tls')
        assert ['-a-b'] == foo_ver('foo-a-b.tls')
        assert ['-alpha'] == foo_ver('foo-alpha.tls')
        assert ['bar'] == foo_ver('foobar.tls')

        assert ['-a.tlf'] == foo_ver('foo-a.tlf')
        assert ['-a-b.tlf'] == foo_ver('foo-a-b.tlf')
        assert ['-alpha.tlf'] == foo_ver('foo-alpha.tlf')
        assert ['bar.tlf'] == foo_ver('foobar.tlf')

    def test_extract_number(self):
        assert ['-', 0] == foo_ver('foo-0.tls')
        assert ['-', 1] == foo_ver('foo-1.tls')
        assert ['-', 2] == foo_ver('foo-2.tls')
        assert ['-', 0] == foo_ver('foo-00000.tls')
        assert ['-', 1] == foo_ver('foo-01.tls')
        assert ['-', 2] == foo_ver('foo-000000000000000002.tls')
        assert ['-', 123] == foo_ver('foo-123.tls')
        assert ['-', 123] == foo_ver('foo-0000123.tls')
        assert ['-', 123000] == foo_ver('foo-0000123000.tls')
        assert ['-', 123, '.', 456] == foo_ver('foo-123.456.tls')
        assert [123000] == foo_ver('foo0000123000.tls')
        assert [123, '.', 456] == foo_ver('foo123.456.tls')

        assert ['-', 0, '.tlf'] == foo_ver('foo-00.tlf')
        assert ['-', 123, '.', 456, '.tlf'] == foo_ver('foo-123.456.tlf')
        assert [123000, '.tlf'] == foo_ver('foo0000123000.tlf')
        assert [123, '.', 456, '.tlf'] == foo_ver('foo123.456.tlf')

    def test_extract_mixed(self):
        assert ['-', 3, '.', 53, 'a', 94, '-test'] == \
            foo_ver('foo-3.53a94-test.tls')
        assert ['-', 3, '.', 53, 'a', 94, '-test'] == \
            foo_ver('foo-003.053a000094-test.tls')

        assert ['-', 3, '.', 53, 'a', 94, '-test.tlf'] == \
            foo_ver('foo-3.53a94-test.tlf')
        assert ['-', 3, '.', 53, 'a', 94, '-test.tlf'] == \
            foo_ver('foo-003.053a000094-test.tlf')
        assert ['-', 3, '.', 53, 'a', 94, '-test', 9, '.tlf'] == \
            foo_ver('foo-003.053a000094-test9.tlf')

class Test_GroupVersion_Ordering(object):

    def test_alpha(self):
        files = [
            'foo-b.foo',
            'foo-c.f',
            'foo-cab.call',
            'foo-a.foo',
            'foo-a-b-c.txt',
            'foo-foo.txt',
            'foo-foo-a.txt',
            'foo.txt',
            'foo-abd.txt',
            'foo-a-a.txt',
        ]

        print(' pre-sort',files)
        files.sort(key=GroupLoadableFiles.versionExtract('foo'))
        print('post-sort',files)

        assert files == [
            'foo-a-a.txt',
            'foo-a-b-c.txt',
            'foo-a.foo',
            'foo-abd.txt',
            'foo-b.foo',
            'foo-c.f',
            'foo-cab.call',
            'foo-foo-a.txt',
            'foo-foo.txt',
            'foo.txt',
        ]

    def test_numeric(self):
        files = [
            'foo-2.tls',
            'foo09.tls',
            'foo-04.tls',
            'foo-3.tls',
            'foo-321.tls',
            'foo-1.tls',
            'foo-1-2-3.tls',
            'foo-12341056.tls',
            'foo0.tls',
            'foo.tls',
            'foo-123.tls',
            'foo-1-1.tls',
            'foo-12340056.tls',
            'foo-0.tls',
        ]

        print(' pre-sort',files)
        files.sort(key=GroupLoadableFiles.versionExtract('foo'))
        print('post-sort',files)

        assert files == [
            'foo.tls',
            'foo0.tls',
            'foo09.tls',
            'foo-0.tls',
            'foo-1.tls',
            'foo-1-1.tls',
            'foo-1-2-3.tls',
            'foo-2.tls',
            'foo-3.tls',
            'foo-04.tls',
            'foo-123.tls',
            'foo-321.tls',
            'foo-12340056.tls',
            'foo-12341056.tls',
        ]

    def test_mixed(self):
        files = [
            'foo-201601280945.tls',
            'foo-201601251202.tls',
            'foo-15.2.1.tls',
            'foo-15a5gamma1.tls',
            'foo-15a5beta4.tls',
            'foo-15a3.tls',
            'foo-15.1.tls',
            'foo-15a1.tls',
            'foo-15a.tls',
            'foo-201601251343.tls',
            'foo-0.tls',
            'foo-15.2.tls',
            'foo-15.tls',
            'foo-05.tls',
            'foo-1.tls',
            'foo.tls',
        ]

        print(' pre-sort',files)
        files.sort(key=GroupLoadableFiles.versionExtract('foo'),
                   reverse=True)
        print('post-sort',files)

        assert files == [
            'foo-201601280945.tls',
            'foo-201601251343.tls',
            'foo-201601251202.tls',
            'foo-15a5gamma1.tls',
            'foo-15a5beta4.tls',
            'foo-15a3.tls',
            'foo-15a1.tls',
            'foo-15a.tls',
            'foo-15.2.1.tls',
            'foo-15.2.tls',
            'foo-15.1.tls',
            'foo-15.tls',
            'foo-05.tls',
            'foo-1.tls',
            'foo-0.tls',
            'foo.tls',
        ]

    def test_dates(self):

        files = [
            'foo-20161223002802.tls',
            'foo-20161223011756.tls',
            'foo-20161223003832.tls',
            'foo-20161223011135.tls',
            'foo-20161223050526.tls',
            'foo-20161223011253.tls',
            'foo-20161223004110.tls',
            'foo-20161223011355.tls',
            'foo-20161223011658.tls',
            'foo-20161223011730.tls',
        ]

        print(' pre-sort',files)
        files.sort(key=GroupLoadableFiles.versionExtract('foo'),
                   reverse=True)
        print('post-sort',files)

        assert files == [
            'foo-20161223050526.tls',
            'foo-20161223011756.tls',
            'foo-20161223011730.tls',
            'foo-20161223011658.tls',
            'foo-20161223011355.tls',
            'foo-20161223011253.tls',
            'foo-20161223011135.tls',
            'foo-20161223004110.tls',
            'foo-20161223003832.tls',
            'foo-20161223002802.tls',
        ]

    def test_dates_full_path(self):

        files = [
            'work/foo/loads/dog-20161223002802.tls',
            'work/foo/loads/dog-20161223011756.tls',
            'work/foo/loads/dog-20161223003832.tls',
            'work/foo/loads/dog-20161223011135.tls',
            'work/foo/loads/dog-20161223050526.tls',
            'work/foo/loads/dog-20161223011253.tls',
            'work/foo/loads/dog-20161223004110.tls',
            'work/foo/loads/dog-20161223011355.tls',
            'work/foo/loads/dog-20161223011658.tls',
            'work/foo/loads/dog-20161223011730.tls',
        ]

        print(' pre-sort',files)
        files.sort(key=GroupLoadableFiles.versionExtract('dog'),
                   reverse=True)
        print('post-sort',files)

        assert files == [
            'work/foo/loads/dog-20161223050526.tls',
            'work/foo/loads/dog-20161223011756.tls',
            'work/foo/loads/dog-20161223011730.tls',
            'work/foo/loads/dog-20161223011658.tls',
            'work/foo/loads/dog-20161223011355.tls',
            'work/foo/loads/dog-20161223011253.tls',
            'work/foo/loads/dog-20161223011135.tls',
            'work/foo/loads/dog-20161223004110.tls',
            'work/foo/loads/dog-20161223003832.tls',
            'work/foo/loads/dog-20161223002802.tls',
        ]
