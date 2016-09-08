from pytest import raises
from thespian.system.utilis import fmap


class plain_obj(object):
    def __init__(self, sublist):
        self._sublist = sublist
    def vals(self): return self._sublist


class fmap_obj(plain_obj):
    def fmap(self, f):
        return fmap_obj(fmap(f, self._sublist))


class TestFMap(object):

    def test_fmap_list(self):
        assert [0,2,4,6,8] == fmap(lambda x: x*2, [0,1,2,3,4])
    def test_fmap_tuple(self):
        assert (0,2,4,6,8) == fmap(lambda x: x*2, (0,1,2,3,4))
    def test_fmap_range(self):
        assert [0,2,4,6,8] == fmap(lambda x: x*2, range(5))
    def test_fmap_filter(self):
        assert [0,2,4,6,8] == fmap(lambda x: x*2, filter(lambda y: y < 5, range(15)))
    def test_fmap_map(self):
        assert [0,2,4,6,8] == fmap(lambda x: x*2, map(lambda y: y - 1, range(1,6)))
    def test_fmap_dict(self):
        assert {0:2, 4:6, 8:10} == fmap(lambda x: x*2, {0:1, 2:3, 4:5})
    def test_fmap_zip(self):
        assert [(0,6), (2,8), (4,10)] == fmap(lambda x: x*2, zip([0,1,2], [3,4,5]))
    def test_fmap_mappable_obj(self):
        assert [0,2,4,6,8] == fmap(lambda x: x*2, fmap_obj([0,1,2,3,4])).vals()
    def test_fmap_plain_obj(self):
        assert 5 == fmap(lambda x: len(x.vals()), plain_obj([0,1,2,3,4]))
    def test_fmap_str_not_supported(self):
        raises(TypeError, fmap, lambda x: chr(ord(x) + 1), "abcde")
    def test_fmap_scalars(self):
        assert 2 == fmap(lambda x: x*2, 1)
        assert 5.5 == fmap(lambda x: x*2, 2.75)
        assert 5 == fmap(lambda x: len(x), "hello")
    def test_fmap_recursive(self):
        result = fmap(lambda x: x*2, ([10,11], (23,24), ([31,32], [42,(55,66)]),
                                           fmap_obj(zip(filter(lambda y: y < 10, range(3)), map(lambda y: y + 3, range(3))))))
        # Arg: realize all of the iterables, since python3 returns things like map objects and filter objects
        # but comparing those to an actual list fails.  That's what all the list(result...) stuff is below.
        print(len(result))
        assert [[20, 22], [46, 48], [[62,64], [84,(110,132)]], [(0,6), (2,8), (4,10)]] == \
            [list(result[0]),
             list(result[1]),
             [list(result[2][0]), list(result[2][1])],
             list(result[3].vals())]

