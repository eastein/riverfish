import memcache_exceptional
import random
import riverfish
import unittest

# TODO
# include an in-process memcached daemon to avoid dealing with issues relating to clobbering existing memcached instances
## no evictions in this daemon
# tests that force memcached operations to sleep for random periods or freeze (perhaps allowing stepping from the test suite?)
## show that during various types of insert and delete, the iteration continues to work at different stages, and not temporary removals or reorderings occur
## show that at no point would a failure in an insert result in corruption that failed iteration or get on previously OK data
## show that concurrent inserts will not corrupt the db, even in the case of conflicts

class RiverfishTests(unittest.TestCase) :
	def _alphaShuffle(self) :
		name = list('abcdefghijklmnopqrstuvwxyz')
		random.shuffle(name)		
		return reduce(lambda a,b: a+b, name)

	def setUp(self) :
		self.rivername = self._alphaShuffle()
		if not hasattr(self, 'client') :
			self.client = memcache_exceptional.Client(['127.0.0.1:11211'], immortal=True, pickleProtocol=True)
		else :
			self.client.flush_cas()

	def test_create(self) :
		river = riverfish.River(self.client, self.rivername, create=True)

	def test_create_twice_fails(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		try :
			recreate = riverfish.River(self.client, self.rivername, create=True)
			self.fail("should have failed on the second create operation")
		except riverfish.RiverAlreadyExistsException :
			pass

	def test_discover(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		findriver = riverfish.River(self.client, self.rivername)

	def test_add(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		river.add(350000, {'KEY' : 350000, 'HI' : 'THERE'})
		try :
			river.add(4000, {'KEY' : 4000, '_KEY' : 4000, 'HELLO' : 'TEST'})
			self.fail("should have failed with _KEY key in data")
		except riverfish.DisallowedMetadataKeyException :
			pass

	def test_get(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		k = 350000
		d = {'KEY' : k, 'HI' : 'THERE'}
		river.add(k, d)
		self.assertEquals([d], river.get(k))

	def test_get_unique(self) :
		river = riverfish.River(self.client, self.rivername, create=True, unique=True)
		k = 350000
		d = {'KEY' : k, 'HI' : 'THERE'}
		river.add(k, d)
		self.assertEquals(d, river.get(k))

	def test_get_unique_nothing_none(self) :
		river = riverfish.River(self.client, self.rivername, create=True, unique=True)
		k = 350000
		d = {'KEY' : k, 'HI' : 'THERE'}
		river.add(k, d)
		self.assertEquals(None, river.get(k + 1))

	def test_get_two(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		k = 350000
		d = {'KEY' : k, 'HI' : 'THERE'}
		d2 = {'KEY' : k, 'HI' : 'WHERE'}
		river.add(k, d)
		river.add(k, d2)
		self.assertEquals([d, d2], river.get(k))

	def _assertIterEquals(self, riv, exp) :
		ind = 0
		for i in riv :
			self.assertEquals(exp[ind], i)
			ind += 1
		self.assertEquals(len(exp), ind)

	def test_iteration_empty(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		self._assertIterEquals(river, [])

	def test_iteration_one(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		river.add(450, {'KEY' : 450, 'hi' : 'there'})
		self._assertIterEquals(river, [(450, {'KEY' : 450, 'hi' : 'there'})])

	def test_iteration_two(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		river.add(3, {'KEY' : 3, 'test1' : 'test1'})
		river.add(riverfish.DefaultLevels.DEFAULT[0] + 3, {'KEY' : riverfish.DefaultLevels.DEFAULT[0] + 3, 'test2' : 'test2'})
		self._assertIterEquals(river, [(3, {'KEY' : 3, 'test1' : 'test1'}), (riverfish.DefaultLevels.DEFAULT[0]+3, {'KEY' : riverfish.DefaultLevels.DEFAULT[0] + 3, 'test2' : 'test2'})])

	def test_iteration_two_equal(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		river.add(3, {'KEY' : 3, 'test1' : 'test1'})
		river.add(3, {'KEY' : 3, 'test2' : 'test2'})
		self._assertIterEquals(river, [(3, {'KEY' : 3, 'test1' : 'test1'}), (3, {'KEY' : 3, 'test2' : 'test2'})])

	def test_iteration_double_reverse_fails(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		try :
			for x in river.reverse.reverse :
				pass
			self.fail("should not allow double reversal")
		except riverfish.IterationOptionsException :
			pass

	def test_iteration_double_lower_bound_fails(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		try :
			for x in river.lowerbound(0).lowerbound(0) :
				pass
			self.fail("should not allow double lowerbound")
		except riverfish.IterationOptionsException :
			pass

	def test_iteration_lower_bound_key_transform(self) :
		river = riverfish.River(self.client, self.rivername, create=True, key_transform='kt_cast')
		river.add("3", {"KEY" : "3", 'A' : 'A'})
		river.add("4", {"KEY" : "4", 'A' : 'B'})
		river.add("5", {"KEY" : "5", 'A' : 'C'})
		self._assertIterEquals(river.lowerbound("4"), [("4", {"KEY" : "4", 'A' : 'B'}), ("5", {"KEY" : "5", 'A' : 'C'})])

	def test_iteration_double_upper_bound_fails(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		try :
			for x in river.upperbound(0).upperbound(0) :
				pass
			self.fail("should not allow double upperbound")
		except riverfish.IterationOptionsException :
			pass

	def test_internal_minn(self) :
		self.assertEquals(riverfish.minn(None, None), None)
		self.assertEquals(riverfish.minn(None, 3), 3)
		self.assertEquals(riverfish.minn(None, 0), 0)
		self.assertEquals(riverfish.minn(3, None), 3)
		self.assertEquals(riverfish.minn(0, None), 0)
		self.assertEquals(riverfish.minn(None, -3), -3)
		self.assertEquals(riverfish.minn(-3, None), -3)
		self.assertEquals(riverfish.minn(-3, -5), -5)
		self.assertEquals(riverfish.minn(3, 5), 3)

	def test_iteration_lower_bound(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		kbig = 3 + 2 * riverfish.DefaultLevels.DEFAULT[-1:][0]
		river.add(1, {'KEY' : 1, 'test' : 'test1'})
		river.add(2, {'KEY' : 2, 'test' : 'test2'})
		river.add(kbig, {'KEY' : kbig, 'test' : 'test3'})
		self._assertIterEquals(river.lowerbound(2), [(2, {'KEY' : 2, 'test' : 'test2'}), (kbig, {'KEY' : kbig, 'test' : 'test3'})])

	def test_iteration_lower_bound_reversed(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		kbig = 3 + 2 * riverfish.DefaultLevels.DEFAULT[-1:][0]
		river.add(1, {'KEY' : 1, 'test' : 'test1'})
		river.add(2, {'KEY' : 2, 'test' : 'test2'})
		river.add(kbig, {'KEY' : kbig, 'test' : 'test3'})
		self._assertIterEquals(river.lowerbound(2).reverse, [(kbig, {'KEY' : kbig, 'test' : 'test3'}), (2, {'KEY' : 2, 'test' : 'test2'})])
		self._assertIterEquals(river.reverse.lowerbound(2), [(kbig, {'KEY' : kbig, 'test' : 'test3'}), (2, {'KEY' : 2, 'test' : 'test2'})])

	def test_iteration_upper_bound(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		kbig = 3 + 2 * riverfish.DefaultLevels.DEFAULT[-1:][0]
		river.add(1, {'KEY' : 1, 'test' : 'test1'})
		river.add(2, {'KEY' : 2, 'test' : 'test2'})
		river.add(kbig, {'KEY' : kbig, 'test' : 'test3'})
		self._assertIterEquals(river.upperbound(2), [(1, {'KEY' : 1, 'test' : 'test1'}), (2, {'KEY' : 2, 'test' : 'test2'})])

	def test_iteration_upper_bound_reversed(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		kbig = 3 + 2 * riverfish.DefaultLevels.DEFAULT[-1:][0]
		river.add(1, {'KEY' : 1, 'test' : 'test1'})
		river.add(2, {'KEY' : 2, 'test' : 'test2'})
		river.add(kbig, {'KEY' : kbig, 'test' : 'test3'})
		self._assertIterEquals(river.upperbound(2).reverse, [(2, {'KEY' : 2, 'test' : 'test2'}), (1, {'KEY' : 1, 'test' : 'test1'})])
		self._assertIterEquals(river.reverse.upperbound(2), [(2, {'KEY' : 2, 'test' : 'test2'}), (1, {'KEY' : 1, 'test' : 'test1'})])

	def test_iteration_upper_bound_key_transform(self) :
		river = riverfish.River(self.client, self.rivername, create=True, key_transform='kt_cast')
		river.add("3", {"KEY" : "3", 'A' : 'A'})
		river.add("4", {"KEY" : "4", 'A' : 'B'})
		river.add("5", {"KEY" : "5", 'A' : 'C'})
		self._assertIterEquals(river.upperbound("4"), [("3", {"KEY" : "3", 'A' : 'A'}), ("4", {"KEY" : "4", 'A' : 'B'})])

	def test_iteration_reverse_equal(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		river.add(3, {'KEY' : 3, 'test1' : 'test1'})
		river.add(3, {'KEY' : 3, 'test2' : 'test2'})
		self._assertIterEquals(river.reverse, [(3, {'KEY' : 3, 'test2' : 'test2'}), (3, {'KEY' : 3, 'test1' : 'test1'})])

	def test_iteration_reverse_notequal(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		kbig = 3 + 2 * riverfish.DefaultLevels.DEFAULT[-1:][0]
		river.add(kbig, {'KEY' : kbig, 'test1' : 'test1'})
		river.add(3, {'KEY' : 3, 'test2' : 'test2'})
		self._assertIterEquals(river.reverse, [(kbig, {'KEY' : kbig, 'test1' : 'test1'}), (3, {'KEY' : 3, 'test2' : 'test2'})])

	def test_1000_random_sequenced_inserts_ordered_iteration_no_keys_equal(self) :
		river = riverfish.River(self.client, self.rivername, create=True)

		n_items = 1000
		n_range = riverfish.DefaultLevels.DEFAULT[0]*10
		total_data = {}
		for i in xrange(n_items) :
			rk = random.randint(0, n_range)
			while rk in total_data :
				rk = random.randint(0, n_range)
			rd = self._alphaShuffle()
			dd = {'KEY' : rk, 'DATA' : rd}
			total_data[rk] = dd
			river.add(rk, dd)

		exp = []
		tk = total_data.keys()
		tk.sort()
		for k in tk :
			exp.append((k, total_data[k]))

		self._assertIterEquals(river, exp)

	def test_kt_crc32_iterate(self) :
		river = riverfish.StringKeyedRiver(self.client, self.rivername, create=True)
		h1 = {'KEY' : 'hi1', 'DATA' : 'test1'}
		h2 = {'KEY' : 'hi2', 'DATA' : 'test2'}
		h3 = {'KEY' : 'hi3', 'DATA' : 'test3'}
		river.add('hi1', h1)
		river.add('hi2', h2)
		river.add('hi3', h3)
		self._assertIterEquals(river, [('hi1', h1), ('hi3', h3), ('hi2', h2)])

	def test_kt_crc32_get1(self) :
		river = riverfish.StringKeyedRiver(self.client, self.rivername, create=True)
		t1 = {'KEY' : 'hi1', 'DATA' : 'test1'}
		river.add('hi1', t1)
		self.assertEquals([t1], river.get('hi1'))

	def test_kt_crc32_get2(self) :
		river = riverfish.StringKeyedRiver(self.client, self.rivername, create=True)
		t1 = {'KEY' : 'hi1', 'DATA' : 'test1'}
		t2 = {'KEY' : 'hi1', 'DATA' : 'test2'}
		river.add('hi1', t1)
		river.add('hi1', t2)
		self.assertEquals([t1, t2], river.get('hi1'))

	def test_kt_collision_get(self) :
		river = riverfish.River(self.client, self.rivername, create=True, ind=riverfish.DefaultLevels.CRC_OPTIMIZED, key_transform='kt_allzero')
		river.add('a', {'KEY' : 'a', 'DATA' : 'should get this.'})
		river.add('b', {'KEY' : 'b', 'DATA' : 'should not get this.'})
		self.assertEquals(river.get('a'), [{'KEY' : 'a', 'DATA' : 'should get this.'}])

	def test_addunique_numeric_fails(self) :
		river = riverfish.River(self.client, self.rivername, create=True, unique=True)
		river.add(1, {'KEY' : 1, 'DATA' : 'test'})
		try :
			river.add(1, {'KEY' : 1, 'DATA' : 'test'})
			self.fail("should not have succeeded adding another key.")
		except riverfish.RiverKeyAlreadyExistsException :
			pass

	def test_addunique_string_fails(self) :
		river = riverfish.StringKeyedRiver(self.client, self.rivername, create=True, unique=True)
		river.add('a', {'KEY' : 'a', 'DATA' : 'test'})
		try :
			river.add('a', {'KEY' : 'a', 'DATA' : 'test'})
			self.fail("should not have succeeded adding another key.")
		except riverfish.RiverKeyAlreadyExistsException :
			pass

	def test_addunique_string_hashcollision_ok(self) :
		river = riverfish.River(self.client, self.rivername, create=True, ind=riverfish.DefaultLevels.CRC_OPTIMIZED, key_transform='kt_allzero', unique=True)
		river.add('a', {'KEY' : 'a', 'DATA' : 'test'})
		river.add('b', {'KEY' : 'b', 'DATA' : 'test2'})
		self.assertEquals(river.get('a'), {'KEY' : 'a', 'DATA' : 'test'})
