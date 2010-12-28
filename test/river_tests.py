import memcache_exceptional
import random
import riverfish
import unittest

# TODO include an in-process memcached daemon to avoid dealing with issues relating to clobbering existing memcached instances

class RiverfishTests(unittest.TestCase) :
	def setUp(self) :
		name = list('abcdefghijklmnopqrstuvwxyz')
		random.shuffle(name)
		self.rivername = reduce(lambda a,b: a+b, name)
		self.client = memcache_exceptional.Client(['127.0.0.1:11211'], immortal=True, pickleProtocol=True)

	def test_create(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		self.assertEquals(river.count, 0, "should initialize with size 0")

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
		self.assertEquals(findriver.count, 0, "should rediscover with size 0")

	def test_add(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		river.add(350000, {'KEY' : 350000, 'HI' : 'THERE'})

	def test_get(self) :
		river = riverfish.River(self.client, self.rivername, create=True)
		k = 350000
		d = {'KEY' : k, 'HI' : 'THERE'}
		river.add(k, d)
		self.assertEquals([d], river.get(k))
