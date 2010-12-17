import memcache
import random
import riverfish
import unittest

# TODO include an in-process memcached daemon to avoid dealing with issues relating to clobbering existing memcached instances

class RiverfishTests(unittest.TestCase) :
	def setUp(self) :
		name = list('abcdefghijklmnopqrstuvwxyz')
		random.shuffle(name)
		self.rivername = reduce(lambda a,b: a+b, name)
		self.client = memcache.Client(['127.0.0.1:11211'], immortal=True, pickleProtocol=True)

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
