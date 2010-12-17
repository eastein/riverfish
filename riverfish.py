"""
* what happens if all of the memcached nodes get restarted between gets() and cas()?  Does reconnect flush anything? Are cas tokens still valid?
* what happens if memcached temporarily fails at any point?
* what happens if memcached permanently fails at some point?
** secondary store/queue for cleanup operations?
* sharing client? can this cause issues?
* keeping one client over the length of operations with a river object? this could be bad too..
* if I am going to allow reindexing, river objects can't cache IND anymore.
"""

import uuid
import msgpack

class RiverfishException(Exception) :
	"""Base exception class for riverfish"""

class SafelyFailedException(RiverfishException) :
	"""Exceptions that resulted from a riverfish operation that did not have any affect."""

class RiverAlreadyExistsException(SafelyFailedException) :
	"""River already existed.  Could not create."""

class RiverDoesNotExistException(SafelyFailedException) :
	"""River does not exist."""

class TransactionFailedException(SafelyFailedException) :
	"""The non-idempotent operation safely failed."""

class RiverDeletedException(SafelyFailedException) :
	"""The River in use was deleted. The current operation failed."""

DEFAULT_INDEX_LEVELS = [10000000, 1000000, 100000]

class River(object) :
	# TODO validate name as fitting a regex
	def __init__(self, client, name, create=False) :
		self.client = client
		self.name = name
		self.rnkey = 't:%s:rn' % self.name
		
		if create :
			self.ind = DEFAULT_INDEX_LEVELS[:]

			data = {
				'IND' : DEFAULT_INDEX_LEVELS,
				'FIN' : None,
				'LIN' : None,
				'TOT' : 0
			}
			if not self.client.add(self.rnkey, msgpack.packs(data)) :
				raise RiverAlreadyExistsException("river %s already exists" % self.name)
		else :
			data = self._getRiverNode()
			if not data :
				raise RiverDoesNotExistException("river %s does not exist" % self.name)
			self.ind = data['IND']


	def _unpack(self, v) :
		"""
		generic unpacker; understands None=None (no unpacking lookup failure)
		"""
		if v is None :
			return None
		else :
			return msgpack.unpacks(v)

	def _gupack(self, k) :
		"""
		get based unpack/lookup
		"""
		return self._unpack(self.client.get(k))

	def _gsupack(self, k) :
		"""
		gets based unpack/lookup
		"""
		return self._unpack(self.client.gets(k))


	def _getRiverNode(self) :
		return self._gupack(self.rnkey)

	def _getsRiverNode(self) :
		return self._gsupack(self.rnkey)

	def _getsIndexNode(self, key, indl) :
		return self._gsupack('t:%s:in:%d:%d' % (self.name, indl, key / indl))

	@property
	def count(self) :
		return self._getRiverNode()['TOT']

	"""
	Add a fish to the river, given the fish's metadata.
	"""
	def add(self, key, metadata, _levels=0) :
		river_node = self._getsRiverNode()
		if river_node['FIN'] is None :
			index_nodes = {}
			for indl in self.ind :
				index_nodes[indl] = self._getsIndexNode(self, key ,indl)
		else :
			raise RuntimeError("you can only add one thing so far.")

	def __iter__(self) :
		return Boat(self)

class Boat(object) :
	def __init__(self, river) :
		self.river = river

	def next() :
		# TODO make this work.
		raise StopIteration()
