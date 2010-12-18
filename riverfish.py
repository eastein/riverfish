"""
* what happens if all of the memcached nodes get restarted between gets() and cas()?  Does reconnect flush anything? Are cas tokens still valid?
* what happens if memcached temporarily fails at any point?
* what happens if memcached permanently fails at some point?
** secondary store/queue for cleanup operations?
* sharing client? can this cause issues?
* keeping one client over the length of operations with a river object? this could be bad too..
* if I am going to allow reindexing, river objects can't cache IND anymore.
* transaction failure during index node creation can produce index node clutter if the transaction isn't retried until success
"""

import uuid
import msgpack

class RiverfishException(Exception) :
	"""Base exception class for riverfish"""

class NoopException(RiverfishException) :
	"""Nothing occurred; nothing was even partially done."""

class PartialFailureException(RiverfishException) :
	"""The operation failed safely, but will leave some data clutter."""

class SafelyFailedException(RiverfishException) :
	"""Exceptions that resulted from a riverfish operation that did not cause problems, but failed ."""


class RiverAlreadyExistsException(SafelyFailedException, NoopException) :
	"""River already existed.  Could not create."""

class RiverDoesNotExistException(SafelyFailedException, NoopException) :
	"""River does not exist."""

# TODO determine if this qualifies as a NoopException
class RiverDeletedException(SafelyFailedException) :
	"""The River in use was deleted. The current operation failed."""

class ContentionFailureException(SafelyFailedException, PartialFailureException) :
	"""The operation failed partially due to contention."""

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

	def _apack(self, k, v) :
		"""
		adds a value at a specific key location, after packing it
		"""
		return self.client.add(k, msgpack.packs(v))

	def _cupack(self, k, v) :
		"""
		sets a value at a specific key location using cas, after packing it
		"""
		return self.client.cas(k, msgpack.packs(v))

	# river nodes
	def _getRiverNode(self) :
		return self._gupack(self.rnkey)

	def _getsRiverNode(self) :
		return self._gsupack(self.rnkey)

	# index nodes
	def _indexNodeName(self, key, indl) :
		return 't:%s:in:%d:%d' % (self.name, indl, key / indl)

	def _getsIndexNode(self, key, indl) :
		return self._gsupack(self._indexNodeName(key, indl))

	def _getIndexNode(self, key, indl) :
		return self._gupack(self._indexNodeName(key, indl))

	def _addIndexNode(self, key, indl, subindl) :
		index_node = self._getsIndexNode(key, indl)
		subindl_div = key / subindl
		ikey = self._indexNodeName(key, indl)
		if index_node :
			index_node['FIN'] = min(subindl_div, index_node['FIN'])
			index_node['LIN'] = max(subindl_div, index_node['LIN'])
			return self._cupack(ikey, index_node)
		else :
			return self._apack(ikey, {'FIN' : subindl_div, 'LIN' : subindl_div})

	# list nodes (a type of index node)
	def _addMetaData(self, key, indl, metadata) :
		likey = self._indexNodeName(key, indl)
		list_node = self._getsIndexNode(key, indl)
		if list_node :
			meta_list = list(list_node.get(key, []))
			meta_list.append(metadata)
			meta_list.sort(cmp=lambda d1,d2: int.__cmp__(d1['KEY'], d2['KEY']))
			list_node[key] = meta_list
			return self._cupack(likey, meta_list)
		else :
			# TODO benchmark dict vs giant list... perhaps use a plain list here? Not sure.
			# should I optimise for lookups or iteration?
			return self._apack(likey, {key : [metadata]})

	@property
	def count(self) :
		return self._getRiverNode()['TOT']

	"""
	Add a fish to the river, given the fish's metadata.
	"""
	def add(self, key, metadata) :
		river_node = self._getsRiverNode()
		if not river_node :
			raise RiverDeletedException("Once the river flows to the sea, is it still a river?")
		
		if river_node['FIN'] is None :
			index_nodes = {}
			for indl_i in range(len(self.ind) - 1) :
				if not self._addIndexNode(key, self.ind[indl_i], self.ind[indl_i + 1]) :
					raise ContentionFailureException("could not add index node for key %d at level %d" % (key, self.ind[indl_i]))
			low_level = self.ind[len(self.ind)-1]
			if not self._addMetaData(key, low_level, metadata) :
				raise ContentionFailureException("could not add list node for key %d at level %d" % (key, low_level))

			# update main node here? FIN/LIN update...
		else :
			raise RuntimeError("you can only add one thing so far.")

	def get(self, key) :
		river_node = self._getRiverNode()
		if not river_node :
			raise RiverDeletedException("Once the river flows to the sea, is it still a river?")
		
		#if river_node['FIN'] is None or river_node['LIN'] is None :
		#	return []

		low_level = self.ind[len(self.ind)-1]
		meta_data = self._getIndexNode(key, low_level)
		if not meta_data or key not in meta_data :
			return []

		return list(meta_data[key])

	def __iter__(self) :
		return Boat(self)

class Boat(object) :
	def __init__(self, river) :
		self.river = river

	def next() :
		# TODO make this work.
		raise StopIteration()
