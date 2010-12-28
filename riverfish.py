"""
* what happens if all of the memcached nodes get restarted between gets() and cas()?  Does reconnect flush anything? Are cas tokens still valid?
* what happens if memcached temporarily fails at any point?
* what happens if memcached permanently fails at some point?
* sharing client? can this cause issues?
* metadata currently can't have any lists in it (directly), they get turned into tuples...
** threadsafe for one client to be accessed from multiple threads?  The cache for cas is shared... NOT SAFE
* keeping one client over the length of operations with a river object? this could be bad too..
* if I am going to allow reindexing, river objects can't cache IND anymore.
* transaction failure during index node creation can produce index node clutter if the transaction isn't retried until success
* check that every added metadata has a KEY which is an int and probably has UUID, size, mime type, and encoding?
* maybe move away from msgpack, it's not great for list/tuple differences
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

# TODO verify that the code works even with less levels, or document the limit
DEFAULT_INDEX_LEVELS = [10000000, 1000000, 100000, 10000]

class River(object) :
	# TODO validate name as fitting a regex
	def __init__(self, client, name, create=False) :
		self.client = client
		self.name = name
		self.rnkey = 't:%s:rn' % self.name
		
		if create :
			self.ind = DEFAULT_INDEX_LEVELS[:]

			data = {
				'IND' : self.ind,
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

	def _addIndexNode(self, key, indl) :
		index_node = self._getsIndexNode(key, indl)
		ikey = self._indexNodeName(key, indl)
		if index_node :
			index_node['FIN'] = min(key, index_node['FIN'])
			index_node['LIN'] = max(key, index_node['LIN'])
			return self._cupack(ikey, index_node)
		else :
			return self._apack(ikey, {'FIN' : key, 'LIN' : key})

	# list nodes (a type of index node)
	def _addMetaData(self, key, indl, metadata) :
		likey = self._indexNodeName(key, indl)
		list_node = self._getsIndexNode(key, indl)
		if list_node :
			meta_list = list(list_node.get(key, []))
			if metadata in meta_list :
				# retries won't know if it's in there yet. Just succeed if the exact metadata exists already.
				return True
			meta_list.append(metadata)
			meta_list.sort(cmp=lambda d1,d2: int.__cmp__(d1['KEY'], d2['KEY']))
			list_node[key] = meta_list
			return self._cupack(likey, list_node)
		else :
			# TODO benchmark dict vs giant list... perhaps use a plain list here? Not sure.
			# should I optimise for lookups or iteration?
			return self._apack(likey, {key : [metadata]})

	@property
	def count(self) :
		# TODO how to make this count work correctly in cases of transaction failure during add.. it's not done 
		return self._getRiverNode()['TOT']

	"""
	Add a fish to the river, given the fish's metadata.
	"""
	def add(self, key, metadata) :
		river_node = self._getsRiverNode()
		if not river_node :
			raise RiverDeletedException("Once the river flows to the sea, is it still a river?")
		
		for indl_i in xrange(len(self.ind) - 1) :
			if not self._addIndexNode(key, self.ind[indl_i]) :
				raise ContentionFailureException("could not add/update index node for key %d at level %d" % (key, self.ind[indl_i]))
		low_level = self.ind[len(self.ind)-1]
		if not self._addMetaData(key, low_level, metadata) :
			raise ContentionFailureException("could not add list node for key %d at level %d" % (key, low_level))

		updated = False
		if river_node['FIN'] is None :
			river_node['FIN'] = key
			updated = True
		else :
			if river_node['FIN'] != key :
				river_node['FIN'] = min(river_node['FIN'], key)
				updated = True
		if river_node['LIN'] is None :
			river_node['LIN'] = key
			updated = True
		else :
			if river_node['LIN'] != key :
				river_node['LIN'] = max(river_node['LIN'], key)
				updated = True

		if updated and not self._cupack(self.rnkey, river_node) :
			raise ContentionFailureException("could not update the river node for FIN/LIN update.")

	def get(self, key) :
		river_node = self._getRiverNode()
		if not river_node :
			raise RiverDeletedException("Once the river flows to the sea, is it still a river?")

		low_level = self.ind[len(self.ind)-1]
		meta_data = self._getIndexNode(key, low_level)
		if not meta_data or key not in meta_data :
			return []

		return list(meta_data[key])

	@property
	def reverse(self) :
		return Wave(self, options=['REV'])

	def __iter__(self) :
		return Boat(self)

class Wave(River) :
	def __init__(self, river, options=[]) :
		self.river = river
		self.options = options

	def __getattr__(self, attr) :
		return getattr(self.river, attr)

class Boat(object) :
	def __init__(self, river) :
		self.river = river
		self.iter = self.iterate()

	def iterate(self) :
		reverse = False
		if hasattr(self.river, 'options') :
			if 'REV' in self.river.options :
				reverse = True

		OP_GET_RN = 0
		OP_GET_IN = 1
		OP_GET_LN = 2

		stack = []
		stack.append((OP_GET_RN, None))
		while stack :
			op, arg = stack.pop()
			if op == OP_GET_RN :
				rn = self.river._getRiverNode()
				ind = rn['IND']
				fin = rn['FIN']
				lin = rn['LIN']
				if fin is not None and lin is not None :
					iind = 0
					fks = fin - (fin % ind[iind])
					lks = lin - (lin % ind[iind])
					if reverse :
						sub = range(fks, lks+1, ind[iind])
					else :
						sub = xrange(lks, fks-1, -ind[iind])
					for key in sub :
						# reverse the order of top level index lookups, add to the stack (start low)
						stack.append((OP_GET_IN, (key, 0)))
			elif op == OP_GET_IN :
				key, iind = arg
				index_node = self.river._getIndexNode(key, ind[iind])
				if not index_node :
					continue
				fin = index_node['FIN']
				lin = index_node['LIN']
				if fin is not None and lin is not None :
					next_iind = iind + 1
					if next_iind < len(ind) - 1 :
						next_op = OP_GET_IN
					else :
						next_op = OP_GET_LN
					fks = fin - (fin % ind[next_iind])
					lks = lin - (lin % ind[next_iind])

					if reverse :
						sub = range(fks, lks+1, ind[next_iind])
					else :
						sub = xrange(lks, fks-1, -ind[next_iind])
					for key in sub :
						stack.append((next_op, (key, next_iind)))
			elif op == OP_GET_LN :
				key, iind = arg
				list_node = self.river._getIndexNode(key, ind[iind])
				if not list_node :
					continue
				list_keys = list_node.keys()
				list_keys.sort(reverse=reverse)
				if reverse :
					for key in list_keys :
						lv = list(list_node[key])
						lv.reverse()
						for value in lv :
							yield key, value						
				else :
					for key in list_keys :
						for value in list_node[key] :
							yield key, value

	def next(self) :
		return self.iter.next()
