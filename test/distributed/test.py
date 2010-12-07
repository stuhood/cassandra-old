#!/usr/bin/env python
#
# System Tests for Incremental Counters on Cassandra
#

import os
import multiprocessing
import random
import re
import struct
import subprocess
import sys
import time

from thrift.protocol  import TBinaryProtocol
from thrift.transport import THttpClient
from thrift.transport import TSocket
from thrift.transport import TTransport

from cassandra.ttypes import *
from cassandra        import Cassandra

CASSANDRA_PORT = 9160
CASSANDRA_HOSTS = [
#TODO: fill in your hosts
	]

TOOL_DIRECTORY = os.path.dirname(__file__) + '/bin/'
TOOLS = [
	'clustertool',
	'nodetool',
	]

# verify required tools exist
#for tool in TOOLS:
#	try:
#		f = open(TOOL_DIRECTORY + tool, 'r')
#	except IOError, ioe:
#		print 'Error: %s could not be found at: %s' % (tool, TOOL_DIRECTORY,)
#		if ioe.errno != 2:
#			print ioe
#		sys.exit(1)

REPLICA_RE = re.compile('\[(.*)\]')

CONCURRENCY_NUM_PROCS   = 20
CONCURRENCY_NUM_INSERTS = 100

KEYSPACE      = 'Keyspace1'
KEY           = 'x'
COLUMN_FAMILY = 'Counter1'
COLUMN_PATH   = ColumnPath(
		column_family = COLUMN_FAMILY,
		column        = KEY,
	)
COLUMN_PARENT   = ColumnParent(
		column_family = COLUMN_FAMILY
	)	

MAX_TIMEOUT = 60 * 2


class TestClient(object):
	def __init__(self, host):
		# setup client
		self._socket    = TSocket.TSocket(host, CASSANDRA_PORT)
		self._transport = TTransport.TFramedTransport(TTransport.TBufferedTransport(self._socket))
		self._protocol  = TBinaryProtocol.TBinaryProtocol(self._transport)
		self._client    = Cassandra.Client(self._protocol)

		# open connection
		self.open()

	def open(self):
		self._transport.open()
		self._client.set_keyspace(KEYSPACE)

	def incr(self, key, delta=1):
		self._client.add(
			key,
			COLUMN_PARENT,
			CounterUpdate(
				counter		= Counter(KEY, delta),
				timestamp	= int(time.time())
				),
			ConsistencyLevel.ONE
			)

	def read(self, key, consistency):
		return self._client.get_counter(
			key,
			COLUMN_PATH,
			consistency
			)

def increment(host, key, num_incr):
	client = TestClient(host)

	try:
		for i in xrange(num_incr):
			num_tries = 3
			timeout   = 5
			for j in xrange(num_tries):
				sys.stdout.flush()
				try:
					client.incr(key)
					sys.stdout.write('.')
					break
				except TimedOutException, toe:
					sys.stdout.write('!')
					time.sleep(timeout)
					timeout=min(60, timeout * 2)
	except KeyboardInterrupt, ki:
		raise SystemExit

def concurrent_increment(hosts, key, num_procs, num_incr):
	randomized_hosts = []
	while len(randomized_hosts) < num_procs:
		random.shuffle(hosts)
		randomized_hosts.extend(hosts)

	try:
		procs = [multiprocessing.Process(
			target = increment,
			args   = (randomized_hosts[i], key, num_incr)
			) for i in xrange(num_procs)]
		print '  INCR PROCESSES: START'
		[p.start() for p in procs]
		[p.join()  for p in procs]
		print
		print '  INCR PROCESSES: END'
	except KeyboardInterrupt, ki:
		raise SystemExit

def _execute_action(host, action):
	process = subprocess.Popen([
		TOOL_DIRECTORY + 'nodetool',
		'-h', host,
		action,
		KEYSPACE,
		COLUMN_FAMILY],
		stdout=subprocess.PIPE)

class BaseTest(object):
	def __init__(self):
		self.existing_hosts = CASSANDRA_HOSTS

		self.desc      = 'base test (abstract)'
		self.num_tries = 15
		self.timeout   = 1

	def _create_temporary_key(self):
		return 'test.key.%f' % (time.time(),)

	def _find_replicas(self, hosts):
		# find replicas
		try:
			process = subprocess.Popen([
				TOOL_DIRECTORY + 'clustertool',
				'-h', hosts[0],
				'get_endpoints',
				KEYSPACE,
				COLUMN_FAMILY,
				self.key],
				stdout=subprocess.PIPE)
			replicas = REPLICA_RE.findall(process.stdout.read())[0]
			replicas = replicas.split(', ')
			replicas = [r[1:] for r in replicas]
			replicas = [re.sub('.*/', '', r) for r in replicas]

			return replicas
		except Exception, e:
			print '  ERROR: processing response from get_endpoints'
			raise e
	
	def setup(self):
		# key
		self.key    = self._create_temporary_key()
		print '  key:  %s' % (self.key,)

		# replicas for key
		self.replicas = self._find_replicas(self.existing_hosts)
		print '  replicas:'
		for r in self.replicas:
			print '    %s' % (r,)
		print

		# target value
		self.target = 0

	def _insert(self, hosts):
		concurrent_increment(
			hosts,
			self.key,
			CONCURRENCY_NUM_PROCS,
			CONCURRENCY_NUM_INSERTS)
		self.target += CONCURRENCY_NUM_PROCS * CONCURRENCY_NUM_INSERTS

	def _backoff(self, multiplier):
		print '   ... pausing for: %ss' % (self.timeout,)
		time.sleep(self.timeout)
		self.timeout = min(MAX_TIMEOUT, self.timeout * multiplier)
	
	def _read_host(self, host):
		read_client = TestClient(host)

		value = 0
		for i in xrange(self.num_tries):
			try:
				count = read_client.read(self.key, ConsistencyLevel.QUORUM)
			except Exception, e:
				self._backoff(5)
				continue

			value   = count.value
			print '  value: %d' % (value,)

			if value == self.target:
				break
			sys.stdout.write('*')
			self._backoff(2)

		return value

	def _read(self, hosts=CASSANDRA_HOSTS):
		for read_host in hosts:
			self._read_host(read_host)

	def _verify(self, hosts=CASSANDRA_HOSTS):
		# check read from each node
		for read_host in hosts:
			print '  reading from %s:' % (read_host,)
			value = self._read_host(read_host)

			# consistency check
			if value == self.target:
				print '  PASS: host: %s' % (read_host,)
				continue
			print '  FAIL: host: %s; value: %s; target: %s' % (
				read_host, value, self.target)
		print

	def test(self):
		raise Exception('test() must be overridden in BaseTest')

	def run(self):
		print '-' * 50
		print 'TEST: %s' % (self.desc,)

		self.setup()
		self.test()

class RepairOnWriteTest(BaseTest):
	# write: into one node
	# read:  from each node (should be correct, already)

	def __init__(self):
		super(RepairOnWriteTest, self).__init__()
		self.desc = 'repair-on-write test: write to one replica, check all are caught up'
	
	def test(self):
		for host in CASSANDRA_HOSTS:
			print '  insert into: %s' % (host,)

			# insert into target replica
			self._insert([host])
			time.sleep(15)

			# check first read from each host
			for read_host in CASSANDRA_HOSTS:
				read_client = TestClient(read_host)
				try:
					count = read_client.read(self.key, ConsistencyLevel.ONE)
					value = count.value
					if value == self.target:
						print '  PASS: host: %s' % (read_host,)
						continue
					print '  FAIL: host: %s; value: %s; target: %s' % (
						read_host, value, self.target)
				except Exception, e:
					print '  FAIL: host: %s; exception: %s' % (read_host, e)
				finally:
					print

class OneNodeWriteTest(BaseTest):
	# write: into one node
	# read:  from each node in cluster

	def __init__(self):
		super(OneNodeWriteTest, self).__init__()
		self.desc = 'write to a single node'

	def test(self):
		for host in CASSANDRA_HOSTS:
			print '  insert into: %s:' % (host,)

			# insert into target node
			self._insert([host])

			self._verify()

class AllNodesWrite(BaseTest):
	# write: into all nodes
	# read:  from each node in cluster

	def __init__(self):
		super(AllNodesWrite, self).__init__()
		self.desc = 'write to all nodes'
	
	def test(self):
		# insert into all nodes
		self._insert(CASSANDRA_HOSTS)

		self._verify()

def execute_action(host, action):
	process = subprocess.Popen([
		TOOL_DIRECTORY + 'nodetool',
		'-h', host,
		action,
		KEYSPACE,
		COLUMN_FAMILY],
		stdout=subprocess.PIPE)

class NodeActionTest(BaseTest):
	# write:     into each node
	# <actions>: node(s) in replica set
	# read:      from each node in cluster
	
	def __init__(self, actions, timeout, num_nodes=1, post_action_write=False):
		super(NodeActionTest, self).__init__()
		self.desc = 'write to all nodes, then initiate: %s on a replica' % (actions,)

		self.actions			= actions
		self.actions_timeout	= timeout

		self.num_nodes = num_nodes

		self.post_action_write = False

	def test(self):
		# insert into all nodes
		self._insert(CASSANDRA_HOSTS)

		# initiate actions on node(s)
		random.shuffle(self.replicas)
		for action in self.actions:
			for replica in self.replicas[:self.num_nodes]:
				execute_action(
					replica,
					action)

			print '    ... pausing (after %s) for: %ss' % (action, self.actions_timeout,)
			time.sleep(self.actions_timeout)

		# do post-action write
		if self.post_action_write:
			self._insert(CASSANDRA_HOSTS)
		
		self._verify()

def execute_cassandra_init(host, action):
	process = subprocess.Popen([
		'ssh',
		'%s' % (host,),
#TODO: fill in command
        ],
		stdout=subprocess.PIPE)

class ReplicaRestartTest(BaseTest):
	# write: into each node
	# fail:  restart node(s) in replica set
	# read:  from each node in cluster

	def __init__(self, num_nodes=1):
		super(ReplicaRestartTest, self).__init__()
		self.desc = 'write to all nodes, then restart one replica'

		self.num_nodes = num_nodes
	
	def test(self):
		print '  incrementing'
		self._insert(CASSANDRA_HOSTS)

		time.sleep(60)

		random.shuffle(self.replicas)
		for replica in self.replicas[:self.num_nodes]:
			print '  stopping  %s' % (replica,)
			execute_cassandra_init(replica, 'stop')
		time.sleep(60)

		for replica in self.replicas[:self.num_nodes]:
			print '  starting  %s' % (replica,)
			execute_cassandra_init(replica, 'start')
		time.sleep(60 * 2)

		self._verify()

class ReadRepairTest(BaseTest):
	# write: into each node
	# fail:  restart a node in replica set
	# read:  from each node in cluster

	def __init__(self):
		super(ReadRepairTest, self).__init__()
		self.desc = 'write to all nodes, then stop one replica, write some more, bring the replica up again, read (repair), bring all other nodes down and check value'
	
	def test(self):
		print '  incrementing'
		self._insert(CASSANDRA_HOSTS)

		# check read from each node
		self._verify()

		# stop one replica
		random.shuffle(self.replicas)
		replica = self.replicas.pop(0)
		print '  stopping  %s' % (replica,)
		execute_cassandra_init(replica, 'stop')
		time.sleep(60)

		# write more, while node is down
		self._insert(self.replicas)

		print '  restarting  %s' % (replica,)
		execute_cassandra_init(replica, 'start')
		time.sleep(60 * 2)

		self._verify(self.replicas)
		time.sleep(15)
		
		self._verify([replica])
		time.sleep(60)

class OneReplicaStopReadTest(BaseTest):
	# write: into each node
	# fail:  stop a node in replica set
	# read:  from live nodes in cluster

	def __init__(self):
		super(OneReplicaStopReadTest, self).__init__()
		self.desc = 'write to all nodes, then restart one replica'
	
	def test(self):
		print '  incrementing'
		self._insert(CASSANDRA_HOSTS)

		time.sleep(60)

		random.shuffle(self.replicas)
		replica = self.replicas.pop(0)
		print '  stopping  %s' % (replica,)
		execute_cassandra_init(replica, 'stop')
		time.sleep(60)

		self._verify(self.replicas)

		print '  restarting  %s' % (replica,)
		execute_cassandra_init(replica, 'start')
		time.sleep(60 * 2)

class OneNodeAESTest(BaseTest):
	# write:   into each node
	# read:    from live nodes in cluster
	# fail:    stop a node in replica set
	# write:   into live nodes in cluster
	# recover: failed node
	# action:  initiate repair (AES) on previously failed node
	# fail:    all replicas, except previously failed replica
	# verify:  on remaining replica

	def __init__(self):
		super(OneNodeAESTest, self).__init__()
		self.desc = 'write to all nodes, bring one replica down, write some more, bring replica up, force (aes) repair, bring all other replicas down, read and verify'
	
	def test(self):
		self._insert(self.replicas)

		print '  target: %d' % (self.target,)	

		print '  reading from replicas'
		self._read(self.replicas)

		replica = random.choice(self.replicas)
		print '  stopping  %s' % (replica,)
		execute_cassandra_init(replica, 'stop')
		time.sleep(50)

		print '  incrementing some more'
		self.replicas.remove(replica)
		self._insert(self.replicas)

		print 'target: %d' % (self.target,)
		print 'starting  %s' % (replica,)
		execute_cassandra_init(replica, 'start')
		time.sleep(120)

		print 'executing repair on replica %s' % (replica,)
		# initiate action on one node
		execute_action(replica, 'repair')

		# wait for action to complete
		time.sleep(40)

		for r in self.replicas:
			print '  stopping  %s' % (r,)
			execute_cassandra_init(r, 'stop')

		time.sleep(60)

		print '  reading'
		self._verify([replica])

		for r in self.replicas:
			print '  restarting  %s' % (r,)
			execute_cassandra_init(r, 'start')

class OneNodeHintedHandoffTest(BaseTest):
	# write into each node
	# stop a replica
	# write into remaining replicas
	# restart replica
	# wait for HH to run its course (kicked off by gossip FD)
	# verify

	def __init__(self):
		super(OneNodeHintedHandoffTest, self).__init__()
		self.desc = 'write to all nodes, bring one replica down, write some more, bring replica up, let hinted handoff do its thing, bring all other replicas down, read and verify'
	
	def test(self):
		print '  incrementing'
		self._insert(CASSANDRA_HOSTS)

		replica = random.choice(self.replicas)
		print '  stopping  %s' % (replica,)
		execute_cassandra_init(replica, 'stop')
		print '  waiting to make sure the node falls out of the cluster'
		time.sleep(2*60)

		print '  incrementing some more'
		self.replicas.remove(replica)
		self._insert(self.replicas)

		print '  starting  %s' % (replica,)
		execute_cassandra_init(replica, 'start')

		print '  waiting for hinted handoff'
		time.sleep(60 * 2)

		for r in self.replicas:
			print '  stopping  %s' % (r,)
			execute_cassandra_init(r, 'stop')

		time.sleep(30)

		self._verify()

		for r in replicas:
			print '  re-starting  %s' % (r,)
			execute_cassandra_init(r, 'start')

class BootstrapTest(BaseTest):
	# write:    into each node
	# boostrap: start host that will bootstrap
	# read + write from bootstrapping node
	#TODO: work out potential problems to test

	def __init__(self):
		super(BootstrapTest, self).__init__()
		self.desc = 'test bootstrapping'

		self.existing_hosts = [
#TODO: fill in hosts
			]
        self.bootstrap_host = #TODO: fill in target host
	
	def setup(self):
		# shouldn't be running
		execute_cassandra_init(self.bootstrap_host, 'stop')

		super(BootstrapTest, self).setup()

	def test(self):
		# verify, not bootstrapped
		if self.bootstrap_host in self.replicas:
			print 'ERROR!!!! Bootstrap host already in replicas!'

		for host in self.existing_hosts:
			self._insert([host])

			self._verify()
		
		# bootstrap node
		print '  starting bootstrap node: %s' % (self.bootstrap_host,)
		execute_cassandra_init(replica, 'restart')

		print '  ... waiting a few minutes for bootstrap to complete'
		time.sleep(60 * 4)

		# find replicas for key, after bootstrap
		self.replicas = self._find_replicas(self.existing_hosts)
		if self.bootstrap_host in self.replicas:
			print '  Bootstrap host FOUND in replicas'
		else:
			print '  [KILLING] Bootstrap host NOT FOUND in replicas!'
			return

		print '  Shutting down all other replicas, besides the bootstrapped one'
		for replica in self.existing_hosts:
			print '  stopping %s' % (replica,)
			execute_cassandra_init(replica, 'stop')
		time.sleep(60)

		# verify on bootstrapped node, which is a replica
		self._verify(hosts=[self.bootstrap_host])

		for replica in self.existing_hosts:
			print '  restarting %s' % (replica,)
			execute_cassandra_init(replica, 'start')
		time.sleep(60 * 2)

		# re-verify w/ replicas
		self._verify(self.replicas)

def main():
	tests = [
		RepairOnWriteTest(),

#		OneNodeWriteTest(),
#		AllNodesWrite(),

#		NodeActionTest(['flush'],	10),
#		NodeActionTest(['compact'],	10),
#		NodeActionTest(['cleanup'],	20),
#		NodeActionTest(['repair'],	30),

#		NodeActionTest(['flush'],	10, num_nodes=2),
#		NodeActionTest(['compact'],	30, num_nodes=2),
#		NodeActionTest(['cleanup'],	20, num_nodes=2),
#		NodeActionTest(['repair'],	60, num_nodes=2),

#		NodeActionTest(['flush'],	10, num_nodes=len(CASSANDRA_HOSTS)),
#		NodeActionTest(['compact'],	30, num_nodes=len(CASSANDRA_HOSTS)),
#		NodeActionTest(['cleanup'],	20, num_nodes=len(CASSANDRA_HOSTS)),
#		NodeActionTest(['repair'],	60, num_nodes=len(CASSANDRA_HOSTS)),

#		NodeActionTest(['flush'],	10, num_nodes=len(CASSANDRA_HOSTS), post_action_write=True),
#		NodeActionTest(['compact'],	30, num_nodes=len(CASSANDRA_HOSTS), post_action_write=True),
#		NodeActionTest(['cleanup'],	20, num_nodes=len(CASSANDRA_HOSTS), post_action_write=True),
#		NodeActionTest(['repair'],	60, num_nodes=len(CASSANDRA_HOSTS), post_action_write=True),

#		NodeActionTest(['flush']   * 3,	10, num_nodes=len(CASSANDRA_HOSTS)),
#		NodeActionTest(['flush']   * 3,	 0, num_nodes=len(CASSANDRA_HOSTS)),
#		NodeActionTest(['compact'] * 3,	30, num_nodes=len(CASSANDRA_HOSTS)),
#		NodeActionTest(['cleanup'] * 3,	20, num_nodes=len(CASSANDRA_HOSTS)),
#		NodeActionTest(['repair']  * 3,	60, num_nodes=len(CASSANDRA_HOSTS)),

#		NodeActionTest(['flush', 'flush', 'compact', 'compact'], 60, num_nodes=len(CASSANDRA_HOSTS)),

#		NodeActionTest(['flush', 'repair', 'flush', 'repair'], 60, num_nodes=len(CASSANDRA_HOSTS)),

#		ReplicaRestartTest(),
#		ReplicaRestartTest(num_nodes=2),
#		ReplicaRestartTest(num_nodes=3),

#		ReadRepairTest(),

#		OneReplicaStopReadTest(),

#		OneNodeAESTest(),

#		OneNodeHintedHandoffTest(),

#		BootstrapTest(),
		]
	for test in tests:
		test.run()

if __name__ == '__main__':
    main()




