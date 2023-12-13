import pickle
import random
import socket
import threading
import time


class node:
    def __init__(self, id, adresses, added=False):
        # 0 -follower 1-candidate 2-leader
        self._random = random.Random(id)
        self._hash_table = {}
        self._state = 0
        if added:
            self._state = -1
        self._leader_id = None
        self._id = id
        self._leader_alive = False
        self.my_addr = adresses.pop(id)
        print(self.my_addr)
        self._other_nodes = adresses
        self._node_states = {}
        for k, _ in adresses.items():
            self._node_states.update({
                k: 2
            })

        self._current_term = 0
        self._voted_for = None
        self._log = ['init']
        self._log_terms = [0]
        self._commit_index = 0
        self._last_applied = 0
        self._next_index = {}
        self._match_index = {}
        self._lock = threading.Lock()
        self._my_clients = {}
        self._election_deadline = 0
        self._thread = threading.Thread(target=self.heartbeat)
        self._thread.start()

    def become_leader(self):
        self._state = 2
        print('I am the leader')
        self._nodes_proceeding_requests = {}
        for i in self._other_nodes.keys():
            self._next_index.update({i: len(self._log)})
            self._match_index.update({i: 0})

    def handle(self, data, client):
        inp_data = pickle.loads(data)
        self._lock.acquire()
        if inp_data['type'] == 'HB' or inp_data['type'] == 'HBR':
            self.handle_heartbeat(inp_data)
            self.check_log()
        elif inp_data['type'] == 'CR':
            self.handle_client_request(inp_data)
        elif inp_data['type'] == 'EL' or inp_data['type'] == 'ELR':
            self.handle_election(inp_data)
        elif inp_data['type'] == 'NUP':
            self.handle_node_update(inp_data)
        elif inp_data['type'] == 'NND':
            self.handle_new_node(inp_data)
        self._lock.release()

    def check_log(self):
        if self._state == 2:
            counts = {}
            for i in self._match_index.values():
                counts.update({i: counts.get(i, 0) + 1})
            for k, v in counts.items():
                if k > self._commit_index and v > (len(self._other_nodes.values()) + 1) / 2 - 1:
                    self._commit_index = k
        if self._commit_index > self._last_applied:
            i = self._last_applied + 1
            while i <= self._commit_index:
                self.apply(i)
                i += 1

    def send(self, address, data):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(address)
            sock.sendall(pickle.dumps(data, pickle.HIGHEST_PROTOCOL))

    def send_to_node(self, id, data):
        try:
            self.send(self._other_nodes[id], data)
        except Exception as err:
            print(f'unable to connect {self._other_nodes[id]} {err}')

    def apply(self, index):
        self._last_applied = index
        if self._log[index] == 'init':
            return

        st = 'fail'
        parsed = self._log[index].split()
        if parsed[0] == 'pop':
            st = self._hash_table.pop(parsed[1], 'fail')
            print(f'Hash table changed: {self._hash_table}')
        elif parsed[0] == 'set':
            self._hash_table.update({parsed[1]: parsed[2]})
            st = 'success'
            print(f'Hash table changed: {self._hash_table}')

        if self._state == 2:
            self.send(self._nodes_proceeding_requests.pop(index), st)

            # self._clients.pop(index).sendall(pickle.dumps(responce,pickle.HIGHEST_PROTOCOL))

    def count_nodes_alive(self):
        self._nodes_alive = 1
        for i in self._node_states.values():
            if i > 0:
                self._nodes_alive += 1

    def _start_election(self):
        self._state = 1
        self._current_term += 1
        self._votes_cnt = 1
        self._total_votes = 1
        self.count_nodes_alive()

        print('leader lost, starting election')
        el_pack = {
            'type': 'EL',
            'id': self._id,
            'term': self._current_term,
            'last_log_index': (len(self._log) - 1),
            'last_log_term': self._log_terms[-1],
            'last_applied': self._last_applied,
            'nodes_alive': self._nodes_alive,
            'vote': 0
        }
        self._election_deadline = time.time() + self._random.randint(4, 10)
        for i in self._other_nodes.keys():
            self.mark_node_as_dead(i)
            self.send_to_node(i, el_pack)

    def handle_election(self, data):
        self.restore_dead_node(data['id'])
        print(data)
        print('^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^')
        if self._state == 1 and data['type'] == 'ELR':
            if self._current_term < data['term']:
                self._current_term = data['term']
                self._state = 0
                return
            if self._last_applied < data['last_applied']:
                self._state = 0
                return
            if self._nodes_alive < data['nodes_alive']:
                self._state = 0
                return

            self._election_deadline = time.time() + self._random.randint(4, 10)
            self._votes_cnt += data['vote']
            self._total_votes += 1
            self.count_nodes_alive()
            if self._votes_cnt > self._nodes_alive / 2 and self._nodes_alive <= self._total_votes:
                self.become_leader()

        if self._state == 1 and data['type'] == 'EL':
            if self._current_term < data['term']:
                self._current_term = data['term']
                self._state = 0
                return
            if self._last_applied < data['last_applied']:
                self._state = 0
                return
            if self._nodes_alive < data['nodes_alive']:
                self._state = 0
                return

        elif self._state == 0 and data['type'] == 'EL':
            self._leader_alive = True
            self.count_nodes_alive()
            reply = {
                'type': 'ELR',
                'id': self._id,
                'term': self._current_term,
                'last_applied': self._last_applied,
                'nodes_alive': self._nodes_alive,
                'vote': 0
            }
            if self._last_applied > data['last_applied']:
                reply.update({'vote': 0})

            if self._current_term > data['term']:
                reply.update({'vote': 0})

            elif len(self._log) - 1 > data['last_log_index']:
                reply.update({'vote': 0})

            else:
                if len(self._log) <= data['last_log_index']:
                    reply.update({'vote': 1})
                elif self._log_terms[data['last_log_index']] == data['last_log_term'] and self._voted_for is None:
                    reply.update({'vote': 1})
                    self._voted_for = data['id']
                else:
                    reply.update({'vote': 0})
            self.send_to_node(data['id'], reply)
            return

    def restore_dead_node(self, id):
        old_val = self._node_states[id]
        new_val = 2
        self._node_states.update({id: new_val})
        if old_val <= 0:
            self.node_update()
            print(f'Node {id} is alive now')

    def mark_node_as_dead(self, id):
        old_val = self._node_states[id]
        if old_val <= 0:
            return
        new_val = old_val - 1
        self._node_states.update({id: new_val})
        if new_val <= 0:
            self.node_update()
            print(f'Node {id} is dead now')

    def node_update(self):
        for i in self._other_nodes.keys():
            other=self._other_nodes.copy()
            other.pop(i)
            other.update({self._id:self.my_addr})
            states=self._node_states.copy()
            states.pop(i)
            states.update({self._id:0})
            pack = {
                'type': 'NUP',
                'id': self._id,
                'nodes': other,
                'states': states
            }
            self.send_to_node(i, pack)

    def handle_node_update(self, data):
        if self._state == -1:
            self._other_nodes = data['nodes']
            self._node_states = data['states']
            print(f'updated node info: {self._node_states}')
            self._state = 0
            print('I am in the node-net now!')
        if self._state == 0:
            if data['id'] == self._leader_id:
                self._other_nodes = data['nodes']
                self._node_states = data['states']
                print(f'updated node info: {self._node_states}')

    def handle_new_node(self, data):
        if self._state == 0 and self._leader_id is not None:
            self.send_to_node(self._leader_id, data)
        elif self._state == 2:
            self._other_nodes.update({data['id']: data['address']})
            self._node_states.update({data['id']: 2})
            id = data['id']
            self._match_index.update({id:0})
            self._next_index.update({id:len(self._log)})
            print(f'Node {id} is in the node-net now!')

    def handle_heartbeat(self, data):
        self._leader_alive = True
        self._voted_for = None
        print(data)
        print('-----------------------------------')
        # print(self._hash_table)
        if self._state == 2 and data['type'] == 'HBR':
            self.restore_dead_node(data['id'])
            if data['term'] > self._current_term:
                self._current_term = data['term']
                self._state = 0
                return
            if data['status'] == 'success':
                self._next_index[data['id']] += data['added']
                self._match_index[data['id']] = data['last_log']
                return
            elif data['status'] == 'fail':
                self._next_index[data['id']] -= 1
                return

        if self._state == 1:
            if data['term'] > self._current_term:
                self._state = 0

        if self._state == 0 and data['type'] == 'HB':
            self._leader_id = data['id']
            if data['term'] > self._current_term:
                self._current_term = data['term']

            if self._current_term > data['term']:
                self.send_to_node(data['id'], {
                    'type': 'HBR',
                    'id': self._id,
                    'status': 'fail',
                    'term': self._current_term
                })
                return
            cond = True
            if len(self._log) > data['prev_log_index']:
                cond = self._log_terms[data['prev_log_index']] != data['prev_log_term']

            if cond:
                self.send_to_node(data['id'], {
                    'type': 'HBR',
                    'id': self._id,
                    'status': 'fail',
                    'term': self._current_term
                })
                return
            tmp = 1
            for term in data['log_terms']:
                if len(self._log_terms) <= data['prev_log_index'] + tmp:
                    break
                if term != self._log_terms[data['prev_log_index'] + tmp]:
                    for i in range(data['prev_log_index'] + tmp, len(self._log_terms)):
                        self._log.pop()
                        self._log_terms.pop()
                    break
                tmp += 1

            self._log.extend(data['log'])
            self._log_terms.extend(data['log_terms'])
            appended_entries = len(data['log'])
            if data['commit_index'] > self._commit_index:
                self._commit_index = min(data['commit_index'], len(self._log) - 1)

            self.send_to_node(data['id'], {
                'type': 'HBR',
                'term': self._current_term,
                'id': self._id,
                'status': 'success',
                'added': appended_entries,
                'last_log': (len(self._log) - 1)
            })

    def create_heartbeat(self, node_id):

        # if self._commit_index > self._next_index[node_id]:
        log = self._log[self._next_index[node_id]:]
        log_terms = self._log_terms[self._next_index[node_id]:]
        pack = {
            'type': 'HB',
            'id': self._id,
            'term': self._current_term,
            'log': log,
            'log_terms': log_terms,
            'prev_log_index': (self._next_index[node_id] - 1),
            'prev_log_term': self._log_terms[self._next_index[node_id] - 1],
            'commit_index': self._commit_index

        }
        return pack

    def heartbeat(self):
        while True:
            if self._state == -1:
                self._lock.acquire()
                pack = {'type': 'NND',
                        'id': self._id,
                        'address': self.my_addr
                        }
                for i in self._other_nodes.keys():
                    self.send_to_node(i, pack)
                self._lock.release()
                time.sleep(10)
            if self._state == 0:
                time.sleep(self._random.randint(10, 15))
                self._lock.acquire()
                if not self._leader_alive:
                    self._start_election()
                self._leader_alive = False
                self._lock.release()
            elif self._state == 2:
                self._lock.acquire()
                for i in self._other_nodes.keys():
                    self.mark_node_as_dead(i)
                    self.send_to_node(i, self.create_heartbeat(i))
                self._lock.release()
                time.sleep(1)
            elif self._state == 1:
                self._lock.acquire()
                if time.time() > self._election_deadline:
                    if self._state == 1:
                        self._state = 0
                self._lock.release()

    def handle_client_request(self, data):
        parsed = data['request'].split()
        if parsed[0] == 'get':
            self.send(data['client'], self._hash_table.get(parsed[1], 'No such key'))
            return
        if self._state == 2:
            self._log.append(data['request'])
            self._nodes_proceeding_requests.update({len(self._log) - 1: data['client']})
            self._log_terms.append(self._current_term)
            print(self._log)
        elif self._state == 0:
            if self._leader_id is not None:
                self.send_to_node(self._leader_id, data)
            # for k in self._other_nodes.keys():
            #     pack = self.create_heartbeat(k)
            #     self.send_to(k, pack)
