import pickle
import random
import socket
import threading
import time

TTL = 10


class node:
    def __init__(self, id, adresses):
        # 0 -follower 1-candidate 2-leader
        self._random = random.Random(id)
        self._hash_table = {}
        self._state = 0
        self._leader_id = None
        self._id = id
        self._leader_alive = False
        self.my_addr = adresses.pop(id)
        print(self.my_addr)
        self._other_nodes = adresses
        self._current_term = 0
        self._voted_for = None
        self._log = ['init']
        self._log_terms = [0]
        self._commit_index = 0
        self._last_applied = 0
        self._next_index = {}
        self._match_index = {}
        self._lock = threading.Lock()
        self._cas_lock = threading.Lock()
        self._election_deadline = 0
        self._key_table = {}
        self._key_info = {}
        self._thread = threading.Thread(target=self.heartbeat)
        self._thread.start()

    def become_leader(self):
        self._state = 2
        print('I am the leader')
        self._return_addresses = {}
        for i in self._other_nodes.keys():
            self._next_index.update({i: len(self._log)})
            self._match_index.update({i: 0})

    def handle(self, data, client):
        dict = pickle.loads(data)
        if type(dict) is not type({}):
            return
        self._lock.acquire()
        if dict['type'] == 'HB' or dict['type'] == 'HBR':
            self.handle_heartbeat(dict)
            self.check_log()
        elif dict['type'] == 'CR':
            self.handle_client_request(dict)
        elif dict['type'] == 'EL' or dict['type'] == 'ELR':
            self.handle_election(dict)
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
            try:
                sock.connect(address)
                sock.sendall(pickle.dumps(data, pickle.HIGHEST_PROTOCOL))
            except Exception as err:
                print(f'unable to connect {address} {err}')

    def send_to_node(self, id, data):
        self.send(self._other_nodes[id], data)

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
        elif parsed[0] == 'lock':
            st = self._lockn(parsed[1], parsed[2])
            print(self._key_table)
            if parsed[2] == self._id:
                print('I got the lock ' + parsed[1])
        elif parsed[0] == 'unlock':
            st = self._unlock(parsed[1], parsed[2])
            print(self._key_table)
            if parsed[2] == self._id:
                print('I lost the lock ' + parsed[1])
        if self._state == 2:
            self.send(self._return_addresses.pop(index), st)

            # self._clients.pop(index).sendall(pickle.dumps(responce,pickle.HIGHEST_PROTOCOL))

    def _start_election(self):
        self._state = 1
        self._current_term += 1
        self._votes_cnt = 1
        print('leader lost, starting election')
        el_pack = {
            'type': 'EL',
            'id': self._id,
            'term': self._current_term,
            'last_log_index': (len(self._log) - 1),
            'last_log_term': self._log_terms[-1],
            'last_applied': self._last_applied,
            'vote': 0
        }
        self._election_deadline = time.time() + self._random.randint(4, 10)
        for i in self._other_nodes.keys():
            self.send_to_node(i, el_pack)

    def handle_election(self, data):
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
            self._election_deadline = time.time() + self._random.randint(4, 10)
            self._votes_cnt += data['vote']
            if self._votes_cnt > (len(self._other_nodes.keys()) + 1) / 2:
                self.become_leader()
        elif self._state == 0 and data['type'] == 'EL':
            self._leader_alive = True
            reply = {
                'type': 'ELR',
                'id': self._id,
                'term': self._current_term,
                'last_applied': self._last_applied,
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

    def handle_heartbeat(self, data):
        self._leader_alive = True
        self._voted_for = None
        #print('-----------------------------------')
        # print(self._hash_table)
        if self._state == 2 and data['type'] == 'HBR':
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
            if self._state == 0:
                time.sleep(self._random.randint(10, 15))
                self._lock.acquire()
                if not self._leader_alive:
                    self._start_election()
                self._leader_alive = False
                self._lock.release()
            elif self._state == 2:
                for i in self._other_nodes.keys():
                    self._lock.acquire()
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
        print(data)
        parsed = data['request'].split()
        if parsed[0] == 'get':
            self.send(data['client'], self._hash_table.get(parsed[1], 'No such key'))
            return
        elif parsed[0] == 'sleep':
            time.sleep(10)
        if self._state == 2:
            self._log.append(data['request'])
            self._return_addresses.update({len(self._log) - 1: data['client']})
            self._log_terms.append(self._current_term)
            print(self._log)
        elif self._state == 0:
            if self._leader_id is not None:
                self.send_to_node(self._leader_id, data)
            # for k in self._other_nodes.keys():
            #     pack = self.create_heartbeat(k)
            #     self.send_to(k, pack)

    def _delete_key(self, key):
        self._cas_lock.acquire()
        self.send_unlock(key, self.my_addr)
        self._cas_lock.release()
        print('TTL ended')

    def _cas(self, key, new_val, old_val, version, ttl=TTL):
        self._cas_lock.acquire()
        if self._key_table.get(key, None) == old_val:
            tp = self._key_info.get(key, [None, my_thread(ttl, lambda: self._delete_key(key))])
            if tp[0] != version and tp[0] is not None:
                res = False
            else:
                self._key_table.update({key: new_val})
                if self._state == 2:
                    tp[1].start()
                tp[0] = version
                self._key_info.update({key: tp})
                res = True

        else:
            res = False
        self._cas_lock.release()
        return res

    def _relock(self, lock_name, ver):
        self._cas(lock_name, 1, 1, ver)

    def _lockn(self, lock_name, ver):
        if locked := self._cas(lock_name, 1, 0, ver):
            return locked
        return self._cas(lock_name, 1, None, ver)

    # версия None чтобы показать что блокировка снята иначе до выхода TTL после unlock нельзя будет сделать lock
    def _unlock(self, lock_name, ver=None):
        if res:=self._cas(lock_name, 0, 1, ver):
            self._key_info.pop(lock_name)
        return res

    def send_lock(self, lock_name, client):
        pach = {'type': 'CR',
                'client': client,
                'request': f'lock {lock_name} {self._key_info.get(lock_name,[self._id,None])[0]}'}
        if self._state == 2:
            self.send(self.my_addr, pach)
            return
        self.send_to_node(self._leader_id, pach)

    def send_unlock(self, lock_name, client):
        pach = {'type': 'CR',
                'client': client,
                'request': f'unlock {lock_name} {self._key_info.get(lock_name,[self._id,None])[0]}'}
        if self._state == 2:
            self.send(self.my_addr, pach)
            return
        self.send_to_node(self._leader_id, pach)


class my_thread():
    def __init__(self, ttl, target):
        self.lock = threading.Lock()
        self.is_running = False
        self.target_time = time.time() + ttl
        self.ttl = ttl
        self.target = target
        self.thread = None

    def run(self):
        self.is_running = True
        while (True):
            self.lock.acquire()
            if time.time() > self.target_time:
                break
            self.lock.release()
        self.target()
        self.is_running = False

    def start(self):
        if self.is_running:
            self.reload()
        else:
            self.thread = threading.Thread(target=self.run)
            self.thread.start()

    def reload(self):
        self.lock.acquire()
        self.target_time = time.time() + self.ttl
        self.lock.release()
