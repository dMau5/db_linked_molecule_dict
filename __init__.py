from CGRtools.containers import MoleculeContainer
from collections import MutableMapping
from itertools import chain
from pickle import loads, dumps
from redis import Redis


class RedisDict(MutableMapping):

    def __init__(self, name):
        self.rdb = Redis()
        self._dict = {}
        self._molecule_dict = {}
        self._name = name

    def __getitem__(self, key):
        if isinstance(key, MoleculeContainer):
            return self._molecule_dict[key.get_signature_hash()]
        else:
            return self._dict[key]

    def __setitem__(self, key, value):
        if isinstance(key, MoleculeContainer):
            key_hash = key.get_signature_hash()
            self.rdb.hset(self._name, key_hash, dumps(key))
            self._molecule_dict[key_hash] = value
        else:
            self._dict[key] = value

    def __delitem__(self, key):
        if isinstance(key, MoleculeContainer):
            key = key.get_signature_hash()
            self.rdb.hdel(self._name, key)
            del self._molecule_dict[key]
        else:
            del self._dict[key]

    def __iter__(self):
        return chain(self._dict, (loads(self.rdb.hget(self._name, x)) for x in self._molecule_dict))

    def __len__(self):
        return len(self._dict) + len(self._molecule_dict)
