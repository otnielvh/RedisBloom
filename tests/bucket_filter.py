#!/usr/bin/env python
from rmtest import ModuleTestCase
from redis import ResponseError
import sys

if sys.version >= '3':
    xrange = range


class CuckooTestCase(ModuleTestCase('../redisbloom.so')):

    def test_add_exists(self):
        data = {
            'a': ['a', 'b', 'd'],
            'b': ['d', 'e', 'f']
        }

        # insert data
        for key, values in data.iteritems():
            for value in values:
                self.assertEqual(1, self.cmd("BBF.ADD", key, value))

        # test data is there
        for key, values in data.iteritems():
            for value in values:
                self.assertEqual(1, self.cmd("BBF.EXISTS", key, value))

        # assert random values don't exist
        self.assertEqual(0, self.cmd("BBF.EXISTS", 'a', 'e'))
        self.assertEqual(0, self.cmd("BBF.EXISTS", 'b', 'a'))
        self.assertEqual(0, self.cmd("BBF.EXISTS", 'a', 'bcd'))

    def test_clear_time(self):
        data = {
            'a': ['a', 'b', 'd'],
            'b': ['d', 'e', 'f']
        }

        # insert data
        for key, values in data.iteritems():
            for value in values:
                self.assertEqual(1, self.cmd("BBF.ADD", key, value))

        # increment time
        self.assertEqual(1, self.cmd("BBF.INCTIME", 'a'))

        # insert new data
        self.assertEqual(1, self.cmd("BBF.ADD", 'a', 'new_time'))

        # delete old time
        self.assertEqual(1, self.cmd("BBF.CLRTIME", 'a', 0))

        # test old data is not there
        for value in data['a']:
            self.assertEqual(0, self.cmd("BBF.EXISTS", 'a', value))

        # test didn't affect b
        for value in data['b']:
            self.assertEqual(1, self.cmd("BBF.EXISTS", 'b', value))

        # test new data is there
        self.assertEqual(1, self.cmd("BBF.EXISTS", 'a', 'new_time'))

        # assert random values don't exist
        self.assertEqual(0, self.cmd("BBF.EXISTS", 'a', 'e'))
        self.assertEqual(0, self.cmd("BBF.EXISTS", 'b', 'a'))
        self.assertEqual(0, self.cmd("BBF.EXISTS", 'a', 'bcd'))

    def test_time(self):
        data = {
            'a': ['a', 'b', 'd'],
            'b': ['d', 'e', 'f']
        }

        # insert a
        for value in data['a']:
            self.assertEqual(1, self.cmd("BBF.ADD", 'a', value))

        self.assertEqual(1, self.cmd("BBF.SETTIME", 'a', '8'))

        for value in data['a']:
            self.assertEqual(1, self.cmd("BBF.ADD", 'a', value + "new"))

        self.assertEqual(1, self.cmd("BBF.CLRTIME", 'a', '0'))

        # assert old value was cleared
        for value in data['a']:
            self.assertEqual(0, self.cmd("BBF.EXISTS", 'a', value))

        # assert new value is inserted
        for value in data['a']:
            self.assertEqual(1, self.cmd("BBF.EXISTS", 'a', value + "new"))


if __name__ == "__main__":
    import unittest
    unittest.main()
