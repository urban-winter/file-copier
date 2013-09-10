'''
Created on Aug 13, 2013

@author: family
'''
import unittest
from DirectoryWatcher import DirectoryWatcher 
import tempfile
import shutil
import os


class TestDirectoryWatcher(unittest.TestCase):
    
    def filename_callback(self, fname):
        self.files_seen.add(fname)
    
    def setUp(self):
        self.test_dirs = []
        self.files_seen = set()

    def tearDown(self):
        self.test_dir = None
        for a_dir in self.test_dirs:
            shutil.rmtree(a_dir)
        self.test_dirs = []

    def test_empty_dir(self):
        self.test_dirs = [tempfile.mkdtemp()]
        watcher = DirectoryWatcher([self.test_dirs[0]], self.filename_callback)
        watcher.look()
        self.assertEqual(len(self.files_seen), 0)
    
    def test_one_file(self):
        self.test_dirs = [tempfile.mkdtemp()]
        filename = tempfile.mkstemp(dir=self.test_dirs[0])[1]
        watcher = DirectoryWatcher({self.test_dirs[0]}, self.filename_callback)
        watcher.look()
        self.assertEqual({filename}, self.files_seen)

    def test_add_second_file_to_one_dir(self):
        self.test_dirs = [tempfile.mkdtemp()]
        filename = tempfile.mkstemp(dir=self.test_dirs[0])[1]
        expected = {filename}
        watcher = DirectoryWatcher({self.test_dirs[0]}, self.filename_callback)
        watcher.look()
        self.assertEqual(len(self.files_seen), 1)
        filename = tempfile.mkstemp(dir=self.test_dirs[0])[1]
        expected.add(filename)
        watcher.look()
        self.assertEqual(expected, self.files_seen)
    
    def test_two_dirs_with_one_file(self):
        self.test_dirs = [tempfile.mkdtemp(),tempfile.mkdtemp()]
        filename1 = tempfile.mkstemp(dir=self.test_dirs[0])[1]
        filename2 = tempfile.mkstemp(dir=self.test_dirs[1])[1]
        expected = {filename1,filename2}
        watcher = DirectoryWatcher(self.test_dirs, self.filename_callback)
        watcher.look()
        self.assertEqual(expected, self.files_seen)        
    
    def test_two_dirs_add_second_file(self):
        self.test_dirs = [tempfile.mkdtemp(),tempfile.mkdtemp()]
        filename1 = tempfile.mkstemp(dir=self.test_dirs[0])[1]
        filename2 = tempfile.mkstemp(dir=self.test_dirs[1])[1]
        watcher = DirectoryWatcher(self.test_dirs, self.filename_callback)
        watcher.look()
        self.assertEqual(len(self.files_seen), 2)
        filename3 = tempfile.mkstemp(dir=self.test_dirs[0])[1]
        filename4 = tempfile.mkstemp(dir=self.test_dirs[1])[1]
        expected = {filename1,filename2,filename3,filename4}
        watcher.look()
        self.assertEqual(expected, self.files_seen)        
    
    def test_delete_file_from_one_dir(self):
        self.test_dirs = [tempfile.mkdtemp()]
        filename = tempfile.mkstemp(dir=self.test_dirs[0])[1]
        watcher = DirectoryWatcher({self.test_dirs[0]}, self.filename_callback)
        watcher.look()
        self.assertEqual({filename}, self.files_seen)
        os.remove(filename)
        watcher.look()
        self.assertEqual({filename}, self.files_seen)
        
if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()