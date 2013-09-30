from FileCopyHistoryStore import FileCopyHistoryStore
import unittest
import tempfile
import logging

class TestFileCopyHistoryStore(unittest.TestCase):
    
    def test_init_no_path(self):
        fchs = FileCopyHistoryStore()
        self.assertEqual(fchs.files_seen, set())
        
    def test_init_with_path(self):
        (dummy,path) = tempfile.mkstemp()
        fchs = FileCopyHistoryStore(path)
        self.assertEqual(fchs.files_seen, set())
        
    def test_save_and_load(self):
        (dummy,path) = tempfile.mkstemp()
        fchs = FileCopyHistoryStore(path)
        fchs.add('testpath', 1)
        fchs1 = FileCopyHistoryStore(path)
        self.assertTrue(fchs1.contains('testpath', 1))
        
if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    unittest.main()