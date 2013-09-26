'''
Created on Aug 17, 2013

@author: family
'''
import unittest
import tempfile
import shutil
import os
from FileCopier import FileCopier, CopySuccess, CopyFailure, MIN_AGE_TO_COPY, Destination, MAX_AGE_TO_COPY
import logging
from datetime import date

from mock import patch, call
import time
import stat

class TestCopy(unittest.TestCase):
    
    TEST_FILE_NAME = 'sausage.txt'

    def setUp(self):
        self.source_dir = tempfile.mkdtemp()
        self.dest_dir = tempfile.mkdtemp()
        self.dest_dir2 = tempfile.mkdtemp()

    def tearDown(self):
        for a_dir in [self.source_dir,self.dest_dir,self.dest_dir2]:
            shutil.rmtree(a_dir)
            
    def _make_aged_empty_file(self,path,file_age):
        open(path, 'a').flush()
        os.utime(path,(file_age,file_age))
            
    def _make_empty_file(self,path):
        # File modification time must be in the past otherwise the file will be
        # skipped as too young
        file_age = time.time() - MIN_AGE_TO_COPY
        self._make_aged_empty_file(path, file_age)

    def test_single_source_single_destination_no_match(self):
        spec = {os.path.join(self.source_dir,self.TEST_FILE_NAME):[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}

        copier = FileCopier(spec)
        copier.poll()
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 0)

    def test_single_source_single_destination(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 1)
        self.assertEqual(os.listdir(self.dest_dir)[0], self.TEST_FILE_NAME)
        
    def test_multiple_file_multiple_destinations(self):
        src_path1 = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        src_path2 = os.path.join(self.source_dir,'chips.txt')
        dst_path11 = os.path.join(self.dest_dir,self.TEST_FILE_NAME)
        dst_path12 = os.path.join(self.dest_dir,'sausages_and_chips.dat')
        dst_path21 = os.path.join(self.dest_dir2,'nothing_to_do_with_sausages.dat')
        dst_path22 = os.path.join(self.dest_dir2,'chips_and_gravy.txt')
        spec = {src_path1:[dst_path11,None,dst_path12],
                src_path2:[dst_path21,None,dst_path22]}
        self._make_empty_file(src_path1)
        self._make_empty_file(src_path2)
        
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()

        self.assertEqual(set(os.listdir(self.dest_dir)), {self.TEST_FILE_NAME,'sausages_and_chips.dat'})
        self.assertEqual(set(os.listdir(self.dest_dir2)), {'nothing_to_do_with_sausages.dat','chips_and_gravy.txt'})
        
    def test_wildcard_no_match(self):
        src_path1 = os.path.join(self.source_dir,'sausa*.txt')
        dst_path1 = os.path.join(self.dest_dir,'sausa*.txt')
        spec = {src_path1:[dst_path1]}
        self._make_empty_file(os.path.join(self.source_dir,'chips.txt'))
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()
        self.assertEqual(set(os.listdir(self.dest_dir)), set([]))
        
    def test_wildcard_match_with_no_wildcard_in_dest_path(self):
        src_path1 = os.path.join(self.source_dir,'sausa*.txt')
        dst_path1 = os.path.join(self.dest_dir,'chips.txt')
        spec = {src_path1:[dst_path1]}
        self._make_empty_file(os.path.join(self.source_dir,'sausage.txt'))
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()
        self.assertEqual(set(os.listdir(self.dest_dir)), set(['chips.txt']))
        
    def _mock_callback(self,status):
        self.mock_callback_called_with_status = status

    def _copy_status_equal(self,this,that):
        return (
                this.destination == that.destination and
                this.source == that.source and type(this) == CopySuccess or
                (type(this.exception) == type(that.exception) and
                this.exception.args == that.exception.args)
                )
    
    def test_file_copy_complete_callback(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier(spec,self._mock_callback)
        copier.poll()
        copier.flush()
        
#        self.assertEqual(self.mock_callback_called_with_status, CopySuccess(src_path,os.path.join(self.dest_dir,self.TEST_FILE_NAME),None,None))
        self.assertTrue(self._copy_status_equal(
                                                self.mock_callback_called_with_status, 
                                                CopySuccess(src_path,os.path.join(self.dest_dir,self.TEST_FILE_NAME),None,None,None)))

    def test_file_copy_complete_callback_not_called_if_file_not_copied(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_aged_empty_file(src_path,MAX_AGE_TO_COPY+1) # too old to copy
        
        copier = FileCopier(spec,self._mock_callback)
        copier.poll()
        copier.flush()

        self.assertFalse(hasattr(self, 'mock_callback_called_with_status'))
        
    @patch('shutil.copyfile')
    def test_file_copy_complete_callback_called_if_exception_raised(self,*args):
        e = Exception('anything')
        shutil.copyfile.side_effect = e
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier(spec,self._mock_callback)
        copier.poll()
        copier.flush()
        
        self.assertTrue(self._copy_status_equal(self.mock_callback_called_with_status, 
                                                CopyFailure(src_path,os.path.join(self.dest_dir,self.TEST_FILE_NAME),None,None,None,e)))
        
    def test_file_copy_complete_callback_called_if_destination_is_read_only(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        os.chmod(self.dest_dir,stat.S_IREAD)
        self._make_empty_file(src_path)
        
        copier = FileCopier(spec,self._mock_callback)
        copier.poll()
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 0)
        self.assertTrue(self._copy_status_equal(self.mock_callback_called_with_status, 
                                                 CopyFailure(src_path,os.path.join(
                                                                                   self.dest_dir,
                                                                                   self.TEST_FILE_NAME),
                                                             None,
                                                             None,
                                                             None,
                                                             IOError(13, 'Permission denied'))))
        
    def history_dir_name(self):
        return date.today().strftime('%Y%m%d')

    def test_that_history_processing_applies_only_to_second_destination(self):
        src_path1 = os.path.join(self.source_dir,'*.txt')
        dst_path11 = os.path.join(self.dest_dir,'a.txt')
        dst_path12 = os.path.join(self.dest_dir,'b.txt')

        spec = {src_path1:[dst_path11,dst_path12],}
        self._make_empty_file(os.path.join(self.source_dir,'sausage.txt'))
        
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()
        
        expected_history_filename = 'b.txtsausage'

        self.assertEqual(set(os.listdir(self.dest_dir)), {'a.txt',self.history_dir_name()})
        self.assertEqual(os.listdir(os.path.join(self.dest_dir,self.history_dir_name())),[expected_history_filename])
        
    def test_dated_history_dir_exists(self):
        src_path = os.path.join(self.source_dir,'*.txt')
        dst_path = os.path.join(self.dest_dir,'b.txt')

        spec = {src_path:[None,dst_path],}
        self._make_empty_file(os.path.join(self.source_dir,'sausage.txt'))
        hist_dir = self.history_dir_name()
        os.mkdir(os.path.join(self.dest_dir,hist_dir))
        
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()

        expected_history_filename = 'b.txtsausage'

        self.assertEqual(os.listdir(self.dest_dir), [hist_dir])
        self.assertEqual(os.listdir(os.path.join(self.dest_dir,hist_dir)),[expected_history_filename])
        
    def test_file_copied_only_once(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 1)
        self.assertEqual(os.listdir(self.dest_dir)[0], self.TEST_FILE_NAME)   
        
        os.remove(os.path.join(self.dest_dir,self.TEST_FILE_NAME))     
        copier.poll()
        copier.flush()
        self.assertEqual(len(os.listdir(self.dest_dir)), 0)
        
    def test_file_copied_when_new_version_arrives(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_aged_empty_file(src_path, time.time() - MIN_AGE_TO_COPY - 1)
        
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 1)
        self.assertEqual(os.listdir(self.dest_dir)[0], self.TEST_FILE_NAME)   
        
        os.remove(os.path.join(self.dest_dir,self.TEST_FILE_NAME))
        os.remove(src_path)
        
        self._make_aged_empty_file(src_path, time.time() - MIN_AGE_TO_COPY)
     
        copier.poll()
        copier.flush()
        self.assertEqual(len(os.listdir(self.dest_dir)), 1)
        self.assertEqual(os.listdir(self.dest_dir)[0], self.TEST_FILE_NAME) 
        
    def test_file_not_copied_if_it_is_too_old(self,*args):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_aged_empty_file(src_path, time.time() - MAX_AGE_TO_COPY - 1)
        
        copier = FileCopier(spec)
        copier.poll()
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 0)
  
class TestDestinationPathDerivation(unittest.TestCase):
    
    def _test_one_example(self,source,target,actual_file,expected_target,is_history_location):  
        dest = Destination(source_spec=source,
                                      dest_spec = target,
                                      source_path = actual_file,
                                      dest_is_history_path = is_history_location)    
        self.assertEqual(
                         dest._path_after_wildcard_substitution(), 
                         expected_target,
                         '%s != %s for source: %s target: %s actual: %s is_history_location: %s' % (
                            dest._path_after_wildcard_substitution(),expected_target,source,target,actual_file,is_history_location))

    @patch('os.mkdir')
    def test_wildcard_match_wildcard_in_dest_path(self,*args):
        #                Source     Target      Actual File  Expected Target    Is history location
        examples = [(   'a*.txt',   'b.abc',    'a.txt',    'b.abc',            False),
                    (   'a*.txt',   'b*.abc',   'a.txt',    'b.abc',            False),
                    (   'a*.txt',   'b*.abc',   'a123.txt', 'b123.abc',         False),
                    (   '*a.txt',   'b*.abc',   'abca.txt', 'babc.abc',         False),
                    (   'a.txt*',   'b*.abc',   'a.txt1',   'b1.abc',           False),
                    (   '*.txt',    '*.abc',    'a.txt',    'a.abc',            False),
                    (   'a*.txt',   'b.abc',    'a.txt',    'b.abc',            True),
                    (   'a.txt*',   'b*.abc',   'a.txt1',   'b1.abc',           True),
                    (   'a*.txt',   'b.abc',    'a123.txt', 'b.abc123',         True),
                    ]
        for example in examples:
            self._test_one_example(*example)
            


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    unittest.main()