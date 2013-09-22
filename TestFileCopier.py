'''
Created on Aug 17, 2013

@author: family
'''
import unittest
import tempfile
import shutil
import os
import FileCopier
import logging
from datetime import date, timedelta

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
        file_age = time.time() - FileCopier.MIN_AGE_TO_COPY
        self._make_aged_empty_file(path, file_age)

    def test_single_source_single_destination_no_match(self):
        spec = {os.path.join(self.source_dir,self.TEST_FILE_NAME):[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}

        copier = FileCopier.FileCopier(spec)
        copier.copy('chips.txt')
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 0)

    def test_single_source_single_destination(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier.FileCopier(spec)
        copier.copy(src_path)
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
        
        copier = FileCopier.FileCopier(spec)
        copier.copy(src_path1)
        copier.copy(src_path2)
        copier.flush()

        self.assertEqual(set(os.listdir(self.dest_dir)), {self.TEST_FILE_NAME,'sausages_and_chips.dat'})
        self.assertEqual(set(os.listdir(self.dest_dir2)), {'nothing_to_do_with_sausages.dat','chips_and_gravy.txt'})
        
    def test_wildcard_no_match(self):
        src_path1 = os.path.join(self.source_dir,'sausa*.txt')
        dst_path1 = os.path.join(self.dest_dir,'sausa*.txt')
        spec = {src_path1:[dst_path1]}
        self._make_empty_file(os.path.join(self.source_dir,'chips.txt'))
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir,'chips.txt'))
        copier.flush()
        self.assertEqual(set(os.listdir(self.dest_dir)), set([]))
        
    def test_wildcard_match_with_no_wildcard_in_dest_path(self):
        src_path1 = os.path.join(self.source_dir,'sausa*.txt')
        dst_path1 = os.path.join(self.dest_dir,'chips.txt')
        spec = {src_path1:[dst_path1]}
        self._make_empty_file(os.path.join(self.source_dir,'sausage.txt'))
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir,'sausage.txt'))
        copier.flush()
        self.assertEqual(set(os.listdir(self.dest_dir)), set(['chips.txt']))
        
    def _mock_callback(self,path):
        self.mock_callback_called_with_path = path
    
    def test_file_copy_complete_callback(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier.FileCopier(spec,self._mock_callback)
        copier.copy(src_path)
        copier.flush()
        
        self.assertEqual(self.mock_callback_called_with_path, os.path.join(self.dest_dir,self.TEST_FILE_NAME))

    def test_file_copy_complete_callback_not_called_if_file_not_copied(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_aged_empty_file(src_path,FileCopier.MAX_AGE_TO_COPY+1) # too old to copy
        
        copier = FileCopier.FileCopier(spec,self._mock_callback)
        copier.copy(src_path)
        copier.flush()

        self.assertFalse(hasattr(self, 'mock_callback_called_with_path'))

    @patch('shutil.copyfile')
    def test_file_copy_complete_callback_not_called_if_exception_raised(self,*args):
        shutil.copyfile.side_effect = Exception('anything')
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier.FileCopier(spec,self._mock_callback)
        copier.copy(src_path)
        copier.flush()
        
        self.assertFalse(hasattr(self, 'mock_callback_called_with_path'))
        
    def test_file_copy_complete_callback_not_called_if_destination_is_read_only(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        os.chmod(self.dest_dir,stat.S_IREAD)
        self._make_empty_file(src_path)
        
        copier = FileCopier.FileCopier(spec,self._mock_callback)
        copier.copy(src_path)
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 0)
        self.assertFalse(hasattr(self, 'mock_callback_called_with_path'))
        
    def history_dir_name(self):
        return date.today().strftime('%Y%m%d')

        
    def test_that_history_processing_applies_only_to_second_destination(self):
        src_path1 = os.path.join(self.source_dir,'*.txt')
        dst_path11 = os.path.join(self.dest_dir,'a.txt')
        dst_path12 = os.path.join(self.dest_dir,'b.txt')

        spec = {src_path1:[dst_path11,dst_path12],}
        self._make_empty_file(os.path.join(self.source_dir,'sausage.txt'))
        
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir,'sausage.txt'))
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
        
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir,'sausage.txt'))
        copier.flush()

        expected_history_filename = 'b.txtsausage'

        self.assertEqual(os.listdir(self.dest_dir), [hist_dir])
        self.assertEqual(os.listdir(os.path.join(self.dest_dir,hist_dir)),[expected_history_filename])
        
    def _create_history_dir_for_yesterday(self):
        yesterday = date.today() - timedelta(1)
        hist_dir_for_yesterday = yesterday.strftime('%Y%m%d')
        hist_path_for_yesterday = os.path.join(self.dest_dir, hist_dir_for_yesterday)
        os.mkdir(hist_path_for_yesterday)
        return hist_path_for_yesterday, hist_dir_for_yesterday

    def _copy_to_history(self):
        src_path = os.path.join(self.source_dir, '*.txt')
        dst_path = os.path.join(self.dest_dir, 'a.txt')
        spec = {src_path:[None, dst_path]}
        self._make_empty_file(os.path.join(self.source_dir, self.TEST_FILE_NAME))
        hist_dir = date.today().strftime('%Y%m%d')
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir, self.TEST_FILE_NAME))
        copier.flush()
        return hist_dir

    def test_history_file_copied_if_in_yesterdays_directory_but_older_than_threshold(self):
        hist_path_for_yesterday, hist_dir_for_yesterday = self._create_history_dir_for_yesterday()

#        create file in history dir with age older than threshold
        file_age = time.time() - FileCopier.HISTORY_FILE_AGE_THRESHOLD - 1
        self._make_aged_empty_file(os.path.join(hist_path_for_yesterday,'a.txtsausage'), file_age)

        hist_dir = self._copy_to_history()

        self.assertEqual(set(os.listdir(self.dest_dir)), set([hist_dir,hist_dir_for_yesterday]))
        self.assertEqual(os.listdir(os.path.join(self.dest_dir,hist_dir)),['a.txtsausage'])

    def test_history_file_not_copied_if_in_yesterdays_directory_but_newer_than_threshold(self):
        hist_path_for_yesterday, hist_dir_for_yesterday = self._create_history_dir_for_yesterday()

#        create file in history dir with age newer than threshold
        file_age = time.time() - FileCopier.HISTORY_FILE_AGE_THRESHOLD + 2
        self._make_aged_empty_file(os.path.join(hist_path_for_yesterday,'a.txtsausage'), file_age)

        hist_dir = self._copy_to_history()

        self.assertEqual(set(os.listdir(self.dest_dir)), set([hist_dir_for_yesterday,hist_dir]))
        self.assertEqual(os.listdir(os.path.join(self.dest_dir,hist_dir)),[])
        
    def test_file_copied_only_once(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier.FileCopier(spec)
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
#        self._make_empty_file(src_path)
        self._make_aged_empty_file(src_path, FileCopier.time.time() - FileCopier.MIN_AGE_TO_COPY - 1)
        
        copier = FileCopier.FileCopier(spec)
        copier.poll()
        copier.flush()
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 1)
        self.assertEqual(os.listdir(self.dest_dir)[0], self.TEST_FILE_NAME)   
        
        os.remove(os.path.join(self.dest_dir,self.TEST_FILE_NAME))
        os.remove(src_path)
        
        self._make_aged_empty_file(src_path, FileCopier.time.time() - FileCopier.MIN_AGE_TO_COPY)
     
        copier.poll()
        copier.flush()
        self.assertEqual(len(os.listdir(self.dest_dir)), 1)
        self.assertEqual(os.listdir(self.dest_dir)[0], self.TEST_FILE_NAME)   

    @patch('os.path.exists')    
    def test_file_not_copied_if_it_exists(self,mock_exists):
        os.path.exists.return_value = True
        self.assertFalse(FileCopier.CopyRules('dummy', 'a_file.txt').file_should_be_copied())
        os.path.exists.assert_called_once_with('a_file.txt')
        
    @patch('os.path.exists')    
    @patch('os.path.getmtime')
    @patch('time.time')
    def test_file_not_copied_if_it_is_too_old(self,*args):
        os.path.exists.return_value = False
        os.path.getmtime.return_value = time.mktime(time.strptime('2012-02-14 01:00:00','%Y-%m-%d %H:%M:%S'))
        time.time.return_value = time.mktime(time.strptime('2012-02-14 13:00:00','%Y-%m-%d %H:%M:%S'))
        print os.path.getmtime
        self.assertFalse(FileCopier.CopyRules('a_file.txt', 'dummy').file_should_be_copied())
        os.path.getmtime.assert_called_once_with('a_file.txt')
        time.time.assert_called_once_with()

    @patch('os.path.exists')    
    @patch('os.path.getmtime')
    @patch('time.time')
    def test_file_is_copied_if_it_is_not_too_old(self,*args):
        os.path.exists.return_value = False
        os.path.getmtime.return_value = time.mktime(time.strptime('2012-02-14 01:00:00','%Y-%m-%d %H:%M:%S'))
        time.time.return_value = time.mktime(time.strptime('2012-02-14 12:59:59','%Y-%m-%d %H:%M:%S'))
        print os.path.getmtime
        self.assertTrue(FileCopier.CopyRules('a_file.txt', 'dummy').file_should_be_copied())
        os.path.getmtime.assert_has_calls([call('a_file.txt'),call('a_file.txt')])
        time.time.assert_has_calls([call(),call()])
        
    @patch('os.path.exists')    
    @patch('os.path.getmtime')
    @patch('time.time')
    def test_file_not_copied_if_it_is_too_new(self,*args):
        os.path.exists.return_value = False
        os.path.getmtime.return_value = time.mktime(time.strptime('2012-02-14 01:00:00','%Y-%m-%d %H:%M:%S'))
        time.time.return_value = time.mktime(time.strptime('2012-02-14 01:00:59','%Y-%m-%d %H:%M:%S'))
        print os.path.getmtime
        self.assertFalse(FileCopier.CopyRules('a_file.txt', 'dummy').file_should_be_copied())
        os.path.getmtime.assert_has_calls([call('a_file.txt'),call('a_file.txt')])
        time.time.assert_has_calls([call(),call()])
            
    def test_file_copied_when_it_becomes_eligible(self):
        # Create a file that is too new
        # Run the copier
        # Check not copied
        # make file old enough
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        self._make_aged_empty_file(src_path, 0)

        copier = FileCopier.FileCopier(spec)
        copier.poll()
        copier.flush()
        self.assertEqual(len(os.listdir(self.dest_dir)), 0)
        
        file_age = time.time() - FileCopier.MIN_AGE_TO_COPY
        os.utime(src_path,(file_age,file_age))
        copier.poll()
        copier.flush()

        self.assertEqual(len(os.listdir(self.dest_dir)), 1)
        self.assertEqual(os.listdir(self.dest_dir)[0], self.TEST_FILE_NAME)                
        
class TestDestinationPathDerivation(unittest.TestCase):
    
    def _test_one_example(self,source,target,actual_file,expected_target,is_history_location):  
        dest = FileCopier.Destination(source_spec=source,
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