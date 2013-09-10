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
        open(path, 'a').close()
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
        
        self.assertEqual(len(os.listdir(self.dest_dir)), 0)

    def test_single_source_single_destination(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier.FileCopier(spec)
        copier.copy(src_path)
        copier.close()
        
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
        copier.close()

        self.assertEqual(set(os.listdir(self.dest_dir)), {self.TEST_FILE_NAME,'sausages_and_chips.dat'})
        self.assertEqual(set(os.listdir(self.dest_dir2)), {'nothing_to_do_with_sausages.dat','chips_and_gravy.txt'})
        
    def test_wildcard_no_match(self):
        src_path1 = os.path.join(self.source_dir,'sausa*.txt')
        dst_path1 = os.path.join(self.dest_dir,'sausa*.txt')
        spec = {src_path1:[dst_path1]}
        self._make_empty_file(os.path.join(self.source_dir,'chips.txt'))
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir,'chips.txt'))
        self.assertEqual(set(os.listdir(self.dest_dir)), set([]))
        
    def test_wildcard_match_with_no_wildcard_in_dest_path(self):
        src_path1 = os.path.join(self.source_dir,'sausa*.txt')
        dst_path1 = os.path.join(self.dest_dir,'chips.txt')
        spec = {src_path1:[dst_path1]}
        self._make_empty_file(os.path.join(self.source_dir,'sausage.txt'))
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir,'sausage.txt'))
        copier.close()
        self.assertEqual(set(os.listdir(self.dest_dir)), set(['chips.txt']))
        
    def _mock_callback(self,path):
        self.mock_callback_called_with_path = path
    
    def test_file_copy_complete_callback(self):
        src_path = os.path.join(self.source_dir,self.TEST_FILE_NAME)
        spec = {src_path:[os.path.join(self.dest_dir,self.TEST_FILE_NAME)]}
        self._make_empty_file(src_path)
        
        copier = FileCopier.FileCopier(spec,self._mock_callback)
        copier.copy(src_path)
        
        self.assertEqual(self.mock_callback_called_with_path, os.path.join(self.dest_dir,self.TEST_FILE_NAME))
        
    def test_that_history_processing_applies_only_to_second_destination(self):
        src_path1 = os.path.join(self.source_dir,'*.txt')
        dst_path11 = os.path.join(self.dest_dir,'a.txt')
        dst_path12 = os.path.join(self.dest_dir,'b.txt')

        spec = {src_path1:[dst_path11,dst_path12],}
        self._make_empty_file(os.path.join(self.source_dir,'sausage.txt'))
        
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir,'sausage.txt'))
        copier.close()
        
        hist_dir = date.today().strftime('%Y%m%d')

        expected_history_filename = 'b.txtsausage'

        self.assertEqual(set(os.listdir(self.dest_dir)), {'a.txt',hist_dir})
        self.assertEqual(os.listdir(os.path.join(self.dest_dir,hist_dir)),[expected_history_filename])
        
    def test_dated_history_dir_exists(self):
        src_path = os.path.join(self.source_dir,'*.txt')
        dst_path = os.path.join(self.dest_dir,'b.txt')

        spec = {src_path:[None,dst_path],}
        self._make_empty_file(os.path.join(self.source_dir,'sausage.txt'))
        hist_dir = date.today().strftime('%Y%m%d')
        os.mkdir(os.path.join(self.dest_dir,hist_dir))
        
        copier = FileCopier.FileCopier(spec)
        copier.copy(os.path.join(self.source_dir,'sausage.txt'))
        copier.close()

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
        copier.close()
        return hist_dir

    def test_history_file_copied_if_in_yesterdays_directory_but_older_than_threshold(self):
        hist_path_for_yesterday, hist_dir_for_yesterday = self._create_history_dir_for_yesterday()

#        create file in history dir with age older than threshold
        file_age = time.time() - FileCopier.HISTORY_FILE_AGE_THRESHOLD - 1
#        self._make_aged_empty_file(os.path.join(hist_path_for_yesterday,self.TEST_FILE_NAME), file_age)
        self._make_aged_empty_file(os.path.join(hist_path_for_yesterday,'a.txtsausage'), file_age)

        hist_dir = self._copy_to_history()

        self.assertEqual(set(os.listdir(self.dest_dir)), set([hist_dir,hist_dir_for_yesterday]))
#        self.assertEqual(os.listdir(os.path.join(self.dest_dir,hist_dir)),[self.TEST_FILE_NAME])
        self.assertEqual(os.listdir(os.path.join(self.dest_dir,hist_dir)),['a.txtsausage'])

    def test_history_file_not_copied_if_in_yesterdays_directory_but_newer_than_threshold(self):
        hist_path_for_yesterday, hist_dir_for_yesterday = self._create_history_dir_for_yesterday()

#        create file in history dir with age newer than threshold
        file_age = time.time() - FileCopier.HISTORY_FILE_AGE_THRESHOLD + 1
#        self._make_aged_empty_file(os.path.join(hist_path_for_yesterday,self.TEST_FILE_NAME), file_age)
        self._make_aged_empty_file(os.path.join(hist_path_for_yesterday,'a.txtsausage'), file_age)

        hist_dir = self._copy_to_history()

        self.assertEqual(set(os.listdir(self.dest_dir)), set([hist_dir_for_yesterday,hist_dir]))
        self.assertEqual(os.listdir(os.path.join(self.dest_dir,hist_dir)),[])

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
        
#    def test_dated_subdir_created_for_history_location(self):
#        self.fail()
        
#    def test_history_file_not_copied_if_in_yesterdays_directory(self):
#        self.fail()
#
#    def test_history_file_copy_with_when_dated_subdir_exists(self):
#        self.fail()
        
        
#- history file location is path with date appended (e.g. {historypath}\20130826\{filename})
#- the filename used has already had the wildcard substitution
#- if the dated directory does not already exist then it is created
#- if the file does not already exist in the directory then
#-- if the file arrived on a date earlier than the current date then
#--- if the file exists in yesterday's history folder then
#---- if yesterday's backup file is less than gintHistoryFileAgeHours (12 hrs) then
#----- do nothing
#- else
#-- copy it

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