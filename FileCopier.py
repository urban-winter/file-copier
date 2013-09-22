'''
Created on Aug 17, 2013

@author: family
'''
import shutil
import logging
import fnmatch
import os
import time
from datetime import date, timedelta
import multiprocessing
import glob


_logger = logging.getLogger(__name__)

SECONDS = 1
HOURS = 60 * 60 * SECONDS

MAX_AGE_TO_COPY = 12 * HOURS
MIN_AGE_TO_COPY = 60 * SECONDS
HISTORY_FILE_AGE_THRESHOLD = 12 * HOURS

MSG_SUCCESS = 'success'
MSG_FAILURE = 'failure'

COPY_POOL_SIZE = 10

class CopyRules(object):
    def __init__(self,src_path,dst_path,is_history_path=False):
        self.src_path = src_path
        self.dst_path = dst_path
        self.is_history_path = is_history_path
        
    def file_should_be_copied(self):
        print 'file_should_be_copied called'
        return (
                self._dest_file_does_not_exist(self.dst_path) and 
                self._src_file_is_not_too_old(self.src_path) and 
                self._src_file_is_not_too_young(self.src_path) and
                self._history_file_does_not_exist()
                )
        
    def _dest_file_does_not_exist(self,path):
        retval = not os.path.exists(path)
        print '_dest_file_does_not_exist ', retval
        return retval
    
    def _src_file_is_not_too_old(self,path):
        file_modified_time = os.path.getmtime(path)
        time_now = time.time()
        retval = file_modified_time > (time_now - MAX_AGE_TO_COPY)
        print '_src_file_is_not_too_old ', retval
        return retval
    
    def _src_file_is_not_too_young(self,path):
        file_modified_time = os.path.getmtime(path)
        time_now = time.time()
        retval = file_modified_time < (time_now - MIN_AGE_TO_COPY)
        print '_src_file_is_not_too_young', file_modified_time < (time_now - MIN_AGE_TO_COPY)
        return retval
    
    def _path_in_yesterdays_history_directory(self,path_in_todays_directory):
        yesterday = date.today() - timedelta(1)
        hist_dir_name_yesterday = yesterday.strftime('%Y%m%d')
        history_path,filename = os.path.split(self.dst_path)
        history_base,dummy = os.path.split(history_path)
        return os.path.join(history_base,hist_dir_name_yesterday,filename)
    
    def _history_file_does_not_exist(self):
        if not self.is_history_path:
            retval = True
        else:
            path_in_yesterdays_history = self._path_in_yesterdays_history_directory(self.dst_path)
            if not os.path.exists(path_in_yesterdays_history):
                retval = True #History file does not exist in yesterday's directory
            else:
                file_modified_time = os.path.getmtime(path_in_yesterdays_history)
                time_now = time.time()
                retval = file_modified_time < (time_now - HISTORY_FILE_AGE_THRESHOLD)
        print '_history_file_does_not_exist ',retval # History file effectively doesn't exist if it is older than threshold
        return retval 

class Destination(object):
    
    def __init__(self,source_spec,source_path,dest_spec,dest_is_history_path):
        self.source_spec = source_spec
        self.source_path = source_path
        self.dest_spec = dest_spec
        self.dest_is_history_path = dest_is_history_path
        self.path = self._path()
        
    def file_should_be_copied(self):
#        print 'self.destspec: %s,self.path: %s' % (self.dest_spec,self.path)
        rules = CopyRules(self.source_path,self.path,self.dest_is_history_path)
        return rules.file_should_be_copied()

    def _path(self):
        path = self._path_after_wildcard_substitution()
        if self.dest_is_history_path:
            path = self._make_path_in_dated_directory(path)
        return path
    
    def _path_after_wildcard_substitution(self):
        '''
        Return the destination path for the copied file
        
        Characters that match wildcards in the source spec are substituted for wildcards in the dest
        spec. 
        E.g. source spec: a*.txt
        actual file: a123.txt
        dest spec: b*.txt
        returned value: b123.txt
        
        Special rules apply if the destination path is for the history location. In this case the
        characters that match the wildcards are appended to the end of the filename, but only if
        the dest spec contains no wildcard.
        '''
        # If dest spec has no wildcards and this isn't the history location 
        # then no substitution is required so just return the spec
        dest_contains_wildcard = (self.dest_spec.count('*') != 0)
        if not dest_contains_wildcard and not self.dest_is_history_path:
            dest = self.dest_spec
        else:
            # Get the characters making up the first * in the source spec
            star_chars = self._characters_matching_wildcard(self.source_spec, self.source_path)
            if dest_contains_wildcard:
                dest = self.dest_spec.replace('*',star_chars)
            else:
                dest = self.dest_spec + star_chars
        return dest
        
    def _characters_matching_wildcard(self, source_spec, actual_filename):
        star_pos = source_spec.find('*')
        chars_after_star = len(source_spec) - star_pos - 1
        star_chars = actual_filename[star_pos:len(actual_filename) - chars_after_star]
        return star_chars

    def _make_path_in_dated_directory(self,path):
        '''
        Create a dated directory if one doesn't already exist and return the path modified to point to it
        
        e.g. if path is /a_directory/a_file 
        and date is 2013-10-29 
        then return value is /a_directory/20131029/a_file
        '''
        (dirname,fname) = os.path.split(path)
        dated_subdir = date.today().strftime('%Y%m%d')
        dated_path = os.path.join(dirname,dated_subdir)
        if not os.path.exists(dated_path):
            os.mkdir(dated_path)
        return os.path.join(dirname,dated_subdir,fname)

def copy_file_task(src,dst,queue):
    try:
        shutil.copyfile(src, dst)
        queue.put([MSG_SUCCESS,src,dst])
    except Exception as e:
        queue.put([MSG_FAILURE,e])

class FileCopier(object):
    '''
    TODO:
    - if dest file already exists then delete before copying
    '''

    def __init__(self,file_copy_spec,file_copied_callback = None):
        '''
        file_copy_spec is dictionary indexed by full source path containing list of full 
        destination paths
        '''
        self.file_copy_spec = file_copy_spec
        self.file_copied_callback = file_copied_callback
        self.pool = multiprocessing.Pool(processes=COPY_POOL_SIZE)
        self.copy_processes_active = 0
        mgr = multiprocessing.Manager()
        self.queue = mgr.Queue()        
        self.copied_files = set()
                
    def _copy_file(self,src,dst):
        _logger.info('Copying from %s to %s',src,dst)
#        shutil.copyfile(src,dst)           
#        self.pool.apply_async(shutil.copyfile, (src,dst))
        self.copy_processes_active += 1
        self.pool.apply_async(copy_file_task, (src, dst, self.queue))
#        self._copy_file_task(src, dst)  
  
    def _wildcard_match(self,spec,target):
        '''
        Does the target match the spec?
        
        '''
        return fnmatch.fnmatch(target, spec)
    
    def _find_copy_spec(self,source_path):
        for source_spec in self.file_copy_spec:
            if self._wildcard_match(source_spec,source_path):
                return self.file_copy_spec[source_spec], source_spec
        return None, None
    
    def _process_one_destination(self,dest_path,source_path,source_spec,dest_is_history_path):
        if dest_path is not None:
            _logger.debug('Processing destination path: %s', dest_path)
            destination = Destination(source_spec,source_path,dest_path,dest_is_history_path)
            if destination.file_should_be_copied():
                _logger.debug('Copying %s to %s',source_path,destination.path)
                self._copy_file(source_path,destination.path)
                self.copied_files.add(source_path)
        
    def copy(self,source_path):
        '''
        Copy the file at source_path to all specified destinations
        
        Destinations are specified when a FileCopier instance is created.
        Destinations are identified by a wildcard search on the spec keys
        '''
        copy_spec, source_spec = self._find_copy_spec(source_path)
        if copy_spec is None:
            return
        _logger.debug('Spec found for %s',source_path)
        self._copy(source_path, source_spec, copy_spec)
        
    def _file_has_already_been_copied(self,path):
        return path in self.copied_files
            
    def _copy(self,source_path,source_spec,copy_spec):
        if not self._file_has_already_been_copied(source_path):
            for idx, dest_path in enumerate(copy_spec):
                dest_is_history_path = idx == 1
                self._process_one_destination(dest_path,source_path,source_spec,dest_is_history_path)
                
    def _check_copy_status(self):
        while not self.queue.empty():
            result = self.queue.get()
            print 'Result retrieved: ', result
            self.copy_processes_active -= 1
            if result[0] == MSG_SUCCESS and self.file_copied_callback is not None:
                dst = result[2]
                self.file_copied_callback(dst)
                
    def flush(self):
        '''
        Wait for all copy processes to complete
        '''
        while self.copy_processes_active > 0:
            self._check_copy_status()
            time.sleep(0.01)
                
    def poll(self):
        '''
        Check whether there are files to copy and if so copy them
        '''
        for destination in self.file_copy_spec:
            matching_files = glob.glob(destination)
            for path in matching_files:
                self._copy(path,destination,self.file_copy_spec[destination])
        
    def close(self):
        '''
        Wait for any in-progress work to complete. This is needed for unit tests.
        '''
        _logger.debug('FileCopier.close()')
        self.pool.close()
        self.pool.join()