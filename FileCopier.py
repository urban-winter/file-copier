'''
Created on Aug 17, 2013

@author: family
'''
import shutil
import logging
import fnmatch
import os
import time
from datetime import date
import multiprocessing
import glob
from FileCopyHistoryStore import FileCopyHistoryStore
from CopyStatus import CopySuccess, CopyFailure

_logger = logging.getLogger(__name__)

SECONDS = 1
HOURS = 60 * 60 * SECONDS

MAX_AGE_TO_COPY = 12 * HOURS
MIN_AGE_TO_COPY = 60 * SECONDS
HISTORY_FILE_AGE_THRESHOLD = 12 * HOURS

COPY_POOL_SIZE = 10
        
class Destination(object):
    
    def __init__(self,source_spec,source_path,dest_spec,dest_is_history_path):
        self.source_spec = source_spec
        self.source_path = source_path
        self.dest_spec = dest_spec
        self.dest_is_history_path = dest_is_history_path
        self.path = self._path()
        
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
    '''
    Copy a file from src to dst and place a CopyStatus object on the queue when complete.
    
    This function supports copying in a separate process. It cannot be an instance method because
    multiprocessing does not allow instance methods to be run in a separate process.
    '''
    try:
        start_time = time.localtime()
        shutil.copyfile(src, dst)
        end_time = time.localtime()
        queue.put(CopySuccess(src,dst,start_time,end_time,os.path.getsize(src)))
    except Exception as e:
        end_time = time.localtime()
        queue.put(CopyFailure(src,dst,start_time,end_time,os.path.getsize(src),e))

class FileCopier(object):

    def __init__(self,file_copy_spec,file_copied_callback = None):
        '''
        file_copy_spec is dictionary indexed by full source path containing list of full 
        destination paths
        
        file_copied_callback is called for each attempted file copy and passed a CopyStatus
        '''
        self.file_copy_spec = file_copy_spec
        self.file_copied_callback = file_copied_callback
        self.pool = multiprocessing.Pool(processes=COPY_POOL_SIZE)
        self.copy_processes_active = 0
        mgr = multiprocessing.Manager()
        self.queue = mgr.Queue()        
        self.copied_files = FileCopyHistoryStore()
                
    def _copy_file(self,src,dst):
        if not os.path.exists(dst):
            _logger.debug('Queuing copy from %s to %s',src,dst)
            self.copy_processes_active += 1
            self.pool.apply_async(copy_file_task, (src, dst, self.queue))
#            copy_file_task(src,dst,self.queue)
  
    def _find_copy_spec(self,source_path):
        for source_spec in self.file_copy_spec:
            if fnmatch.fnmatch(source_path, source_spec):
                return self.file_copy_spec[source_spec], source_spec
        return None, None
    
    def _process_one_destination(self,dest_path,source_path,source_spec,dest_is_history_path):
        if dest_path is not None:
            _logger.debug('Processing destination path: %s for source file: %s', dest_path, source_path)
            destination = Destination(source_spec,source_path,dest_path,dest_is_history_path)
            self._copy_file(source_path,destination.path)
        
    def _file_has_already_been_copied(self,path):
        return self.copied_files.contains(path,os.path.getmtime(path))
    
    def _file_is_not_too_old(self,path):
        file_modified_time = os.path.getmtime(path)
        time_now = time.time()
        retval = file_modified_time > (time_now - MAX_AGE_TO_COPY)
#        print '_src_file_is_not_too_old ', retval
        return retval

    def _file_is_not_too_young(self,path):
        file_modified_time = os.path.getmtime(path)
        time_now = time.time()
        retval = file_modified_time < (time_now - MIN_AGE_TO_COPY)
        return retval

    def _record_that_file_has_been_copied(self, path):
        file_ctime = os.path.getmtime(path)
        self.copied_files.add(path,file_ctime)

    def _process_all_destinations(self,source_path,source_spec,copy_spec):
        if not self._file_has_already_been_copied(source_path):
            for idx, dest_path in enumerate(copy_spec):
                dest_is_history_path = idx == 1
                self._process_one_destination(dest_path,source_path,source_spec,dest_is_history_path)
            self._record_that_file_has_been_copied(source_path)
                
    def _check_copy_status(self):
        '''
        Drain the queue of status messages and process each. 
        
        Call the callback function (if specified) for each status message.
        Decrement the count of outstanding copy processes for each status message received.
        Log each status.
        '''
        while not self.queue.empty():
            result = self.queue.get()
            self.copy_processes_active -= 1
            result.log(_logger)
            if self.file_copied_callback is not None:
                self.file_copied_callback(result)
                
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
        _logger.debug('FileCopier.poll() called')
        for destination in self.file_copy_spec:
            matching_files = glob.glob(destination)
            for path in matching_files:
                if self._file_is_not_too_old(path) and self._file_is_not_too_young(path):
                    self._process_all_destinations(path,destination,self.file_copy_spec[destination])
        self._check_copy_status()
        