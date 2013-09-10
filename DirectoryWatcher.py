'''
Created on Aug 13, 2013

@author: family
'''
import os
import time
import MessageSender
import logging

DIRS_TO_WATCH = {'/Users/family/Dropbox/dev/testdir'}
POLL_INTERVAL = 1

_logger = logging.getLogger(__name__)

class DirectoryWatcher(object):

    def __init__(self, dirlist, new_file_callback):
        '''
        dirlist is a list of directories to watch
        new_file_callback is a callback function which is called with the full path of each
        new file that is detected in the watched directories, e.g.
            def file_name_printer(filename):
                print 'Callback filename: ', filename

        '''
        self.dirlist = dirlist
        self.files_already_seen = set()
        self.new_file_callback = new_file_callback
        _logger.info('DirectoryWatcher is watching %s', self.dirlist)
        
    def _file_is_writable(self,path):
#        print 'checking whether %s is writable' % (path)
#        f = open(path,'a')
#        f.write('sausage')
#        f.close()
        return True
        
    def look(self):
        filelist = []
        for directory in self.dirlist:
            filelist.extend(map(lambda d: os.path.join(directory,d),os.listdir(directory)))
        new_files = set(filelist) - self.files_already_seen
        new_files = filter(self._file_is_writable, new_files)
        self.files_already_seen = set(filelist)
        _logger.info('New files: %s', new_files)
        map(self.new_file_callback,new_files)

def file_name_printer(filename):
    print 'Callback filename: ', filename
                
def main():
    ms = MessageSender.MessageSender()
    watcher = DirectoryWatcher(DIRS_TO_WATCH, ms.send)
    while(True):
        watcher.look()
        time.sleep(POLL_INTERVAL)
        
if __name__ == '__main__':
    main()