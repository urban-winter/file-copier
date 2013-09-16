'''
Created on Aug 20, 2013

@author: family
'''
import time
from ConfigFile import ConfigFile
from FileCopier import FileCopier
from DirectoryWatcher import DirectoryWatcher
import logging

SLEEP_TIME = 5
#FILE_COPY_CONFIG_PATH = '/Users/family/Dropbox/dev/python/Rabbit/sfc_config.cfg'
FILE_COPY_CONFIG_PATH = '/Users/family/Dropbox/dev/python/Rabbit/test.cfg'

_logger = logging.getLogger(__name__)

class FileCopierMain(object):
    '''
    Read config from file
    Set up file listener
    enter loop and poll
    '''

    def __init__(self):
        cfg = ConfigFile(FILE_COPY_CONFIG_PATH)
        self.copier = FileCopier(cfg.file_copier_spec())
    
    def poll(self):
        _logger.debug('FileCopierMain.poll()')
        self.copier.poll()                

def main():
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    fc = FileCopierMain()
    while(True):
        fc.poll()
        time.sleep(SLEEP_TIME)
        
if __name__ == '__main__':
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    main()