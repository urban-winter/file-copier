import csv
import ntpath
import unittest
import logging

_logger = logging.getLogger(__name__)

class ConfigFile(object):
    
    def __init__(self, config_file_path=None):
        '''
        Field     Contents
        0         Sr.No.    
        1         Full UNC path and filename including wildcards for the source data    
        2         Blank Column    
        3         Primary destination location for imports    
        4         Backup (History) location    
        5         Secondary destination location for imports    
        6         Tertiary destination location for imports    
        7         Quarternary destination location for imports    
        8         Quinary destination location for imports

        self.spec is a dictionary of lists of destination paths indexed by source path
        '''
        _logger.debug('ConfigFile.init(%s)',config_file_path)
        if config_file_path == None:
            return
        with open(config_file_path,'r') as config_file:
            self.spec = {}
            all_dests = set()
            next(config_file) # discard header row
            reader = csv.reader(config_file)
            for row in reader:
                src_path = row[1]
                if src_path in self.spec:
                    _logger.warn('Repeated src: %s', src_path)
                destinations = row[3:8]
                self.spec[src_path] = destinations
                
                for destination in filter(lambda x: (x != ''), destinations):
                    if destination in all_dests:
                        _logger.warn('Repeated destination: %s  for source: %s', destination, src_path)
                    else:
                        all_dests.add(destination)

    def source_directories(self):
        '''Return the list of all directories that contain source files'''
        return map(ntpath.dirname, self.spec.keys())
    
    def file_copier_spec(self):
        return self.spec
    
class TestConfigFile(unittest.TestCase):
    
    def test_source_directories(self):
        cfg_file = ConfigFile()
        cfg_file.spec = {r'\\lonif00101.emea.win.ml.com\ReconGL\EQYGICBL_COM1.dat':[]}
        self.assertEqual(
                         cfg_file.source_directories(), 
                         [r'\\lonif00101.emea.win.ml.com\ReconGL'])
        
    def test_file_copier_spec(self):
        cfg_file = ConfigFile()
        cfg_file.spec = {r'\\lonif00101.emea.win.ml.com\ReconGL\EQYGICBL_COM1.dat':[]}
        self.assertEqual(
                         cfg_file.file_copier_spec(), 
                         cfg_file.spec)
        

