import time
import logging

class CopyStatus(object):

    def __init__(self,source,destination,start_time,end_time,file_size):
        self.source = source
        self.destination = destination
        self.start_time = start_time
        self.end_time = end_time
        self.file_size = file_size
        
    def __eq__(self, other): 
        # TODO: Comparison (only used for testing) does not work for the CopyFailure sub-class because
        # exceptions are not comparable
        print 'CopyStatus.__eq__ called'
        for element in self.__dict__:
            print '__eq__ %s: %s' % (element, self.__dict__[element] == other.__dict__[element])
        return self.__dict__ == other.__dict__
    
    def __str__(self):
        return str(self.__dict__)
    
    def _log_details(self):
        fmt = '%d %b %Y %H:%M:%S'
        return 'Source: %s Destination: %s Start: %s End: %s Duration: %s Size: %s ' % (
                    self.source, 
                    self.destination, 
                    time.strftime(fmt,self.start_time), 
                    time.strftime(fmt,self.end_time),
                    time.mktime(self.end_time) - time.mktime(self.start_time),
                    self.file_size)
    
    def _log_pre(self):
        assert(False) # should be overriden in subclass
        
    def _log_post(self):
        return ''
        
    def _log_class(self):
        assert(False) # should be overriden in subclass
        
    def _log_msg(self):
        return self._log_pre() + self._log_details() + self._log_post() 
    
    def log(self,logger):
        logger.log(self._log_class(),self._log_msg())
        
class CopySuccess(CopyStatus):
    
    def _log_pre(self):
        return 'File copied. '
    
    def _log_class(self):
        return logging.INFO
    
class CopyFailure(CopyStatus):
    
    def __init__(self,source,destination,start_time,end_time,file_size,exception):
        super(CopyFailure,self).__init__(source,destination,start_time,end_time,file_size)
        self.exception = exception

    def _log_pre(self):
        return 'File copy failed. '
    
    def _log_class(self):
        return logging.ERROR
    
    def _log_post(self):
        return 'Exception: ' + str(self.exception)