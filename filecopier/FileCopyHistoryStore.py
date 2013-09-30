import pickle
import os

class FileCopyHistoryStore(object):
    
    def __init__(self,persistence_path=None):
        self.persistence_path = persistence_path
        self._unpickle()
        
    def _unpickle(self):
        if self.persistence_path is not None and os.path.exists(self.persistence_path):
            with open(self.persistence_path,'rb') as f:
                try:
                    self.files_seen = pickle.load(f)
                except EOFError:
                    self.files_seen = set()
        else:
            self.files_seen = set()
            
    def _pickle(self):
        if self.persistence_path is not None:
            with open(self.persistence_path,'wb') as f:
                pickle.dump(self.files_seen, f)
        
    def add(self,path,ctime):
        self.files_seen.add((path,ctime))
        self._pickle()
        
    def contains(self,path,ctime):
        return (path,ctime) in self.files_seen
