from os import scandir
from os.path import join
import threading

class Scanner(threading.Thread):
    def __init__(self, path, fileQ, errorReportQ):
        threading.Thread.__init__(self)
        self.filesScanned = 0
        self.path = path
        self.fileQ = fileQ
        self.errorReportQ = errorReportQ
        self.shutdown = threading.Event()

    def run(self):
        def doDir(path):
            if self.shutdown.isSet():
                return
            try:
                for file in scandir(path):
                    if self.shutdown.isSet():
                        return
                    fullPath = join(path, file.name)
                    if file.is_file():
                        self.fileQ.put(fullPath)
                        self.filesScanned += 1
                    elif file.is_dir():
                        doDir(fullPath)
            except Exception as err:
                self.errorReportQ.put({
                    'filePath': path,
                    'key': None,
                    'operation': 'scandir',
                    'result': 'failed',
                    'error': str(err)
                })
        doDir(self.path)
