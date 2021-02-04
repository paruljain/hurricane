from os import scandir
from os.path import join
from threading import Thread

__instances = []

def run(path, fileQ, errorReportQ):
    if not fileQ or not errorReportQ:
        raise SyntaxError('FileQ and errorReportQ have not been specified')
    if not path:
        raise SyntaxError('path has not been specified')
    s = __Scanner(path, fileQ, errorReportQ)
    __instances.append(s)
    s.run()

def shutdown():
    for instance in __instances:
        instance.shutdown = True

def getScannedCount():
    count = 0
    for instance in __instances:
        count += instance.filesScanned
    return count

class __Scanner:
    def __init__(self, path, fileQ, errorReportQ):
        self.filesScanned = 0
        self.path = path
        self.shutdown = False
        self.fileQ = fileQ
        self.errorReportQ = errorReportQ

    def run(self):
        t = Thread(target=self.scanDir, args=(self.path,))
        t.start()

    def scanDir(self, path):
        if self.shutdown:
            return
        try:
            for file in scandir(path):
                if self.shutdown:
                    return
                fullPath = join(path, file.name)
                if file.is_file():
                    self.fileQ.put(fullPath, True)
                    self.filesScanned += 1
                elif file.is_dir():
                    self.scanDir(fullPath)
        except Exception as err:
            self.errorReportQ.put({
                'filePath': path,
                'key': None,
                'operation': 'scandir',
                'result': 'failed',
                'error': str(err)
            })
