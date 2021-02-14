import threading
import queue
import stat
import os

class FileReader:
    def __init__(self, fileQ, errorReportQ, blockSize=1048576):
        self.blockSize = blockSize
        self.chunkQ = queue.Queue(2)
        self.fileQ = fileQ
        self.errorReportQ = errorReportQ
        self.shutdown = threading.Event()

    def run(self):
        threading.Thread(target=self.readFileChunks, daemon=True).start()

    def readFileChunks(self):
        fd = None
        while not self.shutdown.isSet():
            if not fd:
                try:
                    filePath = self.fileQ.get(True, 1)
                except queue.Empty:
                    break
                stats = os.stat(filePath)
                fd = open(filePath, 'rb')

            chunk = fd.read(self.blockSize)
            self.chunkQ.put((chunk, filePath, stats.st_size))

            if not chunk:
                fd.close()
                fd = None
        
        if fd:
            fd.close()
        self.chunkQ.put(None)
