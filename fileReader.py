from threading import Thread
from queue import Queue

class FileReader:
    def __init__(self, filePath, blocksize=1048576):
        self.fd = open(filePath, 'rb')
        self.blocksize = blocksize
        self.chunkQ = Queue(2)
        self.bytesSent = 0

    def run(self):
        self.runWorker = True
        Thread(target=self.getFileChunk).start()

    def getFileChunk(self):
        while self.runWorker:
            chunk = self.fd.read(self.blocksize)
            if not chunk:
                self.fd.close()
                self.chunkQ.put('')
                break
            self.chunkQ.put(chunk)

    def read(self, _):
        data = self.chunkQ.get()
        self.chunkQ.task_done()
        self.bytesSent += len(data)
        return data

    def close(self):
        self.runWorker = False
        self.fd.close()
