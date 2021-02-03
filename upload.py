import http.client
import os
from aws_request_signer import AwsRequestSigner, UNSIGNED_PAYLOAD
from urllib.parse import quote
from threading import Thread
from queue import Queue

shutdown = False
abort = False

class FileReader:
    def __init__(self, filePath, blocksize=1048576):
        self.fd = open(filePath, 'rb')
        self.blocksize = blocksize
        self.chunkQ = Queue(2)
        self.bytesRead = 0

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
            self.bytesRead += len(chunk)
            self.chunkQ.put(chunk)

    def read(self, size):
        return self.chunkQ.get()

    def close(self):
        self.runWorker = False
        self.fd.close()

def upload(fileQ):
    global fileCount
    global totalSize
    requestSigner = AwsRequestSigner(AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, 's3')
    conn = http.client.HTTPConnection(host=AWS_S3_HOST, port=AWS_S3_PORT, blocksize=BLOCKSIZE)

    while True:
        if shutdown:
            break
        try:
            f = fileQ.get(True, 5)
        except Exception:
            # There are no more files in the queue to upload, so terminate thread
            conn.close()
            break
        
        if os.name == 'nt':
            k = '/' + f.replace('\\', '/')
        else:
            k = f

        URL = 'http://' + AWS_S3_HOST + ':' + str(AWS_S3_PORT) + '/' + BUCKET + quote(k)

        # The headers we'll provide and want to sign.
        try:
            fileSize = os.stat(f).st_size
        except Exception as err:
            continue
        headers = {"Content-Type": "application/octet-stream", "Content-Length": str(fileSize)}

        # Add the authentication headers.
        headers.update(requestSigner.sign_with_headers("PUT", URL, headers, content_hash=UNSIGNED_PAYLOAD))

        try:
            fh = FileReader(f, BLOCKSIZE)
        except Exception:
            continue
        fh.run()
        try:
            conn.request(method='PUT', url='/' + BUCKET + quote(k), headers=headers, body=fh)
            res = conn.getresponse()
            data = res.read()
        except Exception as err:
            print("s3 connection error:", err)
            conn.close()
            fh.close()
            break

        if res.status < 200 or res.status > 299:
            print('Error connecting to s3:', res.status, data)
            conn.close()
            fh.close()
            break
        
        fileQ.task_done()
        fileCount += 1
        totalSize += fileSize
