from aws_request_signer import AwsRequestSigner, UNSIGNED_PAYLOAD
import http.client
import queue
from os import scandir
import os
from urllib.parse import quote
from os.path import join
import time
from threading import Thread

MAX_THREADS = 10
BLOCKSIZE = 1048576

AWS_REGION = 'us-east-1'
AWS_ACCESS_KEY_ID = 'RNLJDN0K03B7HHZPZTK3'
AWS_SECRET_ACCESS_KEY = 'joayaC6Gw5JfzHDoYTFWcQH0xJT94Bpb5Eroood2'
AWS_S3_HOST = '192.168.68.113'
AWS_S3_PORT = 9000
BUCKET = 'test'
FOLDER = 'c:\\python'

fileQ = queue.Queue(1000)
fileCount = 0
totalSize = 0
filesScanned = 0
shutdown = False
fileUploadResultQ = queue.Queue()
s3ConnectionErrorLogQ = queue.Queue()

class FileReader:
    def __init__(self, filePath, blocksize=1048576):
        self.fd = open(filePath, 'rb')
        self.blocksize = blocksize
        self.chunkQ = queue.Queue(2)

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

    def read(self, size):
        return self.chunkQ.get()

    def close(self):
        self.runWorker = False
        self.fd.close()

# Put files to copy to s3 on the queue
# path is the root path from where to recursively list files to copy
def scanDir(path):
    global filesScanned
    if shutdown:
        return
    try:
        for file in scandir(path):
            if shutdown:
                return
            fullPath = join(path, file.name)
            if file.is_file():
                fileQ.put(fullPath, True)
                filesScanned += 1
            elif file.is_dir():
                scanDir(fullPath)
    except:
        pass # Ignore folder access permission errors

def upload():
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

startTime = time.time()

# Reports status of the copy job
def monitor():
    while runMonitor:
        print(filesScanned, 'files scanned;', fileCount, 'files uploaded;', round(totalSize/1024/1024, 2), 'MB uploaded')
        time.sleep(5)

copyOps = []
for _ in range(MAX_THREADS):
    t = Thread(target=upload)
    copyOps.append(t)
    t.start()

print('Starting ...')

# Start the monitoring thread
runMonitor = True
Thread(target=monitor).start()

scanDir(FOLDER)

# Wait for all copy jobs to finish
for copyOp in copyOps:
    copyOp.join()

runMonitor = False

timeTakenSeconds = round(time.time() - startTime, 2)
print(filesScanned, 'files scanned;', fileCount, 'files uploaded;', round(totalSize/1024/1024, 2), 'MB uploaded;', timeTakenSeconds, 'seconds')