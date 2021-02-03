from aws_request_signer import AwsRequestSigner, UNSIGNED_PAYLOAD
import requests
import queue
from os import scandir
import os
from urllib.parse import quote
from os.path import join
import time
from threading import Thread

MAX_THREADS = 100

AWS_REGION = 'us-east-1'
AWS_ACCESS_KEY_ID = 'RNLJDN0K03B7HHZPZTK3'
AWS_SECRET_ACCESS_KEY = 'joayaC6Gw5JfzHDoYTFWcQH0xJT94Bpb5Eroood2'
AWS_S3_END_POINT = 'http://192.168.68.113:9000'
BUCKET = 'test'

FOLDER_TO_COPY = 'c:\\users\\parul'

sess = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_maxsize=MAX_THREADS, pool_block=True)
sess.mount('http://', adapter)

fileQ = queue.Queue(1000)
fileCount = 0
totalSize = 0
# Put files to copy to s3 on the queue
# path is the root path from where to recursively list files to copy
def scanDir(path):
    try:
        for file in scandir(path):
            fullPath = join(path, file.name)
            if file.is_file():
                fileQ.put(fullPath, True)
            elif file.is_dir():
                scanDir(fullPath)
    except:
        pass # Ignore folder access permission errors

def upload():
    global fileCount
    global totalSize
    while True:
        try:
            f = fileQ.get(True, 5)
        except:
            break
        k = f.replace('c:\\', '').replace('\\', '/')
        requestSigner = AwsRequestSigner(
            AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, 's3'
        )

        URL = AWS_S3_END_POINT + '/' + BUCKET + quote(k)

        # The headers we'll provide and want to sign.
        try:
            fileSize = os.stat(f).st_size
            totalSize = totalSize + fileSize
            headers = {"Content-Type": "application/octet-stream", "Content-Length": str(fileSize)}

            # Add the authentication headers.
            headers.update(
                requestSigner.sign_with_headers("PUT", URL, headers, content_hash=UNSIGNED_PAYLOAD)
            )
            with open(f, 'rb') as fh:
                sess.put(URL, headers=headers, data=fh)
            
            fileQ.task_done()
            fileCount = fileCount + 1
        except:
            pass        

startTime = time.time()

# Reports status of the copy job
def monitor():
    global fileCount
    global totalSize
    global startTime
    while True:
        print(fileQ.qsize(), 'files in queue;', fileCount, 'files uploaded;', round(totalSize/1024/1024, 2), 'MB uploaded')
        time.sleep(5)

copyOps = []
for i in range(MAX_THREADS):
    t = Thread(target=upload)
    copyOps.append(t)
    t.start()


print('Starting ...')

# Start the monitoring thread
# Because this thread is started as daemon the main thread will not wait for it
# to complete
Thread(target=monitor, daemon=True).start()

scanDir(FOLDER_TO_COPY)
print('Scanning task is now done. Waiting for copy jobs to finish')

# Wait for all copy jobs to finish
for copyOp in copyOps:
    copyOp.join()

timeTakenSeconds = round(time.time() - startTime, 2)
print(fileCount, 'files were uploaded in', timeTakenSeconds, 'seconds')