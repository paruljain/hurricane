import scanner
from fileReader import FileReader
from s3FileUpload import S3FileUpload
from queue import Queue
import time
import os
import threading

fileQ = Queue(10000)
errorReportQ = Queue()

scanner.Scanner('c:\\python', fileQ, errorReportQ).start()

def upload():
    fr = FileReader(fileQ, errorReportQ, 1024 * 1024 * 10)
    fr.run()
    s3 = S3FileUpload(
            'us-east-1',
            'RNLJDN0K03B7HHZPZTK3',
            'joayaC6Gw5JfzHDoYTFWcQH0xJT94Bpb5Eroood2',
            'http://192.168.68.113:9000',
            'test'
        )

    while True:
        chunk = fr.chunkQ.get()
        if not chunk:
            break
        [data, filePath, fileSize] = chunk
        # print(filePath)
        key = filePath
        if os.name == 'nt':
            key = '/' + filePath.replace('\\', '/').replace(':', '')
        s3.startFileSend(key, fileSize)
        s3.sendFileData(data)
        while True:
            if not data:
                s3.endFileSend()
                break

            [data, filePath, fileSize] = fr.chunkQ.get()
            s3.sendFileData(data)

startTime = time.time()
threads = []
for _ in range(10):
    t = threading.Thread(target=upload)
    threads.append(t)
    t.start()

for t in threads:
    t.join()

print('All done in', round(time.time() - startTime), 'seconds')