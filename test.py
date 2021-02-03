from Queue import Queue
from scandir import scandir
from os.path import join
from threading import Thread
import time
import s3
 
#s3 = boto3.client(
#    's3',
#    endpoint_url='http://localhost:9000',
#    region_name='us-east-1',
#    aws_access_key_id='LKKB31TCA0VVORCCZI6Y',
#    aws_secret_access_key='5PJ7uKvsDbPFsmI2mYSFWZBtctJ726yV6MGEHCxY'
#)
  
fileQ = Queue(10000)

def scanDir(path):
    try:
        for file in scandir(path):
            fullPath = join(path, file.name)
            if file.is_file():
                fileQ.put(fullPath, True)
            elif file.is_dir():
                scanDir(fullPath)
    except:
        pass

def upload():
    global fileCount
    conn = s3.S3Connection({
        'aws_key_id': 'LKKB31TCA0VVORCCZI6Y',
        'secret_access_key': '5PJ7uKvsDbPFsmI2mYSFWZBtctJ726yV6MGEHCxY',
        'endpoint': '192.168.157.1:9000',
        'region': 'us-east-1',
        'default_bucket': 'test'
    })
    storage = s3.Storage(conn)

    while True:
        try:
            f = fileQ.get(True, 5)
            storage.write(f, f)
            fileCount = fileCount + 1
        except:
            break

t1 = Thread(target=upload)
t1.start()
t2 = Thread(target=upload)
t2.start()

startTime = time.time()
fileCount = 0
scanDir('c:\\python27\\scripts')
t1.join()
t2.join()
print fileCount, 'files have been uploaded'