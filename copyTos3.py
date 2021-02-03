import queue
from os import scandir
from os.path import join
from threading import Thread
import time
 
# Number of concurrent copy operations
MAX_THREADS = 30

# Queue of files to copy to s3
fileQ = queue.Queue(10000)

# Number of files copied
fileCount = 0

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

# Copies a file in queue to s3
def upload():
    global fileCount # strange Python language!

    import boto3
    from botocore.client import Config

    s3 = boto3.resource('s3', 
        endpoint_url='http://192.168.68.113:9000', 
        config=boto3.session.Config(signature_version='s3v4'),
        #aws_access_key_id = 'LKKB31TCA0VVORCCZI6Y',
        #aws_secret_access_key = '5PJ7uKvsDbPFsmI2mYSFWZBtctJ726yV6MGEHCxY',
        aws_access_key_id = 'RNLJDN0K03B7HHZPZTK3',
        aws_secret_access_key = 'joayaC6Gw5JfzHDoYTFWcQH0xJT94Bpb5Eroood2',
        region_name = 'us-east-1'
    )

    bucket = s3.Bucket('test')
    
    while True:
        try:
            # Remove a file from the queue. Block for 1 second then error out
            # Since we are running on Windows f contains is Windows style path c:\\blah\\blah
            f = fileQ.get(True, 1)
            
            # Convert the Windows style path to Unix style
            # This will be our key name in s3
            k = f.replace('c:\\', '').replace('\\', '/')

            # Do not print anything to console except when debugging
            # Printing to console becomes a bottleneck
            # print(f)
            
            # Upload the file
            bucket.upload_file(f, k)

            # Mark the task done in the queue
            # We are not really using this Queue feature but we should do it
            # anyways for completeness
            fileQ.task_done()

            # Is this thread safe? TBD
            fileCount = fileCount + 1
        except:
            break

# Reports status of the copy job
def monitor():
    while True:
        print(fileQ.qsize(), 'files in queue')
        time.sleep(5)

copyOps = []
for i in range(MAX_THREADS):
    t = Thread(target=upload)
    copyOps.append(t)
    t.start()

startTime = time.time()
print('Starting ...')

# Start the monitoring thread
# Because this thread is started as daemon the main thread will not wait for it
# to complete
Thread(target=monitor, daemon=True).start()

scanDir('c:\\python27')
print('Scanning task is now done. Waiting for copy jobs to finish')

# Wait for all copy jobs to finish
for copyOp in copyOps:
    copyOp.join()

timeTakenSeconds = round(time.time() - startTime, 2)
print(fileCount, 'files were uploaded in', timeTakenSeconds, 'seconds')