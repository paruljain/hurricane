import scanner
import upload
from queue import Queue
import time

fileQ = Queue()
errorReportQ = Queue()

scanner.run('c:\\python', fileQ, errorReportQ)

upload.run(
    awsRegion = 'us-east-1',
    awsAccessKey = 'RNLJDN0K03B7HHZPZTK3',
    awsSecretKey = 'joayaC6Gw5JfzHDoYTFWcQH0xJT94Bpb5Eroood2',
    awsHost = '192.168.68.113',
    awsPort = 9000,
    awsBucket = 'test',
    fileQ = fileQ,
    errorReportQ = errorReportQ,
    numThreads=100
)

while not upload.isDone():
    print(scanner.getScannedCount(), 'files scanned;', upload.filesUploaded(), 'files uploaded')
    time.sleep(2)

for _ in range(errorReportQ.qsize()):
    print(errorReportQ.get())