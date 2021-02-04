import scanner
import upload
from queue import Queue

upload.awsHost = '192.168.68.113'
upload.awsPort = 9000
upload.awsRegion = 'us-east-1'
upload.awsAccessKey = 'RNLJDN0K03B7HHZPZTK3'
upload.awsSecretKey = 'joayaC6Gw5JfzHDoYTFWcQH0xJT94Bpb5Eroood2'
#upload.awsBucket = 'test'

fileQ = Queue()
errorReportQ = Queue()

upload.fileQ = fileQ
upload.errorReportQ = errorReportQ

#upload.run(1)
scanner.run('c:\\python\\scripts', fileQ, errorReportQ)
while True:
    try:
        print(fileQ.get(True, 5))
    except:
        break
