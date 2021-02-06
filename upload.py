import http.client
import os
from aws_request_signer import AwsRequestSigner, UNSIGNED_PAYLOAD
from urllib.parse import quote
from fileReader import FileReader as FileReader
from threading import Thread

class __Uploader:
    def __init__(self, awsRegion, awsAccessKey, awsSecretKey, awsHost, awsPort, awsBucket, fileQ, errorReportQ, blockSize):
        self.filesUploaded = 0
        self.bytesUploaded = 0
        self.awsHost = awsHost
        self.awsPort = awsPort
        self.fileQ = fileQ
        self.errorReportQ = errorReportQ
        self.blockSize = blockSize
        self.awsBucket = awsBucket
        self.requestSigner = AwsRequestSigner(awsRegion, awsAccessKey, awsSecretKey, 's3')
        self.conn = http.client.HTTPConnection(host=awsHost, port=awsPort, blocksize=blockSize)
        self.shutdown = False
        self.done = False

    def upload(self):
        while True:
            if self.shutdown:
                break
            try:
                f = self.fileQ.get(True, 5)
            except Exception:
                # Exit as there are no more files in the queue to upload
                self.conn.close()
                self.done = True
                break
            
            if os.name == 'nt':
                k = '/' + f.replace('\\', '/').replace(':', '')
            else:
                k = f

            URL = 'http://' + self.awsHost + ':' + str(self.awsPort) + '/' + self.awsBucket + quote(k)

            try:
                fileSize = os.stat(f).st_size
            except Exception as err:
                # For whatever reason we are unable to stat the file
                # Perhaps the file does not exist, or we do not have access to it
                # Record the problem and go to the next file
                self.errorReportQ.put({
                    'filePath': f,
                    'key': k,
                    'operation': 'stat file',
                    'result': 'failed',
                    'error': str(err)
                })
                continue
            headers = {"Content-Type": "application/octet-stream", "Content-Length": str(fileSize)}

            # Add the AWS authentication header
            headers.update(self.requestSigner.sign_with_headers("PUT", URL, headers, content_hash=UNSIGNED_PAYLOAD))

            try:
                fr = FileReader(f, self.blockSize)
            except Exception:
                self.errorReportQ.put({
                    'filePath': f,
                    'key': k,
                    'operation': 'open file',
                    'result': 'failed',
                    'error': str(err)
                })
                continue
            fr.run()
            try:
                self.conn.request(method='PUT', url='/' + self.awsBucket + quote(k), headers=headers, body=fr)
                res = self.conn.getresponse()
                respBody = res.read()
            except Exception as err:
                self.errorReportQ.put({
                    'filePath': f,
                    'key': k,
                    'operation': 's3 connect',
                    'result': 'failed',
                    'error': str(err)
                })
                self.conn.close()
                fr.close()
                self.done = True
                break

            if res.status < 200 or res.status > 299:
                self.errorReportQ.put({
                    'filePath': f,
                    'key': k,
                    'operation': 'http request',
                    'result': 'failed',
                    'error': 'Status code: ' + str(res.status) + ' Body: ' + str(respBody)
                })
                self.conn.close()
                fr.close()
                self.done = True
                break
            
            self.filesUploaded += 1

    def run(self):
        self.thread = Thread(target=self.upload)
        self.thread.start()

__instances = []
def run(awsRegion, awsAccessKey, awsSecretKey, awsHost, awsPort, awsBucket, fileQ, errorReportQ, blockSize=1048576, numThreads=1):
    global __instances
    
    for _ in range(numThreads):
        instance = __Uploader(
                        awsRegion = awsRegion,
                        awsAccessKey = awsAccessKey,
                        awsSecretKey = awsSecretKey,
                        awsHost = awsHost,
                        awsPort = awsPort,
                        awsBucket = awsBucket,
                        blockSize = blockSize,
                        fileQ = fileQ,
                        errorReportQ = errorReportQ
            )
        __instances.append(instance)
        instance.run()

def isDone():
    for instance in __instances:
        if not instance.done:
            return False
    return True

def filesUploaded():
    uploaded = 0
    for instance in __instances:
        uploaded += instance.filesUploaded
    return uploaded

def shutdown():
    for instance in __instances:
        instance.shutdown = True