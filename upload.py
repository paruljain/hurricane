import http.client
import os
from aws_request_signer import AwsRequestSigner, UNSIGNED_PAYLOAD
from urllib.parse import quote
from fileReader import FileReader
from threading import Thread

__shutdown = False
awsRegion = None
awsAccessKey = None
awsSecretKey = None
awsHost = None
awsPort = None
awsBucket = None
blockSize = 1048576
fileQ = None
errorReportQ = None

def shutdown():
    global __shutdown
    __shutdown = True

class __Uploader:
    def __init__(self):
        self.filesUploaded = 0
        self.bytesUploaded = 0
        self.requestSigner = AwsRequestSigner(awsRegion, awsAccessKey, awsSecretKey, 's3')
        self.conn = http.client.HTTPConnection(host=awsHost, port=awsPort, blocksize=blockSize)

    def upload(self):
        while True:
            if __shutdown:
                break
            try:
                f = fileQ.get(True, 5)
            except Exception:
                # Exit as there are no more files in the queue to upload
                self.conn.close()
                break
            
            if os.name == 'nt':
                k = '/' + f.replace('\\', '/')
            else:
                k = f

            URL = 'http://' + awsHost + ':' + str(awsPort) + '/' + awsBucket + quote(k)

            try:
                fileSize = os.stat(f).st_size
            except Exception as err:
                # For whatever reason we are unable to stat the file
                # Perhaps the file does not exist, or we do not have access to it
                # Record the problem and go to the next file
                errorReportQ.put({
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
                fr = FileReader(f, blockSize)
            except Exception:
                errorReportQ.put({
                    'filePath': f,
                    'key': k,
                    'operation': 'open file',
                    'result': 'failed',
                    'error': str(err)
                })
                continue
            fr.run()
            try:
                self.conn.request(method='PUT', url='/' + awsBucket + quote(k), headers=headers, body=fr)
                res = self.conn.getresponse()
                respBody = res.read()
            except Exception as err:
                errorReportQ.put({
                    'filePath': f,
                    'key': k,
                    'operation': 's3 connect',
                    'result': 'failed',
                    'error': str(err)
                })
                self.conn.close()
                fr.close()
                break

            if res.status < 200 or res.status > 299:
                errorReportQ.put({
                    'filePath': f,
                    'key': k,
                    'operation': 'http request',
                    'result': 'failed',
                    'error': 'Status code: ' + str(res.status) + ' Body: ' + str(respBody)
                })
                self.conn.close()
                fr.close()
                break
            
            self.filesUploaded += 1

    def run(self):
        self.thread = Thread(target=self.upload)
        self.thread.start()


__instances = []
def run(numThreads=1):
    global __instances
    if all(v is not None for v in [awsRegion, awsAccessKey, awsHost, awsPort, awsBucket, fileQ, errorReportQ]):
        raise SyntaxError('All class variables must be set before run is called')
    
    for _ in range(numThreads):
        instance = __Uploader()
        __instances.append(instance)
        instance.run()
