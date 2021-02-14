from urllib.parse import urlparse, quote
import http.client
from aws_request_signer import AwsRequestSigner, UNSIGNED_PAYLOAD

class S3FileUpload:
    def __init__(self, awsRegion, awsAccessKey, awsSecretKey, s3EndPoint, s3Bucket):
        if not s3EndPoint.startswith('http://'):
            raise ValueError('s3EndPoint must start with http://')
        parsedURL = urlparse(s3EndPoint)
        self.requestSigner = AwsRequestSigner(awsRegion, awsAccessKey, awsSecretKey, 's3')
        self.conn = http.client.HTTPConnection(host=parsedURL.hostname, port=parsedURL.port or 80)
        self.pathBase = '/' + quote(s3Bucket)
        self.urlBase = s3EndPoint + self.pathBase
        self.readyForNewFile = True
        self.totalBytesUploaded = 0
        self.bytesUploaded = 0
        self.filesUploaded = 0
        self.fileSize = None

    def startFileSend(self, key, fileSize):
        if not self.readyForNewFile:
            self.abort()

        headers = {"Content-Type": "application/octet-stream", "Content-Length": str(fileSize)}
        headers.update(self.requestSigner.sign_with_headers("PUT", self.urlBase + quote(key), headers, content_hash=UNSIGNED_PAYLOAD))

        self.bytesUploaded = 0
        self.fileSize = fileSize
        self.readyForNewFile = False
        self.conn.putrequest(method='PUT', url=self.pathBase + quote(key)) # will auto open socket to s3 server if not open
        for k, v in headers.items():
            self.conn.putheader(k, v)
        self.conn.endheaders()
        
    def sendFileData(self, data):
        if self.readyForNewFile:
            raise http.client.HTTPException('Must call startFileSend before sending data')
        if data and len(data) > 0:
            self.conn.send(data)
            self.bytesUploaded += len(data)
            self.totalBytesUploaded += len(data)

    def endFileSend(self):
        # print('size =', self.fileSize, '; uploaded =', self.bytesUploaded)
        if self.bytesUploaded < self.fileSize:
            self.abort()
            raise http.client.HTTPException('File send was aborted')
        res = self.conn.getresponse()
        respBody = res.read()
        self.readyForNewFile = True
        if res.status < 200 or res.status > 299:
            raise http.client.HTTPException('status: ' + str(res.status) + ': ' + str(respBody))
        self.filesUploaded += 1

    def abort(self):
        self.conn.close()
        self.readyForNewFile = True
