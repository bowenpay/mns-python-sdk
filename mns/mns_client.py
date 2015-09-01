#coding=utf-8
# Copyright (C) 2015, Alibaba Cloud Computing

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import time
import hashlib
import hmac
import base64
import string
import platform
import pkg_info
from  mns_xml_handler import *
from mns_exception import *
from mns_request import *
from mns_tool import *
from mns_http import *

URISEC_QUEUE = "queues"
URISEC_MESSAGE = "messages"
class MNSClient:
    def __init__(self, host, access_id, access_key, version = "2015-06-06"):
        self.host = self.process_host(host)
        self.access_id = access_id
        self.access_key = access_key
        self.http = MNSHttp(self.host)
        self.version = version

    def set_connection_timeout(self, connection_timeout):
        self.http.set_connection_timeout(connection_timeout)

    def set_keep_alive(self, keep_alive):
        self.http.set_keep_alive(keep_alive)

    def close_connection(self):
        self.http.conn.close()

#===============================================sdk===============================================#
       
    def create_queue(self, req, resp):
        #check parameter
        CreateQueueValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s" % (URISEC_QUEUE, req.queue_name))
        req_inter.data = QueueEncoder.encode(req)
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            resp.queue_url = resp.header["location"]

    def delete_queue(self, req, resp):
        #check parameter
        DeleteQueueValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s" % (URISEC_QUEUE, req.queue_name))
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)

    def list_queue(self, req, resp):
        #check parameter
        ListQueueValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s" % URISEC_QUEUE)
        if req.prefix != "":
            req_inter.header["x-mns-prefix"] = req.prefix
        if req.ret_number != -1:
            req_inter.header["x-mns-ret-number"] = str(req.ret_number)
        if req.marker != "":
            req_inter.header["x-mns-marker"] = str(req.marker)
        if req.with_meta:
            req_inter.header["x-mns-with-meta"] = "true"
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            resp.queueurl_list, resp.next_marker, resp.queuemeta_list = ListQueueDecoder.decode(resp_inter.data, req.with_meta)

    def set_queue_attributes(self, req, resp):
        #check parameter
        SetQueueAttrValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s?metaoverride=true" % (URISEC_QUEUE, req.queue_name))
        req_inter.data = QueueEncoder.encode(req, False)
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response 
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)

    def get_queue_attributes(self, req, resp):
        #check parameter
        GetQueueAttrValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s" % (URISEC_QUEUE, req.queue_name))
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            queue_attr = GetQueueAttrDecoder.decode(resp_inter.data)
            resp.active_messages = string.atoi(queue_attr["ActiveMessages"])
            resp.create_time = string.atoi(queue_attr["CreateTime"])
            resp.delay_messages = string.atoi(queue_attr["DelayMessages"])
            resp.delay_seconds = string.atoi(queue_attr["DelaySeconds"])
            resp.inactive_messages = string.atoi(queue_attr["InactiveMessages"])
            resp.last_modify_time = string.atoi(queue_attr["LastModifyTime"])
            resp.maximum_message_size = string.atoi(queue_attr["MaximumMessageSize"])
            resp.message_retention_period = string.atoi(queue_attr["MessageRetentionPeriod"])
            resp.queue_name = queue_attr["QueueName"]
            resp.visibility_timeout = string.atoi(queue_attr["VisibilityTimeout"])
            resp.polling_wait_seconds = string.atoi(queue_attr["PollingWaitSeconds"])

    def send_message(self, req, resp):
        #check parameter
        SendMessageValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, uri = "/%s/%s/%s" % (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE))
        req_inter.data = MessageEncoder.encode(req)
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            resp.message_id, resp.message_body_md5 = SendMessageDecoder.decode(resp_inter.data)

    def batch_send_message(self, req, resp):
        #check parameter
        BatchSendMessageValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, uri = "/%s/%s/%s" % (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE))
        req_inter.data = MessagesEncoder.encode(req.message_list, req.base64encode)
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter) 

        #handle result, make response
        resp.status = resp_inter.status 
        resp.header = resp_inter.header 
        self.check_status(resp_inter, resp, BatchSendMessageDecoder)
        if resp.error_data == "":
            resp.message_list = BatchSendMessageDecoder.decode(resp_inter.data)

    def receive_message(self, req, resp):
        #check parameter
        ReceiveMessageValidator.validate(req)

        #make request internal
        req_url =  "/%s/%s/%s" % (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE)
        if req.wait_seconds != -1:
            req_url += "?waitseconds=%s" % req.wait_seconds
        req_inter = RequestInternal(req.method, req_url)
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            data = RecvMessageDecoder.decode(resp_inter.data, req.base64decode)
            self.make_recvresp(data, resp)

    def batch_receive_message(self, req, resp):
        #check parameter
        BatchReceiveMessageValidator.validate(req)

        #make request internal
        req_url =  "/%s/%s/%s?numOfMessages=%s" % (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE, req.batch_size)
        if req.wait_seconds != -1:
            req_url += "&waitseconds=%s" % req.wait_seconds

        req_inter = RequestInternal(req.method, req_url)
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            resp.message_list = BatchRecvMessageDecoder.decode(resp_inter.data, req.base64decode)

    def delete_message(self, req, resp):
        #check parameter
        DeleteMessageValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s/%s?ReceiptHandle=%s" % 
                                                (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE, req.receipt_handle))
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)

    def batch_delete_message(self, req, resp):
        #check parameter
        BatchDeleteMessageValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s/%s" % (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE))
        req_inter.data = ReceiptHandlesEncoder.encode(req.receipt_handle_list)
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp, BatchDeleteMessageDecoder)

    def peek_message(self, req, resp):
        #check parameter
        PeekMessageValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s/%s?peekonly=true" % 
                                                (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE))
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            data = PeekMessageDecoder.decode(resp_inter.data, req.base64decode)
            self.make_peekresp(data, resp)

    def batch_peek_message(self, req, resp):
        #check parameter
        BatchPeekMessageValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s/%s?peekonly=true&numOfMessages=%s" % 
                                                (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE, req.batch_size))
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            resp.message_list = BatchPeekMessageDecoder.decode(resp_inter.data, req.base64decode)

    def change_message_visibility(self, req, resp):
        #check parameter
        ChangeMsgVisValidator.validate(req)

        #make request internal
        req_inter = RequestInternal(req.method, "/%s/%s/%s?ReceiptHandle=%s&VisibilityTimeout=%d" % 
                                                (URISEC_QUEUE, req.queue_name, URISEC_MESSAGE, req.receipt_handle, req.visibility_timeout))
        self.build_header(req, req_inter)

        #send request
        resp_inter = self.http.send_request(req_inter)

        #handle result, make response
        resp.status = resp_inter.status
        resp.header = resp_inter.header
        self.check_status(resp_inter, resp)
        if resp.error_data == "":
            resp.receipt_handle, resp.next_visible_time = ChangeMsgVisDecoder.decode(resp_inter.data)

    
###################################################################################################        
#----------------------internal-------------------------------------------------------------------#
    def build_header(self, req, req_inter):
        if self.http.is_keep_alive():
            req_inter.header["Connection"] = "Keep-Alive"
        if req_inter.data != "":
            req_inter.header["content-md5"] = base64.b64encode(hashlib.md5(req_inter.data).hexdigest())
            req_inter.header["content-type"] = "text/xml;charset=UTF-8"
        req_inter.header["x-mns-version"] = self.version
        req_inter.header["host"] = self.host
        req_inter.header["date"] = time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime())
        req_inter.header["user-agent"] = "aliyun-sdk-python/%s(%s/%s;%s)" % \
                                         (pkg_info.version, platform.system(), platform.release(), platform.python_version())
        req_inter.header["Authorization"] = self.get_signature(req_inter.method, req_inter.header, req_inter.uri)

    def get_signature(self,method,headers,resource):
        content_md5 = self.get_element('content-md5', headers)
        content_type = self.get_element('content-type', headers)
        date = self.get_element('date', headers)
        canonicalized_resource = resource
        canonicalized_mns_headers = ""
        if len(headers) > 0:
            x_header_list = headers.keys()
            x_header_list.sort()
            for k in x_header_list:
                if k.startswith('x-mns-'):
                    canonicalized_mns_headers += k + ":" + headers[k] + "\n"
        string_to_sign = "%s\n%s\n%s\n%s\n%s%s" % (method, content_md5, content_type, date, canonicalized_mns_headers, canonicalized_resource)
        h = hmac.new(self.access_key, string_to_sign, hashlib.sha1)
        signature = base64.b64encode(h.digest())
        signature = "MNS " + self.access_id + ":" + signature
        return signature

    def get_element(self, name, container):
        if name in container:
            return container[name]
        else:
            return ""

    def check_status(self, resp_inter, resp, decoder=ErrorDecoder):
        if resp_inter.status >= 200 and resp_inter.status < 400:
            resp.error_data = ""
        else:
            resp.error_data = resp_inter.data
            if resp_inter.status >= 400 and resp_inter.status <= 600:
                excType, excMessage, reqId, hostId, subErr = decoder.decodeError(resp.error_data)
                if reqId is None:
                    reqId = resp.header["x-mns-request-id"]
                raise MNSServerException(excType, excMessage, reqId, hostId, subErr)
            else:
                raise MNSClientNetworkException("UnkownError", resp_inter.data)

    def make_recvresp(self, data, resp):
        resp.dequeue_count = string.atoi(data["DequeueCount"])
        resp.enqueue_time = string.atoi(data["EnqueueTime"])
        resp.first_dequeue_time = string.atoi(data["FirstDequeueTime"])
        resp.message_body = data["MessageBody"]
        resp.message_id = data["MessageId"]
        resp.message_body_md5 = data["MessageBodyMD5"]
        resp.next_visible_time = string.atoi(data["NextVisibleTime"])
        resp.receipt_handle = data["ReceiptHandle"]
        resp.priority = string.atoi(data["Priority"])

    def make_peekresp(self, data, resp):
        resp.dequeue_count = string.atoi(data["DequeueCount"])
        resp.enqueue_time = string.atoi(data["EnqueueTime"])
        resp.first_dequeue_time = string.atoi(data["FirstDequeueTime"])
        resp.message_body = data["MessageBody"]
        resp.message_id = data["MessageId"]
        resp.message_body_md5 = data["MessageBodyMD5"]
        resp.priority = string.atoi(data["Priority"])

    def process_host(self, host):
        if host.startswith("http://"):
            if host.endswith("/"):
                host =  host[:-1]
            host = host[len("http://"):]
            return host
        else:
            raise MNSClientParameterException("InvalidHost", "Only support http prototol. Invalid host:%s" % host)
