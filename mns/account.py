#coding=utf-8
# Copyright (C) 2015, Alibaba Cloud Computing

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

from mns_client import MNSClient
from mns_request import *
from queue import Queue

class Account:
    def __init__(self, host, access_id, access_key):
        """ 
            @type host: string
            @param host: 访问的url，例如：http://$accountid.mns.cn-hangzhou.aliyuncs.com

            @type access_id: string
            @param access_id: 用户的AccessId, 阿里云官网获取

            @type access_key: string
            @param access_key: 用户的AccessKey，阿里云官网获取

            @note: Exception
            :: MNSClientParameterException host格式错误
        """
        self.access_id = access_id
        self.access_key = access_key
        self.mns_client = MNSClient(host, access_id, access_key)
        self.debug = False

    def set_debug(self, debug):
        self.debug = debug

    def set_client(self, host):
        """ 设置访问的url

            @type host: string
            @param host: 访问的url，例如：http://$accountid-new.mns.cn-hangzhou.aliyuncs.com

            @note: Exception
            :: MNSClientParameterException host格式错误
        """
        self.mns_client = MNSClient(host, self.access_id, self.access_key)

    def get_queue(self, queue_name):
        """ 获取Account的一个Queue对象

            @type queue_name: string
            @param queue_name: 队列名

            @rtype: Queue object
            @return: 返回该Account的一个Queue对象
        """
        return Queue(queue_name, self.mns_client, self.debug)

    def get_client(self):
        """ 获取queue client 

            @rtype: MNSClient object
            @return: 返回使用的MNSClient object
        """
        return self.mns_client

    def list_queue(self, prefix = "", ret_number = -1, marker = ""):
        """ 列出Account的队列

            @type prefix: string
            @param prefix: 队列名的前缀

            @type ret_number: int
            @param ret_number: list_queue最多返回的队列数

            @type marker: string
            @param marker: list_queue的起始位置，上次list_queue返回的next_marker

            @rtype: tuple
            @return: QueueURL的列表和下次list queue的起始位置; 如果所有queue都list出来，next_marker为"".

            @note: Exception
            :: MNSClientParameterException  参数格式异常
            :: MNSClientNetworkException    网络异常
            :: MNSServerException           mns处理异常
        """
        req = ListQueueRequest(prefix, ret_number, marker)
        resp = ListQueueResponse()
        self.mns_client.list_queue(req, resp)
        self.debuginfo(resp)
        return resp.queueurl_list, resp.next_marker

    def debuginfo(self, resp):
        if self.debug:
            print "===================DEBUG INFO==================="
            print "RequestId: %s" % resp.header["x-mns-request-id"]
            print "================================================"

