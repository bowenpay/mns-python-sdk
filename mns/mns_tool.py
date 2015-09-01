#coding=utf-8
# Copyright (C) 2015, Alibaba Cloud Computing

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import sys
import string
import types
from mns_exception import *

METHODS = ["PUT", "POST", "GET", "DELETE"]
PERMISSION_ACTIONS = ["setqueueattributes", "getqueueattributes", "sendmessage", "receivemessage", "deletemessage", "peekmessage", "changevisibility"]

class ValidatorBase:
    @staticmethod
    def validate(req):
        pass

    @staticmethod        
    def type_validate(item, valid_type):
        if not (type(item) is valid_type):
            raise MNSClientParameterException("TypeInvalid", "Bad type: '%s', '%s' expect type '%s'." % (type(item), item, valid_type))

    @staticmethod
    def marker_validate(req):
        ValidatorBase.type_validate(req.marker, types.StringType)

    @staticmethod
    def retnumber_validate(req):
        ValidatorBase.type_validate(req.ret_number, types.IntType)
        if (req.ret_number != -1 and req.ret_number <= 0 ):
            raise MNSClientParameterException("HeaderInvalid", "Bad value: '%s', x-mns-number should larger than 0." % req.ret_number)

    @staticmethod
    def queuename_validate(queue_name):
        #type
        ValidatorBase.type_validate(queue_name, types.StringType)

        #length
        if len(queue_name) < 1:
            raise MNSClientParameterException("QueueNameInvalid", "Bad value: '%s', the length of queue_name should larger than 1." % queue_name)

    @staticmethod
    def list_condition_validate(req):
        if req.prefix != "":
            ValidatorBase.queuename_validate(req.prefix)

        ValidatorBase.marker_validate(req)
        ValidatorBase.retnumber_validate(req)

class QueueValidator(ValidatorBase):
    @staticmethod
    def queue_validate(req):
        #type
        ValidatorBase.type_validate(req.visibility_timeout, types.IntType)
        ValidatorBase.type_validate(req.maximum_message_size, types.IntType)
        ValidatorBase.type_validate(req.message_retention_period, types.IntType)
        ValidatorBase.type_validate(req.delay_seconds, types.IntType)
        ValidatorBase.type_validate(req.polling_wait_seconds, types.IntType)

        #value
        if req.visibility_timeout != -1 and req.visibility_timeout <= 0:
            raise MNSClientParameterException("QueueAttrInvalid", "Bad value: '%d', visibility timeout should larger than 0." % req.visibility_timeout)
        if req.maximum_message_size != -1 and req.maximum_message_size <= 0:
            raise MNSClientParameterException("QueueAttrInvalid", "Bad value: '%d', maximum message size should larger than 0." % req.maximum_message_size)
        if req.message_retention_period != -1 and req.message_retention_period <= 0:
            raise MNSClientParameterException("QueueAttrInvalid", "Bad value: '%d', message retention period should larger than 0." % req.message_retention_period)
        if req.delay_seconds != -1 and req.delay_seconds < 0:
            raise MNSClientParameterException("QueueAttrInvalid", "Bad value: '%d', delay seconds should larger than 0." % req.delay_seconds)
        if req.polling_wait_seconds != -1 and req.polling_wait_seconds < 0:
            raise MNSClientParameterException("QueueAttrInvalid", "Bad value: '%d', polling wait seconds should larger than 0." % req.polling_wait_seconds)

class MessageValidator(ValidatorBase):
    @staticmethod 
    def sendmessage_attr_validate(req):
        #type
        ValidatorBase.type_validate(req.message_body, types.StringType)
        ValidatorBase.type_validate(req.delay_seconds, types.IntType)
        ValidatorBase.type_validate(req.priority, types.IntType)

        #value
        if req.message_body == "":
            raise MNSClientParameterException("MessageBodyInvalid", "Bad value: '', message body should not be None.")

        if req.delay_seconds != -1 and req.delay_seconds < 0:
            raise MNSClientParameterException("DelaySecondsInvalid", "Bad value: '%d', delay_seconds should larger than 0." % req.delay_seconds)

        if req.priority != -1 and req.priority < 0:
            raise MNSClientParameterException("PriorityInvalid", "Bad value: '%d', priority should larger than 0." % req.priority)

    @staticmethod
    def receiphandle_validate(receipt_handle):
        if (receipt_handle == ""):
            raise MNSClientParameterException("ReceiptHandleInvalid", "The receipt handle should not be null.")       

    @staticmethod
    def waitseconds_validate(wait_seconds):
        if wait_seconds != -1 and wait_seconds < 0:
            raise MNSClientParameterException("WaitSecondsInvalid", "Bad value: '%d', wait_seconds should larger than 0." % wait_seconds)

    @staticmethod
    def batchsize_validate(batch_size):
        if batch_size != -1 and batch_size < 0:
            raise MNSClientParameterException("BatchSizeInvalid", "Bad value: '%d', batch_size should larger than 0." % batch_size)

class CreateQueueValidator(QueueValidator):
    @staticmethod
    def validate(req):
        QueueValidator.validate(req)
        QueueValidator.queuename_validate(req.queue_name)
        QueueValidator.queue_validate(req)

class DeleteQueueValidator(QueueValidator):
    @staticmethod
    def validate(req):
        QueueValidator.validate(req)
        QueueValidator.queuename_validate(req.queue_name)

class ListQueueValidator(QueueValidator):
    @staticmethod
    def validate(req):
        QueueValidator.validate(req)
        QueueValidator.list_condition_validate(req)

class SetQueueAttrValidator(QueueValidator):
    @staticmethod
    def validate(req):
        QueueValidator.validate(req)
        QueueValidator.queuename_validate(req.queue_name)

class GetQueueAttrValidator(QueueValidator):
    @staticmethod
    def validate(req):
        QueueValidator.validate(req)
        QueueValidator.queuename_validate(req.queue_name)

class SendMessageValidator(MessageValidator):
    @staticmethod
    def validate(req):
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
        MessageValidator.sendmessage_attr_validate(req)

class BatchSendMessageValidator(MessageValidator):
    @staticmethod
    def validate(req): 
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
        for entry in req.message_list:
            MessageValidator.sendmessage_attr_validate(entry)

class ReceiveMessageValidator(MessageValidator):
    @staticmethod
    def validate(req):
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
        MessageValidator.waitseconds_validate(req.wait_seconds)
       
class BatchReceiveMessageValidator(MessageValidator):
    @staticmethod
    def validate(req):
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
        MessageValidator.batchsize_validate(req.batch_size)
        MessageValidator.waitseconds_validate(req.wait_seconds)
        
class DeleteMessageValidator(MessageValidator):
    @staticmethod
    def validate(req):
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
        MessageValidator.receiphandle_validate(req.receipt_handle)

class BatchDeleteMessageValidator(MessageValidator):
    @staticmethod
    def validate(req):
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
        for receipt_handle in req.receipt_handle_list:
            MessageValidator.receiphandle_validate(receipt_handle)

class PeekMessageValidator(MessageValidator):
    @staticmethod
    def validate(req):
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
       
class BatchPeekMessageValidator(MessageValidator):
    @staticmethod
    def validate(req):
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
        MessageValidator.batchsize_validate(req.batch_size)
        
class ChangeMsgVisValidator(MessageValidator):
    @staticmethod
    def validate(req):
        MessageValidator.validate(req)
        MessageValidator.queuename_validate(req.queue_name)
        MessageValidator.receiphandle_validate(req.receipt_handle)
        if (req.visibility_timeout < 0 or req.visibility_timeout > 43200 ):
            raise MNSClientParameterException("VisibilityTimeoutInvalid", "Bad value: '%d', visibility timeout should between 0 and 43200." % req.visibility_timeout)
