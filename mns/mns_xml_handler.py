#coding=utf-8
# Copyright (C) 2015, Alibaba Cloud Computing

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import xml.dom.minidom
import base64
import string
from mns_exception import *
from mns_request import *

XMLNS = "http://mns.aliyuncs.com/doc/v1/"
class EncoderBase:
    @staticmethod
    def insert_if_valid(item_name, item_value, invalid_value, data_dic):
        if item_value != invalid_value:
            data_dic[item_name] = item_value

    @staticmethod
    def list_to_xml(tag_name1, tag_name2, data_list):
        doc = xml.dom.minidom.Document() 
        rootNode = doc.createElement(tag_name1)
        rootNode.attributes["xmlns"] = XMLNS
        doc.appendChild(rootNode)
        if data_list:
            for item in data_list:
                keyNode = doc.createElement(tag_name2)
                rootNode.appendChild(keyNode)
                keyNode.appendChild(doc.createTextNode(item))
        else:
            nullNode = doc.createTextNode("")
            rootNode.appendChild(nullNode)
        return doc.toxml("utf-8")

    @staticmethod
    def dic_to_xml(tag_name, data_dic):
        doc = xml.dom.minidom.Document()
        rootNode = doc.createElement(tag_name)
        rootNode.attributes["xmlns"] = XMLNS
        doc.appendChild(rootNode)
        if data_dic:
            for k,v in data_dic.items():
                keyNode = doc.createElement(k)
                rootNode.appendChild(keyNode)
                keyNode.appendChild(doc.createTextNode(v))
        else:
            nullNode = doc.createTextNode("")
            rootNode.appendChild(nullNode)
        return doc.toxml("utf-8")

    @staticmethod
    def listofdic_to_xml(root_tagname, sec_tagname, dataList):
        doc = xml.dom.minidom.Document() 
        rootNode = doc.createElement(root_tagname)
        rootNode.attributes["xmlns"] = XMLNS
        doc.appendChild(rootNode)
        if dataList:
            for subData in dataList:
                secNode = doc.createElement(sec_tagname)
                rootNode.appendChild(secNode)
                if not subData:
                    nullNode = doc.createTextNode("")
                    secNode.appendChild(nullNode)
                    continue
                for k,v in subData.items():
                    keyNode = doc.createElement(k)
                    secNode.appendChild(keyNode)
                    keyNode.appendChild(doc.createTextNode(v))
        else:
            nullNode = doc.createTextNode("")
            rootNode.appendChild(nullNode)
        return doc.toxml("utf-8")

class QueueEncoder(EncoderBase):
    @staticmethod
    def encode(data, has_slice = True):
        queue = {}
        EncoderBase.insert_if_valid("VisibilityTimeout", str(data.visibility_timeout), "-1", queue)
        EncoderBase.insert_if_valid("MaximumMessageSize", str(data.maximum_message_size), "-1", queue)
        EncoderBase.insert_if_valid("MessageRetentionPeriod", str(data.message_retention_period), "-1", queue)
        EncoderBase.insert_if_valid("DelaySeconds", str(data.delay_seconds), "-1", queue)
        EncoderBase.insert_if_valid("PollingWaitSeconds", str(data.polling_wait_seconds), "-1", queue)
        return EncoderBase.dic_to_xml("Queue", queue)

class MessageEncoder(EncoderBase):
    @staticmethod
    def encode(data):
        message = {}
        if data.base64encode:
            msgbody = base64.b64encode(data.message_body)
        else:
            msgbody = data.message_body
        EncoderBase.insert_if_valid("MessageBody", msgbody, "", message)
        EncoderBase.insert_if_valid("DelaySeconds", str(data.delay_seconds), "-1", message)
        EncoderBase.insert_if_valid("Priority", str(data.priority), "-1", message)
        return EncoderBase.dic_to_xml("Message", message)

class MessagesEncoder:
    @staticmethod
    def encode(message_list, base64encode):
        msglist = []
        for msg in message_list:
            item = {}
            if base64encode:
                msgbody = base64.b64encode(msg.message_body)
            else:
                msgbody = msg.message_body
            EncoderBase.insert_if_valid("MessageBody", msgbody, "", item)
            EncoderBase.insert_if_valid("DelaySeconds", str(msg.delay_seconds), "-1", item)
            EncoderBase.insert_if_valid("Priority", str(msg.priority), "-1", item)
            msglist.append(item)
        return EncoderBase.listofdic_to_xml("Messages", "Message", msglist)

class ReceiptHandlesEncoder:
    @staticmethod
    def encode(receipt_handle_list):
        return EncoderBase.list_to_xml("ReceiptHandles", "ReceiptHandle", receipt_handle_list)
        
#-------------------------------------------------decode-----------------------------------------------------#
class DecoderBase:
    @staticmethod
    def xml_to_nodes(tag_name, xml_data):
        if xml_data == "":
            raise MNSClientNetworkException("RespDataDamaged", "Xml data is \"\"!")
            
        try:
            dom = xml.dom.minidom.parseString(xml_data)
        except Exception, e:
            raise MNSClientNetworkException("RespDataDamaged", xml_data)
            
        nodelist = dom.getElementsByTagName(tag_name)
        if not nodelist:
            raise MNSClientNetworkException("RespDataDamaged", "No element with tag name '%s'.\nData:%s" % (tag_name, xml_data))
            
        return nodelist[0].childNodes
    
    @staticmethod
    def xml_to_dic(tag_name, xml_data, data_dic):
        for node in DecoderBase.xml_to_nodes(tag_name, xml_data):
            if ( node.nodeName != "#text" and node.childNodes != []):
                data_dic[node.nodeName] = str(node.childNodes[0].nodeValue.strip())

    @staticmethod
    def xml_to_listofdic(root_tagname, sec_tagname, xml_data, data_listofdic):
        for message in DecoderBase.xml_to_nodes(root_tagname, xml_data):
            if message.nodeName != sec_tagname:
                continue
            
            data_dic = {}
            for property in message.childNodes:
                if property.nodeName != "#text" and property.childNodes != []:
                    data_dic[property.nodeName] = str(property.childNodes[0].nodeValue.strip())
            data_listofdic.append(data_dic)

class ListQueueDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data, with_meta):
        queueurl_list = []
        queuemeta_list = []
        next_marker = ""
        if (xml_data != ""):
            try:
                dom = xml.dom.minidom.parseString(xml_data)
            except Exception, e:
                raise MNSClientNetworkException("RespDataDamaged", xml_data)
            nodelist = dom.getElementsByTagName("Queues")
            if ( nodelist != [] and nodelist[0].childNodes != []):
                for node in nodelist[0].childNodes:
                    if ( node.nodeName == "Queue" and node.childNodes != []):
                        queuemeta = {}
                        for node1 in node.childNodes:
                            if ( node1.nodeName == "QueueURL" and node1.childNodes != []):
                                queueurl_list.append(str(node1.childNodes[0].nodeValue.strip()))
                            if ( with_meta and node1.nodeName != "#text" and node1.childNodes != []):
                                queuemeta[str(node1.nodeName)] = str(node1.childNodes[0].nodeValue.strip())
                        if with_meta:
                            queuemeta_list.append(queuemeta)
                    elif ( node.nodeName == "NextMarker" and node.childNodes != []):
                        next_marker = str(node.childNodes[0].nodeValue.strip())
        else:
            raise MNSClientNetworkException("RespDataDamaged", "Xml data is \"\"!")
        return queueurl_list, str(next_marker), queuemeta_list

class GetQueueAttrDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data):
        data_dic = {}
        DecoderBase.xml_to_dic("Queue", xml_data, data_dic)
        key_list = ["ActiveMessages", "CreateTime", "DelayMessages", "DelaySeconds", "InactiveMessages", "LastModifyTime", "MaximumMessageSize", "MessageRetentionPeriod", "QueueName", "VisibilityTimeout", "PollingWaitSeconds"]
        for key in key_list:
            if key not in data_dic.keys():
                raise MNSClientNetworkException("RespDataDamaged", xml_data)
        return data_dic

class SendMessageDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data):
        data_dic = {}
        DecoderBase.xml_to_dic("Message", xml_data, data_dic)
        key_list = ["MessageId", "MessageBodyMD5"]
        for key in key_list:
            if key not in data_dic.keys():
                raise MNSClientNetworkException("RespDataDamaged", xml_data)
        return data_dic["MessageId"], data_dic["MessageBodyMD5"]

class BatchSendMessageDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data):
        data_listofdic = []
        message_list = []
        DecoderBase.xml_to_listofdic("Messages", "Message", xml_data, data_listofdic)
        try:
            for data_dic in data_listofdic:
                entry = SendMessageResponseEntry()
                entry.message_id = data_dic["MessageId"]
                entry.message_body_md5 = data_dic["MessageBodyMD5"]
                message_list.append(entry)
        except Exception, e:
            raise MNSClientNetworkException("RespDataDamaged", xml_data)
        return message_list
    
    @staticmethod
    def decodeError(xml_data):
        try:
            return ErrorDecoder.decodeError(xml_data)
        except Exception,e:
            pass
        
        data_listofdic = []
        DecoderBase.xml_to_listofdic("Messages", "Message", xml_data, data_listofdic)
        if len(data_listofdic) == 0:
            raise MNSClientNetworkException("RespDataDamaged", xml_data)

        errType = None
        errMsg = None
        key_list1 = sorted(["ErrorCode", "ErrorMessage"])
        key_list2 = sorted(["MessageId", "MessageBodyMD5"])
        for data_dic in data_listofdic:
            keys = sorted(data_dic.keys())
            if keys != key_list1 and keys != key_list2:
                raise MNSClientNetworkException("RespDataDamaged", xml_data)
            if keys == key_list1 and errType is None:
                errType = data_dic["ErrorCode"]
                errMsg = data_dic["ErrorMessage"]
        return errType, errMsg, None, None, data_listofdic
        
class RecvMessageDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data, base64decode):
        data_dic = {}
        DecoderBase.xml_to_dic("Message", xml_data, data_dic)
        key_list = ["DequeueCount", "EnqueueTime", "FirstDequeueTime", "MessageBody", "MessageId", "MessageBodyMD5", "NextVisibleTime", "ReceiptHandle", "Priority"]
        for key in key_list:
            if key not in data_dic.keys():
                raise MNSClientNetworkException("RespDataDamaged", xml_data)
        if base64decode:
            decode_str = base64.b64decode(data_dic["MessageBody"])
            data_dic["MessageBody"] = decode_str
        return data_dic

class BatchRecvMessageDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data, base64decode):
        data_listofdic = []
        message_list = []
        DecoderBase.xml_to_listofdic("Messages", "Message", xml_data, data_listofdic)
        try:
            for data_dic in data_listofdic:
                msg = ReceiveMessageResponseEntry()
                if base64decode:
                    msg.message_body = base64.b64decode(data_dic["MessageBody"])
                else:
                    msg.message_body = data_dic["MessageBody"]
                msg.dequeue_count = string.atoi(data_dic["DequeueCount"])
                msg.enqueue_time = string.atoi(data_dic["EnqueueTime"]) 
                msg.first_dequeue_time = string.atoi(data_dic["FirstDequeueTime"])
                msg.message_id = data_dic["MessageId"]
                msg.message_body_md5 = data_dic["MessageBodyMD5"]
                msg.priority = string.atoi(data_dic["Priority"])
                msg.next_visible_time = string.atoi(data_dic["NextVisibleTime"])
                msg.receipt_handle = data_dic["ReceiptHandle"]
                message_list.append(msg)
        except Exception, e:
            raise MNSClientNetworkException("RespDataDamaged", xml_data) 
        return message_list            

class PeekMessageDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data, base64decode):
        data_dic = {}
        DecoderBase.xml_to_dic("Message", xml_data, data_dic)
        key_list = ["DequeueCount", "EnqueueTime", "FirstDequeueTime", "MessageBody", "MessageId", "MessageBodyMD5", "Priority"]
        for key in key_list:
            if key not in data_dic.keys():
                raise MNSClientNetworkException("RespDataDamaged", xml_data)
        if base64decode:
            decode_str = base64.b64decode(data_dic["MessageBody"])
            data_dic["MessageBody"] = decode_str
        return data_dic

class BatchPeekMessageDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data, base64decode):
        data_listofdic = []
        message_list = []
        DecoderBase.xml_to_listofdic("Messages", "Message", xml_data, data_listofdic)
        try:
            for data_dic in data_listofdic:
                msg = PeekMessageResponseEntry()
                if base64decode:
                    msg.message_body = base64.b64decode(data_dic["MessageBody"])
                else:
                    msg.message_body = data_dic["MessageBody"]
                msg.dequeue_count = string.atoi(data_dic["DequeueCount"])
                msg.enqueue_time = string.atoi(data_dic["EnqueueTime"])
                msg.first_dequeue_time = string.atoi(data_dic["FirstDequeueTime"])
                msg.message_id = data_dic["MessageId"]
                msg.message_body_md5 = data_dic["MessageBodyMD5"]
                msg.priority = string.atoi(data_dic["Priority"])
                message_list.append(msg)
        except Exception, e:
            raise MNSClientNetworkException("RespDataDamaged", xml_data)
        return message_list            
        
class BatchDeleteMessageDecoder(DecoderBase):
    @staticmethod
    def decodeError(xml_data):
        try:
            return ErrorDecoder.decodeError(xml_data)
        except Exception,e:
            pass
        
        data_listofdic = []
        DecoderBase.xml_to_listofdic("Errors", "Error", xml_data, data_listofdic)
        if len(data_listofdic) == 0:
            raise MNSClientNetworkException("RespDataDamaged", xml_data)

        key_list = sorted(["ErrorCode", "ErrorMessage", "ReceiptHandle"])
        for data_dic in data_listofdic:
            for key in key_list:
                keys = sorted(data_dic.keys())
                if keys != key_list:
                    raise MNSClientNetworkException("RespDataDamaged", xml_data)
        return data_listofdic[0]["ErrorCode"], data_listofdic[0]["ErrorMessage"], None, None, data_listofdic

class ChangeMsgVisDecoder(DecoderBase):
    @staticmethod
    def decode(xml_data):
        data_dic = {}
        DecoderBase.xml_to_dic("ChangeVisibility", xml_data, data_dic)

        if "ReceiptHandle" in data_dic.keys() and "NextVisibleTime" in data_dic.keys():
            return data_dic["ReceiptHandle"], data_dic["NextVisibleTime"]
        else:
            raise MNSClientNetworkException("RespDataDamaged", xml_data)

class ErrorDecoder(DecoderBase):
    @staticmethod
    def decodeError(xml_data):
        data_dic = {}
        DecoderBase.xml_to_dic("Error", xml_data, data_dic)
        key_list = ["Code", "Message", "RequestId", "HostId"]
        for key in key_list:
            if key not in data_dic.keys():
                raise MNSClientNetworkException("RespDataDamaged", xml_data)
        return data_dic["Code"], data_dic["Message"], data_dic["RequestId"], data_dic["HostId"], None                 
