#!/usr/bin/env python
#coding=utf8
# Copyright (C) 2015, Alibaba Cloud Computing

#Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

#The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import sys
import time
from mns.account import Account
from mns.queue import *
import ConfigParser

cfgFN = "sample.cfg"
required_ops = [("Base", "AccessKeyId"), ("Base", "AccessKeySecret"), ("Base", "Endpoint")]

parser = ConfigParser.ConfigParser()
parser.read(cfgFN)
for sec,op in required_ops:
    if not parser.has_option(sec, op):
        sys.stderr.write("ERROR: need (%s, %s) in %s.\n" % (sec,op,cfgFN))
        sys.stderr.write("Read README to get help inforamtion.\n")
        sys.exit(1)

#获取配置信息
## AccessKeyId      阿里云官网获取
## AccessKeySecret  阿里云官网获取
## Endpoint         阿里云消息和通知服务官网获取, Example: http://$AccountId.mns.cn-hangzhou.aliyuncs.com
accessKeyId = parser.get("Base", "AccessKeyId")
accessKeySecret = parser.get("Base", "AccessKeySecret")
endpoint = parser.get("Base", "Endpoint")

#初始化my_account和my_queue
my_account = Account(endpoint, accessKeyId, accessKeySecret)
my_queue = my_account.get_queue("MyQueue-%s" % time.strftime("%y%m%d-%H%M%S", time.localtime()))

#创建队列
## message被receive后，持续不可消费的时间   100秒
## message body的最大长度                   10240Byte
## message最长存活时间                      3600秒
## 新message可消费的默认延迟时间            10秒
## receive message时，长轮询时间            20秒
queue_meta = QueueMeta()
queue_meta.set_visibilitytimeout(100)
queue_meta.set_maximum_message_size(10240)
queue_meta.set_message_retention_period(3600)
queue_meta.set_delay_seconds(10)
queue_meta.set_polling_wait_seconds(20)
try:
    queue_url = my_queue.create(queue_meta)
    sys.stdout.write("Create Queue Succeed!\nQueueURL:%s\n\n" % queue_url)
except MNSExceptionBase, e:
    sys.stderr.write("Create Queue Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#修改队列属性
## message被receive后，持续不可消费的时间   50秒
## message body的最大长度                   5120Byte
## message最长存活时间                      1800秒
## 新message可消费的默认延迟时间            5秒
## receive message时，长轮询时间            10秒
queue_meta = QueueMeta()
queue_meta.set_visibilitytimeout(50)
queue_meta.set_maximum_message_size(5120)
queue_meta.set_message_retention_period(1800)
queue_meta.set_delay_seconds(5)
queue_meta.set_polling_wait_seconds(10)
try:
    queue_url = my_queue.set_attributes(queue_meta)
    sys.stdout.write("Set Queue Attributes Succeed!\n\n")
except MNSExceptionBase, e:
    sys.stderr.write("Set Queue Attributes Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#获取队列属性
## 除可设置属性外，返回如下属性：
## ActiveMessages:      可消费消息数，近似值
## InactiveMessages：   正在被消费的消息数，近似值
## DelayMessages：      延迟消息数，近似值
## CreateTime：         queue创建时间，单位：秒
## LastModifyTime：     修改queue属性的最近时间，单位：秒
try:
    queue_meta = my_queue.get_attributes()
    sys.stdout.write("Get Queue Attributes Succeed! \
                      \nQueueName: %s\nVisibilityTimeout: %s \
                      \nMaximumMessageSize: %s\nDelaySeconds: %s \
                      \nPollingWaitSeconds: %s\nActiveMessages: %s \
                      \nInactiveMessages: %s\nDelayMessages: %s \
                      \nCreateTime: %s\nLastModifyTime: %s\n\n" % 
                      (queue_meta.queue_name, queue_meta.visibility_timeout,
                       queue_meta.maximum_message_size, queue_meta.delay_seconds,
                       queue_meta.polling_wait_seconds, queue_meta.active_messages,
                       queue_meta.inactive_messages, queue_meta.delay_messages,
                       queue_meta.create_time, queue_meta.last_modify_time))
except MNSExceptionBase, e:
    sys.stderr.write("Get Queue Attributes Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#列出所有队列
## prefix               指定queue name前缀
## ret_number           单次list_queue最大返回队列个数
## marker               list_queue的开始位置; 当一次list queue不能列出所有队列时，返回的next_marker作为下一次list queue的marker参数
try:
    prefix = ""
    ret_number = 10
    marker = ""
    total_qcount = 0
    while(True):
        queue_url_list, next_marker = my_account.list_queue(prefix, ret_number, marker)
        total_qcount += len(queue_url_list)
        for queue_url in queue_url_list:
            sys.stdout.write("QueueURL:%s\n" % queue_url)
        if(next_marker == ""):
            break
        marker = next_marker
    sys.stdout.write("List Queue Succeed! Total Queue Count:%s!\n\n" % total_qcount)        
except MNSExceptionBase, e:
    sys.stderr.write("List Queue Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#发送消息
## set_delayseconds     设置消息的延迟时间，单位：秒
## set_priority         设置消息的优先级
## 返回如下属性：
## MessageId            消息编号
## MessageBodyMd5       消息正文的MD5值
msg_body = "I am test Message."
message = Message(msg_body)
message.set_delayseconds(0)
message.set_priority(10)
try:
    send_msg = my_queue.send_message(message)
    sys.stdout.write("Send Message Succeed.\nMessageBody:%s\nMessageId:%s\nMessageBodyMd5:%s\n\n" % (msg_body, send_msg.message_id, send_msg.message_body_md5))
except MNSExceptionBase, e:
    sys.stderr.write("Send Message Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#查看消息
## 返回如下属性：
## MessageId            消息编号
## MessageBodyMD5       消息正文的MD5值
## MessageBody          消息正文
## DequeueCount         消息被消费的次数
## EnqueueTime          消息发送到队列的时间，单位：毫秒
## FirstDequeueTime     消息第一次被消费的时间，单位：毫秒
## Priority             消息的优先级
try:
    peek_msg = my_queue.peek_message()
    sys.stdout.write("Peek Message Succeed! \
                      \nMessageId: %s\nMessageBodyMD5: %s \
                      \nMessageBody: %s\nDequeueCount: %s \
                      \nEnqueueTime: %s\nFirstDequeueTime: %s \
                      \nPriority: %s\n\n" % 
                      (peek_msg.message_id, peek_msg.message_body_md5,
                       peek_msg.message_body, peek_msg.dequeue_count,
                       peek_msg.enqueue_time,  peek_msg.first_dequeue_time,
                       peek_msg.priority))
except MNSExceptionBase, e:
    sys.stderr.write("Peek Message Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#消费消息
## wait_seconds 指定长轮询时间，单位：秒
## 返回属性和查看消息基本相同，增加NextVisibleTime和ReceiptHandle
## NextVisibleTime      消息下次可被消费的时间，单位：毫秒
## ReceiptHandle        本次消费消息产生的临时句柄，用于删除或修改处于Inactive的消息，NextVisibleTime之前有效
try:
    wait_seconds = 10
    recv_msg = my_queue.receive_message(wait_seconds)
    sys.stdout.write("Receive Message Succeed! \
                      \nMessageId: %s\nMessageBodyMD5: %s \
                      \nMessageBody: %s\nDequeueCount: %s \
                      \nEnqueueTime: %s\nFirstDequeueTime: %s \
                      \nPriority: %s\nNextVisibleTime: %s \
                      \nReceiptHandle: %s\n\n" % 
                      (recv_msg.message_id, recv_msg.message_body_md5,
                       recv_msg.message_body, recv_msg.dequeue_count,
                       recv_msg.enqueue_time, recv_msg.first_dequeue_time,
                       recv_msg.priority, recv_msg.next_visible_time,
                       recv_msg.receipt_handle))
except MNSExceptionBase, e:
    sys.stderr.write("Receive Message Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#修改消息下次可消费时间
## 参数1:recv_msg.receitp_handle    消费消息返回的临时句柄
## 参数2:35                         将消息下次可被消费时间修改为：now+35秒
## 返回属性：
## ReceiptHandle                    新的临时句柄，下次删除或修改这条消息时使用
## NextVisibleTime                  消息下次可被消费的时间，单位：毫秒
try:
    change_msg_vis = my_queue.change_message_visibility(recv_msg.receipt_handle, 35)
    sys.stdout.write("Change Message Visibility Succeed!\nReceiptHandle:%s\nNextVisibleTime:%s\n\n" % 
                      (change_msg_vis.receipt_handle, change_msg_vis.next_visible_time))
except MNSExceptionBase, e:
    sys.stderr.write("Change Message Visibility Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#删除消息
## change_msg_vis.receipt_handle    最近操作该消息的临时句柄
try:
    my_queue.delete_message(change_msg_vis.receipt_handle)
    sys.stdout.write("Delete Message Succeed.\n\n")
except MNSExceptionBase, e:
    sys.stderr.write("Delete Message Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#批量发送消息,消息条数的限制请参考官网API文档
## 返回多条消息的MessageId和MessageBodyMD5
messages = []
msg_cnt = 10
for i in range(msg_cnt):
    msg = Message("I am test Message %s." % i)
    msg.set_delayseconds(0)
    msg.set_priority(6)
    messages.append(msg)
try:
    send_msgs = my_queue.batch_send_message(messages)
    sys.stdout.write("Batch Send Message Succeed.")
    for msg in send_msgs:
        sys.stdout.write("MessageId:%s\nMessageBodyMd5:%s\n\n" % (msg.message_id, msg.message_body_md5))
except MNSExceptionBase, e:
    sys.stderr.write("Batch Send Message Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#批量查看消息
## batch_size   指定批量获取的消息数
## 返回多条消息的属性，每条消息属性和peek_message相同
try:
    batch_size = 3
    peek_msgs = my_queue.batch_peek_message(batch_size)
    sys.stdout.write("Batch Peek Message Succeed.\n")
    for msg in peek_msgs:
        sys.stdout.write("MessageId: %s\nMessageBodyMD5: %s \
                          \nMessageBody: %s\nDequeueCount: %s \
                          \nEnqueueTime: %s\nFirstDequeueTime: %s \
                          \nPriority: %s\n\n" %
                          (msg.message_id, msg.message_body_md5,
                           msg.message_body, msg.dequeue_count,
                           msg.enqueue_time, msg.first_dequeue_time,
                           msg.priority))
except MNSExceptionBase, e:
    sys.stderr.write("Batch Peek Message Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#批量消费消息
## batch_size 指定批量获取的消息数
## wait_seconds 指定长轮询时间，单位：秒
## 返回多条消息的属性，每条消息属性和receive_message相同
try:
    batch_size = 3
    wait_seconds = 10
    recv_msgs = my_queue.batch_receive_message(batch_size, wait_seconds)
    sys.stdout.write("Batch Receive Message Succeed.\n")
    for msg in recv_msgs:
        sys.stdout.write("MessageId: %s\nMessageBodyMD5: %s \
                          \nMessageBody: %s\nDequeueCount: %s \
                          \nEnqueueTime: %s\nFirstDequeueTime: %s \
                          \nPriority: %s\nNextVisibleTime: %s \
                          \nReceiptHandle: %s\n\n" %
                          (msg.message_id, msg.message_body_md5,
                           msg.message_body, msg.dequeue_count,
                           msg.enqueue_time, msg.first_dequeue_time,
                           msg.priority, msg.next_visible_time,
                           msg.receipt_handle))
except MNSExceptionBase, e:
    sys.stderr.write("Batch Receive Message Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#批量删除消息
## receipt_handle_list batch_receive_message返回的多个receipt handle
try:
    receipt_handle_list = [msg.receipt_handle for msg in recv_msgs]
    my_queue.batch_delete_message(receipt_handle_list)
    sys.stdout.write("Batch Delete Message Succeed.\n")
except MNSExceptionBase, e:
    sys.stderr.write("Batch Delete Message Fail!\nException:%s\n\n" % e)
    sys.exit(1)

#删除队列
try:
    my_queue.delete()
    sys.stdout.write("Delete Queue Succeed!\n\n")
except MNSExceptionBase, e:
    sys.stderr.write("Delete Queue Fail!\nException:%s\n\n" % e)
    sys.exit(1)
