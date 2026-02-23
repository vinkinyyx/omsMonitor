#-*-  coding: utf-8 -*-
# 简易版 WXBizMsgCrypt，用于处理企业微信回调验证过程中的内容加解密
import base64
import random
import string
import struct
import hashlib
import time
import socket
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend

class XMLParse:
    """提供提取消息格式中的密文及生成回复消息格式的接口"""
    def extract(self, xmltext):
        """提取出xml数据包中的加密消息"""
        try:
            import xml.etree.cElementTree as ET
            root = ET.fromstring(xmltext)
            msg_encrypt = root.find("Encrypt").text
            return 0, msg_encrypt
        except Exception:
            return -40002, None

    def generate(self, encrypt, signature, timestamp, nonce):
        """生成xml格式的回复消息"""
        resp_dict = {
            "Encrypt": encrypt,
            "MsgSignature": signature,
            "TimeStamp": timestamp,
            "Nonce": nonce,
        }
        resp_xml = "<xml><Encrypt><![CDATA[%(Encrypt)s]]></Encrypt><MsgSignature><![CDATA[%(MsgSignature)s]]></MsgSignature><TimeStamp>%(TimeStamp)s</TimeStamp><Nonce><![CDATA[%(Nonce)s]]></Nonce></xml>"
        return resp_xml % resp_dict

class PKCS7Encoder():
    """提供基于PKCS7算法的加解密接口"""
    def __init__(self, k=32):
        self.k = k

    def encode(self, text):
        """对需要加密的明文进行填充"""
        import binascii
        text_length = len(text)
        amount_to_pad = self.k - (text_length % self.k)
        if amount_to_pad == 0:
            amount_to_pad = self.k
        pad = chr(amount_to_pad)
        return text + (pad * amount_to_pad).encode('utf-8')

    def decode(self, decrypted):
        """删除解密后明文的填充部分"""
        pad = decrypted[-1]
        if pad < 1 or pad > 32:
            pad = 0
        return decrypted[:-pad]

class Prpcrypt:
    """提供接收和推送给公众平台消息的加解密接口"""
    def __init__(self, key):
        self.key = key
        self.mode = modes.CBC(key[:16]) # 这里企微固定用 IV 为 key 的前16字节

    def encrypt(self, text, corpid):
        """对明文进行加密"""
        # 16位随机字符串 + 4位正文长度 + 正文 + corpid
        random_str = ''.join(random.sample(string.ascii_letters + string.digits, 16)).encode('utf-8')
        text_bytes = text.encode('utf-8')
        text_len = struct.pack("I", socket.htonl(len(text_bytes)))
        corpid_bytes = corpid.encode('utf-8')
        
        raw_text = random_str + text_len + text_bytes + corpid_bytes
        # 补码
        pkcs7 = PKCS7Encoder()
        raw_text = pkcs7.encode(raw_text)
        
        # 加密
        backend = default_backend()
        cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.key[:16]), backend=backend)
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(raw_text) + encryptor.finalize()
        return 0, base64.b64encode(ciphertext).decode('utf-8')

    def decrypt(self, ciphertext, corpid):
        """对密文进行解密"""
        try:
            backend = default_backend()
            cipher = Cipher(algorithms.AES(self.key), modes.CBC(self.key[:16]), backend=backend)
            decryptor = cipher.decryptor()
            plain_text = decryptor.update(base64.b64decode(ciphertext)) + decryptor.finalize()
        except Exception:
            return -40007, None

        pkcs7 = PKCS7Encoder()
        plain_text = pkcs7.decode(plain_text)
        
        # 去掉随机字符串
        content = plain_text[16:]
        xml_len = struct.unpack("I", content[:4])[0]
        xml_len = socket.ntohl(xml_len)
        xml_content = content[4:xml_len+4].decode('utf-8')
        from_corpid = content[xml_len+4:].decode('utf-8')
        
        if from_corpid != corpid:
            return -40005, None
        return 0, xml_content

import socket
class WXBizMsgCrypt:
    def __init__(self, token, encodingAesKey, corpid):
        self.token = token
        self.corpid = corpid
        try:
            self.key = base64.b64decode(encodingAesKey + "=")
        except Exception:
            raise Exception("EncodingAESKey Error")

    def VerifyURL(self, sMsgSignature, sTimeStamp, sNonce, sEchoStr):
        """验证URL回调请求"""
        sha1 = hashlib.sha1()
        data = sorted([self.token, sTimeStamp, sNonce, sEchoStr])
        sha1.update(''.join(data).encode('utf-8'))
        hash_res = sha1.hexdigest()
        
        if hash_res != sMsgSignature:
            return -40001, None
            
        pc = Prpcrypt(self.key)
        ret, sReplyEchoStr = pc.decrypt(sEchoStr, self.corpid)
        return ret, sReplyEchoStr

    def DecryptMsg(self, sPostData, sMsgSignature, sTimeStamp, sNonce):
        """解密企微推送的消息"""
        xml_parse = XMLParse()
        ret, encrypt = xml_parse.extract(sPostData)
        if ret != 0:
            return ret, None
            
        sha1 = hashlib.sha1()
        data = sorted([self.token, sTimeStamp, sNonce, encrypt])
        sha1.update(''.join(data).encode('utf-8'))
        if sha1.hexdigest() != sMsgSignature:
            return -40001, None
            
        pc = Prpcrypt(self.key)
        ret, xml_content = pc.decrypt(encrypt, self.corpid)
        return ret, xml_content
