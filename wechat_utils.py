import requests
import json
import time
import os

class WeChatUtils:
    def __init__(self, config):
        self.config = config.get("wechat", {})
        self.corpid = self.config.get("corpid")
        self.corpsecret = self.config.get("corpsecret")
        self.agentid = self.config.get("agentid")
        self.access_token = None
        self.token_expiry = 0

    def get_access_token(self):
        """获取或刷新 Access Token"""
        if self.access_token and time.time() < self.token_expiry:
            return self.access_token

        url = f"https://qyapi.weixin.qq.com/cgi-bin/gettoken?corpid={self.corpid}&corpsecret={self.corpsecret}"
        resp = requests.get(url).json()
        if resp.get("errcode") == 0:
            self.access_token = resp.get("access_token")
            # 提前 5 分钟刷新
            self.token_expiry = time.time() + resp.get("expires_in") - 300
            return self.access_token
        else:
            print(f"获取 Access Token 失败: {resp}")
            return None

    def upload_file(self, file_path):
        """上传文件到企微临时素材库，返回 media_id"""
        token = self.get_access_token()
        if not token:
            return None

        url = f"https://qyapi.weixin.qq.com/cgi-bin/media/upload?access_token={token}&type=file"
        file_name = os.path.basename(file_path)
        with open(file_path, 'rb') as f:
            files = {'file': (file_name, f)}
            resp = requests.post(url, files=files).json()
            
        if resp.get("errcode") == 0:
            return resp.get("media_id")
        else:
            print(f"上传文件失败: {resp}")
            return None

    def send_file(self, user_id, media_id):
        """发送文件消息给指定用户"""
        token = self.get_access_token()
        if not token:
            return False

        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
        data = {
            "touser": user_id,
            "msgtype": "file",
            "agentid": self.agentid,
            "file": {
                "media_id": media_id
            },
            "safe": 0
        }
        resp = requests.post(url, json=data).json()
        if resp.get("errcode") == 0:
            return True
        else:
            print(f"发送消息失败: {resp}")
            return False

    def send_text(self, user_id, content):
        """发送文本消息给指定用户"""
        token = self.get_access_token()
        if not token:
            return False

        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
        data = {
            "touser": user_id,
            "msgtype": "text",
            "agentid": self.agentid,
            "text": {
                "content": content
            },
            "safe": 0
        }
        resp = requests.post(url, json=data).json()
        return resp.get("errcode") == 0

    def send_markdown(self, user_id, content):
        """发送 Markdown 格式文本给指定用户"""
        token = self.get_access_token()
        if not token:
            return False

        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
        data = {
            "touser": user_id,
            "msgtype": "markdown",
            "agentid": self.agentid,
            "markdown": {
                "content": content
            },
            "safe": 0
        }
        try:
            resp = requests.post(url, json=data).json()
            if resp.get("errcode") != 0:
                print(f"❌ 企微发送 Markdown 失败: {resp}")
            return resp.get("errcode") == 0
        except Exception as e:
            print(f"❌ 企微发送 Markdown 异常: {e}")
            return False
