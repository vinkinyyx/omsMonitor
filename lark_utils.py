import requests
import json
import time
import os

class LarkUtils:
    def __init__(self, config):
        self.config = config.get("lark", {})
        self.app_id = self.config.get("app_id")
        self.app_secret = self.config.get("app_secret")
        self.tenant_access_token = None
        self.token_expiry = 0

    def get_tenant_access_token(self):
        """获取或刷新 Tenant Access Token"""
        if self.tenant_access_token and time.time() < self.token_expiry:
            return self.tenant_access_token

        url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
        headers = {"Content-Type": "application/json; charset=utf-8"}
        payload = {
            "app_id": self.app_id,
            "app_secret": self.app_secret
        }
        try:
            resp = requests.post(url, headers=headers, json=payload).json()
            if resp.get("code") == 0:
                self.tenant_access_token = resp.get("tenant_access_token")
                # 提前 5 分钟刷新
                self.token_expiry = time.time() + resp.get("expire") - 300
                return self.tenant_access_token
            else:
                print(f"获取飞书 Access Token 失败: {resp}")
        except Exception as e:
            print(f"获取飞书 Access Token 出错: {e}")
        return None

    def upload_file(self, file_path):
        """上传文件到飞书，返回 file_key"""
        token = self.get_tenant_access_token()
        if not token:
            return None

        # 💡 改进1：增加空文件拦截。飞书严禁上传 0 字节文件，否则直接报 234001 错误
        if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            print(f"⚠️ 文件不存在或大小为0，已跳过上传: {file_path}")
            return None

        url = "https://open.feishu.cn/open-apis/im/v1/files"
        file_name = os.path.basename(file_path)

        headers = {
            "Authorization": f"Bearer {token}"
        }

        try:
            with open(file_path, 'rb') as f:
                # 💡 改进2：统一使用 "stream"。这样飞书会将其视为普通附件，
                # 避免你传 .xlsx 却声明为 xls 导致的格式严格校验失败。
                data = {
                    "file_type": "stream",
                    "file_name": file_name
                }
                files = {
                    "file": (file_name, f, "application/octet-stream")
                }
                resp = requests.post(url, headers=headers, data=data, files=files).json()

            if resp.get("code") == 0:
                return resp.get("data", {}).get("file_key")
            else:
                print(f"❌ 飞书上传文件失败: {resp}")
        except Exception as e:
            print(f"❌ 飞书上传文件异常: {e}")
        return None

    def send_file(self, receive_id, file_key, receive_id_type="open_id"):
        """发送文件消息给指定接收者"""
        token = self.get_tenant_access_token()
        if not token:
            return False

        url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        payload = {
            "receive_id": receive_id,
            "msg_type": "file",
            "content": json.dumps({"file_key": file_key})
        }
        try:
            resp = requests.post(url, headers=headers, json=payload).json()
            # 💡 改进3：增加错误打印，一旦没权限或参数错，控制台立马现身
            if resp.get("code") != 0:
                print(f"❌ 飞书发送文件消息失败: {resp}")
            return resp.get("code") == 0
        except Exception as e:
            print(f"❌ 飞书发送文件异常: {e}")
            return False

    def send_text(self, receive_id, text, receive_id_type="open_id"):
        """发送文本消息给指定接收者"""
        token = self.get_tenant_access_token()
        if not token:
            return False

        url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }
        payload = {
            "receive_id": receive_id,
            "msg_type": "text",
            "content": json.dumps({"text": text})
        }
        try:
            resp = requests.post(url, headers=headers, json=payload).json()
            # 💡 改进3：同上
            if resp.get("code") != 0:
                print(f"❌ 飞书发送文本消息失败: {resp}")
            return resp.get("code") == 0
        except Exception as e:
            print(f"❌ 飞书发送文本异常: {e}")
            return False

    def send_markdown_card(self, receive_id, md_text, title="日志巡检报告", template="red", receive_id_type="open_id"):
        """使用飞书消息卡片发送富文本，带彩色标题栏"""
        token = self.get_tenant_access_token()
        if not token:
            return False

        url = f"https://open.feishu.cn/open-apis/im/v1/messages?receive_id_type={receive_id_type}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json; charset=utf-8"
        }

        # 构造飞书卡片结构，增加彩色 Header
        card_content = {
            "config": {
                "wide_screen_mode": True
            },
            "header": {
                "template": template,
                "title": {
                    "content": title,
                    "tag": "plain_text"
                }
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": md_text
                }
            ]
        }

        payload = {
            "receive_id": receive_id,
            "msg_type": "interactive",
            "content": json.dumps(card_content)
        }

        try:
            resp = requests.post(url, headers=headers, json=payload).json()
            if resp.get("code") != 0:
                print(f"❌ 飞书发送卡片失败: {resp}")
            return resp.get("code") == 0
        except Exception as e:
            print(f"❌ 飞书发送卡片异常: {e}")
            return False