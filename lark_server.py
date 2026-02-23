from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
import json
import os
import subprocess
from lark_utils import LarkUtils
import hashlib
import base64
from Crypto.Cipher import AES
import sys
import time
import re
from datetime import datetime

app = FastAPI()

# 简单的本地防重缓存 (Message ID -> 时间戳)
# 作用：飞书在 3s 内未收到 200/0 的响应会重试，这会导致同一个指令触发多次巡检
processed_msg_ids = {}

# 飞书多轮对话状态机，结构：{ open_id: {"step": "START_DATE", "retries": 0, "data": {}} }
lark_sessions = {}

def clean_expired_msg_ids():
    current_time = time.time()
    # 清理 5 分钟前的记录
    expired_keys = [k for k, v in processed_msg_ids.items() if current_time - v > 300]
    for k in expired_keys:
        del processed_msg_ids[k]

class AESCipher(object):
    def __init__(self, key):
        self.bs = AES.block_size
        self.key = hashlib.sha256(key.encode('utf-8')).digest()

    def decrypt(self, enc):
        enc = base64.b64decode(enc)
        iv = enc[:AES.block_size]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)
        decrypted_bytes = cipher.decrypt(enc[AES.block_size:])
        # PKCS7 反填充
        pad_len = decrypted_bytes[-1]
        return decrypted_bytes[:-pad_len].decode('utf-8')

def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

config = load_config()
lark_config = config.get("lark", {})
utils = LarkUtils(config)

def parse_date(date_str):
    """尝试容错解析用户输入的日期，转为 YYYY-MM-DD，并且校验真实性"""
    date_str = date_str.strip().replace(" ", "").replace("/", "-")
    # 匹配 20260226 (纯数字)
    if re.match(r"^\d{8}$", date_str):
        date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    
    try:
        # 使用 datetime 强制转换，既能自动补齐 2026-2-2，也能过滤如 2026-13-40 这类非法日期
        valid_date = datetime.strptime(date_str, "%Y-%m-%d")
        return valid_date.strftime("%Y-%m-%d")
    except ValueError:
        return None


def run_inspection_and_reply_lark(receive_id: str, user_data: dict = None, receive_id_type: str = "open_id"):
    """后台执行巡检并实时播报进度，最终发送卡片与文件"""
    print(f"开始为飞书用户 {receive_id} 执行巡检任务...")
    utils.send_text(receive_id, "收到指令，正在启动日志巡检，请稍候...", receive_id_type=receive_id_type)

    try:
        env = os.environ.copy()
        if user_data:
            env["DYNAMIC_PARAMS"] = json.dumps(user_data)

        # ================= 核心升级：流式读取实时日志 =================
        # 使用 Popen 替代 run，这样程序就可以边执行边输出
        process = subprocess.Popen(
            [sys.executable, "main.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # 将报错信息也合并进来防止漏看
            text=True,
            encoding='utf-8',
            env=env,
            bufsize=1  # 开启行缓冲，确保日志 0 延迟推送
        )

        # 实时逐行读取 main.py 的控制台打印
        for line in iter(process.stdout.readline, ''):
            clean_line = line.strip()
            if not clean_line:
                continue

            print(f"巡检控制台: {clean_line}")  # 依然保留在本地服务端的终端显示

            # 🎯 拦截带有 [PROGRESS] 标记的日志，立刻发射给飞书
            if "[PROGRESS]" in clean_line:
                # 稍微美化一下，把冰冷的 [PROGRESS] 替换成小图标，让气泡更好看
                display_text = clean_line.replace("[PROGRESS]", "🚀").strip()
                utils.send_text(receive_id, display_text, receive_id_type=receive_id_type)

        process.stdout.close()
        process.wait()  # 等待脚本彻底运行完毕
        # ==============================================================

        md_path = "report_summary.md"
        excel_path = "error_logs.xlsx"

        # 1. 优先发送炫酷的可视化卡片
        if os.path.exists(md_path):
            with open(md_path, "r", encoding="utf-8") as f:
                md_content = f.read()

            if "🎉" in md_content:
                utils.send_markdown_card(receive_id, md_content, title="✅ 巡检正常", template="green",
                                         receive_id_type=receive_id_type)
            else:
                utils.send_markdown_card(receive_id, md_content, title="🚨 异常报警", template="red",
                                         receive_id_type=receive_id_type)
        else:
            utils.send_text(receive_id, "巡检已执行完毕。", receive_id_type=receive_id_type)

        # 2. 如果有文件，作为附件紧跟着发送
        if os.path.exists(excel_path):
            file_key = utils.upload_file(excel_path)
            if file_key:
                utils.send_file(receive_id, file_key, receive_id_type=receive_id_type)
            else:
                utils.send_text(receive_id, "⚠️ 巡检已完成，但长篇 Excel 报告上传失败。", receive_id_type=receive_id_type)

    except Exception as e:
        print(f"飞书平台执行巡检出错: {e}")
        utils.send_text(receive_id, f"执行巡检时发生致命错误: {str(e)}", receive_id_type=receive_id_type)

@app.post("/lark")
async def handle_lark_event(request: Request, background_tasks: BackgroundTasks):
    """处理飞书事件订阅回调"""
    body = await request.json()

    # === 核心新增：自动剥开飞书的加密外壳 ===
    encrypt_data = body.get("encrypt")
    if encrypt_data:
        encrypt_key = lark_config.get("encrypt_key")
        try:
            cipher = AESCipher(encrypt_key)
            decrypted_str = cipher.decrypt(encrypt_data)
            # 解密成功后，用真实的明文数据替换掉加密外壳
            body = json.loads(decrypted_str)
        except Exception as e:
            print(f"解密飞书消息失败: {e}")
            return {"code": 1, "msg": "decrypt failed"}
    # =======================================

    # 1. 处理 Url Verification
    if body.get("type") == "url_verification":
        return {"challenge": body.get("challenge")}

    # 2. 获取事件头和内容
    header = body.get("header", {})
    event = body.get("event", {})

    # 鉴权校验
    v_token = lark_config.get("verification_token")
    if v_token and header.get("token") != v_token:
        print(f"飞书事件校验 Token 不匹配！预期:{v_token}, 实际:{header.get('token')}")
        return {"code": 1, "msg": "invalid token"}

    event_type = header.get("event_type")

    # 3. 处理“接收消息”事件
    if event_type == "im.message.receive_v1":
        message = event.get("message", {})
        sender = event.get("sender", {})

        if message.get("message_type") == "text":
            try:
                content_json = json.loads(message.get("content"))
                text = content_json.get("text", "").strip()
                open_id = sender.get("sender_id", {}).get("open_id")
                message_id = message.get("message_id")
                
                # 去重校验
                if message_id:
                    clean_expired_msg_ids()
                    if message_id in processed_msg_ids:
                        print(f"检测到飞书重推的消息 {message_id}，已忽略")
                        return {"code": 0, "msg": "ok"}
                    processed_msg_ids[message_id] = time.time()

                print(f"成功解密并收到消息: [{text}] 来自用户: {open_id}")

                # 判断是否为重新触发
                if text in ["巡检", "开始巡检", "run"]:
                    # 开辟新的会话状态
                    lark_sessions[open_id] = {"step": "START_DATE", "retries": 0, "data": {}}
                    utils.send_text(open_id, "已收到指令。请回复「开始日期」 (支持 2026/02/12、2026-02-12 或 20260212 格式):")
                    return {"code": 0, "msg": "ok"}
                
                # 如果当前存在上下话会话，处理参数收集
                if open_id in lark_sessions:
                    session = lark_sessions[open_id]
                    step = session["step"]
                    
                    if step == "START_DATE":
                        p_date = parse_date(text)
                        if p_date:
                            session["data"]["start_date"] = p_date
                            session["step"] = "END_DATE"
                            session["retries"] = 0
                            utils.send_text(open_id, f"✅ 已记录开始日期: {p_date}\n请回复「结束日期」 (格式同上):")
                        else:
                            session["retries"] += 1
                            if session["retries"] >= 3:
                                del lark_sessions[open_id]
                                utils.send_text(open_id, "❌ 错误次数超过 3 次，已取消本次巡检创建。")
                            else:
                                utils.send_text(open_id, "⚠️ 日期格式不正确，请重新回复「开始日期」:")
                                
                    elif step == "END_DATE":
                        p_date = parse_date(text)
                        if p_date:
                            session["data"]["end_date"] = p_date
                            session["step"] = "STATUS"
                            session["retries"] = 0
                            utils.send_text(open_id, f"✅ 已记录结束日期: {p_date}\n请回复排查「状态」\n(填写: 0 代表成功, 1 代表失败, 2 代表所有状态):")
                        else:
                            session["retries"] += 1
                            if session["retries"] >= 3:
                                del lark_sessions[open_id]
                                utils.send_text(open_id, "❌ 错误次数超过 3 次，已取消本次巡检创建。")
                            else:
                                utils.send_text(open_id, "⚠️ 日期格式不正确，请重新回复「结束日期」:")
                                
                    elif step == "STATUS":
                        status_val = str(text.strip())
                        if status_val in ["0", "1", "2"]:
                            session["data"]["status"] = int(status_val)
                            session["step"] = "FLOW"
                            session["retries"] = 0
                            
                            # 获取配置中的流选项，预备菜单
                            flow_opts = config.get("integration_flows", ["所有"])
                            opts_str = "\n".join([f"{i}. {opt}" for i, opt in enumerate(flow_opts, 1)])
                            utils.send_text(open_id, f"✅ 已记录状态过滤。\n请回复「集成流选单对应的编号」：\n{opts_str}")
                        else:
                            session["retries"] += 1
                            if session["retries"] >= 3:
                                del lark_sessions[open_id]
                                utils.send_text(open_id, "❌ 错误次数超过 3 次，已取消本次巡检创建。")
                            else:
                                utils.send_text(open_id, "⚠️ 状态不正确 (必须是 0 或 1 或 2)，请重试:")
                                
                    elif step == "FLOW":
                        flow_opts = config.get("integration_flows", ["所有"])
                        try:
                            choice = int(text.strip())
                            if 1 <= choice <= len(flow_opts):
                                selected_flow = flow_opts[choice - 1]
                                session["data"]["integration_flow"] = selected_flow
                                user_data = session["data"].copy()
                                del lark_sessions[open_id]
                                
                                # 将完整动态参数送入真正的巡检后台进程执行
                                background_tasks.add_task(run_inspection_and_reply_lark, open_id, user_data)
                            else:
                                raise ValueError("超出选项范围")
                        except ValueError:
                            session["retries"] += 1
                            if session["retries"] >= 3:
                                del lark_sessions[open_id]
                                utils.send_text(open_id, "❌ 错误次数超过 3 次，已取消本次巡检创建。")
                            else:
                                utils.send_text(open_id, "⚠️ 输入不是有效的菜单编号，请重新输入:")
                    
                    return {"code": 0, "msg": "ok"}
                else:
                    # 如果发了别的且不在会话中，提示下
                    utils.send_text(open_id, "⚠️ 输入指令有误，请向我发送「巡检」、「开始巡检」或「run」中的任意一个指令以启动巡检向导。")
                    
            except Exception as e:
                print(f"解析飞书消息内容或处理状态机时失败: {e}")

    return {"code": 0, "msg": "ok"}

if __name__ == "__main__":
    # 飞书服务默认启动在 8081 端口，以防与企微 8080 冲突
    port = int(os.getenv("LARK_PORT", 8081))
    print(f"飞书回调服务已启动在端口: {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
