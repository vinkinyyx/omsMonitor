from fastapi import FastAPI, Request, Query, BackgroundTasks
from fastapi.responses import PlainTextResponse
import uvicorn
import json
import os
import xml.etree.ElementTree as ET
from wechat_msg_crypt import WXBizMsgCrypt
from wechat_utils import WeChatUtils
import subprocess
import time
import sys
from datetime import datetime

app = FastAPI()

def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

config = load_config()
w_config = config.get("wechat", {})
crypt = WXBizMsgCrypt(
    token=w_config.get("token"),
    encodingAesKey=w_config.get("aes_key"),
    corpid=w_config.get("corpid")
)
utils = WeChatUtils(config)

@app.get("/wechat", response_class=PlainTextResponse)
async def verify_url(
    msg_signature: str = Query(...),
    timestamp: str = Query(...),
    nonce: str = Query(...),
    echostr: str = Query(...)
):
    """验证 URL (企微后台设置时使用)"""
    print(f"收到企微验证请求: signature={msg_signature}, timestamp={timestamp}, nonce={nonce}")
    ret, sEchoStr = crypt.VerifyURL(msg_signature, timestamp, nonce, echostr)
    if ret == 0:
        print("URL 验证成功，正在返回 echostr...")
        return sEchoStr
    print(f"URL 验证失败，错误代码: {ret}")
    return "error"

def run_inspection_and_reply(user_id: str):
    """后台执行巡检并发送实时进度及文件"""
    print(f"开始为用户 {user_id} 执行巡检任务...")
    utils.send_text(user_id, "🚀 收到指令，正在启动日志巡检...")
    
    # 初始化日志目录
    os.makedirs("logs", exist_ok=True)
    task_time = time.strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join("logs", f"task_{task_time}.txt")
    
    try:
        # 使用 Popen 以便实时获取 stdout
        process = subprocess.Popen(
            [sys.executable, "main.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            bufsize=1
        )
        
        with open(log_file_path, "w", encoding="utf-8") as lf:
            for line in iter(process.stdout.readline, ''):
                clean_line = line.strip()
                if not clean_line:
                    continue
                
                # 写入本地备份日志，增加时间戳前缀
                current_time_str = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
                lf.write(f"{current_time_str} {clean_line}\n")
                lf.flush()
                
                # 只有匹配 [PROGRESS] 标签的行才发给用户
                if "[PROGRESS]" in clean_line:
                    msg = clean_line.replace("[PROGRESS]", "").strip()
                    utils.send_text(user_id, f"📌 {msg}")
                
                # 同时在服务端控制台打印所有输出
                print(clean_line)
            
        process.stdout.close()
        process.wait()
        
        excel_path = "error_logs.xlsx"
        txt_path = "error_logs.txt"
        
        has_sent_file = False
        
        if os.path.exists(txt_path):
            media_id_txt = utils.upload_file(txt_path)
            if media_id_txt:
                utils.send_file(user_id, media_id_txt)
                has_sent_file = True

        if os.path.exists(excel_path):
            media_id_xls = utils.upload_file(excel_path)
            if media_id_xls:
                utils.send_file(user_id, media_id_xls)
                has_sent_file = True

        # ===== 追加发送摘要战报逻辑 =====
        summary_path = "report_summary.md"
        if os.path.exists(summary_path):
            try:
                with open(summary_path, "r", encoding="utf-8") as rf:
                    summary_content = rf.read()
                # 企微直接通过文本渠道推送生成的 markdown 内容（企微会部分自动解析）
                utils.send_text(user_id, summary_content)
            except Exception as e:
                print(f"读取或发送摘要战报失败: {e}")
                
        if has_sent_file:
            utils.send_text(user_id, "✅ 巡检完成，以上是为您自动生成的日志与报表文件。")
        else:
            utils.send_text(user_id, "ℹ️ 巡检完成，但由于系统环境原因，未找到可发送的报告文件。")
            
    except Exception as e:
        print(f"执行巡检出错: {e}")
        utils.send_text(user_id, f"❌ 执行任务时发生错误: {str(e)}")

@app.post("/wechat")
async def handle_message(
    request: Request,
    background_tasks: BackgroundTasks,
    msg_signature: str = Query(...),
    timestamp: str = Query(...),
    nonce: str = Query(...)
):
    """处理用户发送的消息"""
    body = await request.body()
    ret, xml_content = crypt.DecryptMsg(body.decode('utf-8'), msg_signature, timestamp, nonce)
    
    if ret == 0:
        root = ET.fromstring(xml_content)
        user_id = root.find("FromUserName").text
        msg_type = root.find("MsgType").text
        
        if msg_type == "text":
            content = root.find("Content").text.strip()
            print(f"收到用户 {user_id} 的消息: {content}")
            
            if content in ["开始巡检", "巡检", "run"]:
                # 使用 BackgroundTasks 异步运行，避免请求超时
                background_tasks.add_task(run_inspection_and_reply, user_id)
                return "success"
            else:
                utils.send_text(user_id, "⚠️ 输入指令有误，请向我发送「巡检」、「开始巡检」或「run」中的任意一个指令以启动巡检向导。")
        
    return "success"

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    print(f"微信回调服务已启动在端口: {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
