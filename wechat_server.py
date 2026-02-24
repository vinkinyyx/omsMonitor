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
import re

app = FastAPI()

# 简单的本地防重缓存 (Message ID -> 时间戳)
processed_msg_ids = {}

# 企微多轮对话状态机，结构：{ user_id: {"step": "START_DATE", "retries": 0, "data": {}} }
wechat_sessions = {}

def clean_expired_msg_ids():
    current_time = time.time()
    # 清理 5 分钟前的记录
    expired_keys = [k for k, v in processed_msg_ids.items() if current_time - v > 300]
    for k in expired_keys:
        del processed_msg_ids[k]

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

def run_inspection_and_reply(user_id: str, user_data: dict = None):
    """后台执行巡检并发送实时进度及文件"""
    print(f"开始为用户 {user_id} 执行巡检任务...")
    utils.send_text(user_id, "收到指令，正在启动日志巡检，请稍候...")
    
    # 初始化日志目录
    os.makedirs("logs", exist_ok=True)
    task_time = time.strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join("logs", f"task_{task_time}.txt")
    
    try:
        env = os.environ.copy()
        if user_data:
            env["DYNAMIC_PARAMS"] = json.dumps(user_data)

        # 使用 Popen 以便实时获取 stdout
        process = subprocess.Popen(
            [sys.executable, "main.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            env=env,
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
        excel_path = "error_logs.xlsx"
        
        # 1. 企微发送原生 Markdown 战报卡片
        summary_path = "report_summary.md"
        if os.path.exists(summary_path):
            try:
                with open(summary_path, "r", encoding="utf-8") as f:
                    md_content = f.read()
                # 企微软原生不支持带颜色的 config，但原生支持 markdown 格式解析
                utils.send_markdown(user_id, md_content)
            except Exception as e:
                print(f"读取或发送摘要战报失败: {e}")
        else:
            utils.send_text(user_id, "巡检已执行完毕。")

        # 2. 如果有文件，作为附件紧跟着发送
        if os.path.exists(excel_path):
            media_id_xls = utils.upload_file(excel_path)
            if media_id_xls:
                utils.send_file(user_id, media_id_xls)
            else:
                utils.send_text(user_id, "⚠️ 巡检已完成，但长篇 Excel 报告上传企微临时素材库失败。")
            
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
            # 拿到 MsgId 防重
            msg_id = root.find("MsgId").text
            
            clean_expired_msg_ids()
            if msg_id in processed_msg_ids:
                return "success"
            processed_msg_ids[msg_id] = time.time()

            content = root.find("Content").text.strip()
            print(f"收到用户 {user_id} 的消息: {content}")
            
            # --- 多轮会话路由 ---
            if user_id in wechat_sessions:
                session = wechat_sessions[user_id]
                step = session["step"]
                
                # 用户主动取消
                if content.lower() in ['取消', '取消巡检', 'cancel', '退出']:
                    del wechat_sessions[user_id]
                    utils.send_text(user_id, "已为您取消本次巡检引导。")
                    return "success"
                    
                if step == "START_DATE":
                    if content.lower() in ["无", "跳过", "今天"]:
                        session["data"]["start_date"] = None
                        session["step"] = "END_DATE"
                        session["retries"] = 0
                        utils.send_text(user_id, "✅ 已跳过开始日期。请回复「结束日期」(如: 2026-02-15)。若不需要，请回复「无」或「跳过」:")
                    else:
                        parsed = parse_date(content)
                        if parsed:
                            session["data"]["start_date"] = parsed
                            session["step"] = "END_DATE"
                            session["retries"] = 0
                            utils.send_text(user_id, f"✅ 已记录开始日期为 {parsed}。请回复「结束日期」(如: 2026-02-15)。若不需要，请回复「无」或「跳过」:")
                        else:
                            session["retries"] += 1
                            if session["retries"] >= 3:
                                del wechat_sessions[user_id]
                                utils.send_text(user_id, "❌ 多次输入错误，为防止卡死，已自动退出巡检引导。请重新输入“巡检”唤起。")
                            else:
                                utils.send_text(user_id, "⚠️ 日期格式无法被系统识别，请按照「YYYY-MM-DD」或者「20260212」格式重新输入！(若想退出请回复 取消)")
                
                elif step == "END_DATE":
                    if content.lower() in ["无", "跳过", "今天"]:
                        session["data"]["end_date"] = None
                        session["step"] = "STATUS"
                        session["retries"] = 0
                        utils.send_text(user_id, "✅ 已跳过结束日期。\n请回复想要查询的「状态」：\n- 回复 `1` (或 `报错`): 只查询报错记录 (推荐)\n- 回复 `0` (或 `成功`): 只查询成功记录\n- 回复 `2` (或 `全部`): 拉取所有请求并在本地过滤")
                    else:
                        parsed = parse_date(content)
                        if parsed:
                            session["data"]["end_date"] = parsed
                            session["step"] = "STATUS"
                            session["retries"] = 0
                            utils.send_text(user_id, f"✅ 已记录结束日期为 {parsed}。\n请回复想要查询的「状态」：\n- 回复 `1` (或 `报错`): 只查询报错记录 (推荐)\n- 回复 `0` (或 `成功`): 只查询成功记录\n- 回复 `2` (或 `全部`): 拉取所有请求并在本地过滤")
                        else:
                            session["retries"] += 1
                            if session["retries"] >= 3:
                                del wechat_sessions[user_id]
                                utils.send_text(user_id, "❌ 多次输入错误，由于安全策略，已自动退出向导。")
                            else:
                                utils.send_text(user_id, "⚠️ 日期格式无法被系统识别，请正确如 2026-02-28 格式重新输入:")
                
                elif step == "STATUS":
                    status_map = {"1": "1", "报错": "1", "0": "0", "成功": "0", "2": "2", "全部": "2"}
                    if content in status_map:
                        session["data"]["status"] = status_map[content]
                        session["step"] = "FLOW"
                        session["retries"] = 0
                        
                        flows = config.get("integration_flows", ["所有"])
                        flow_str = "\n".join([f"- {i+1}. {name}" for i, name in enumerate(flows)])
                        utils.send_text(user_id, f"✅ 已确认状态过滤级别。\n最后一步，请告诉我您监控的「集成流」要求：\n您可以直接输入集成流名称关键词或下方序号，如果不需要过滤请回复「所有」或数字「1」:\n{flow_str}")
                    else:
                        session["retries"] += 1
                        if session["retries"] >= 3:
                            del wechat_sessions[user_id]
                            utils.send_text(user_id, "❌ 多次输入错误，向导已退出。")
                        else:
                            utils.send_text(user_id, "⚠️ 无法识别。请明确回复数字 `1` (报错) 或 `2` (全部):")
                
                elif step == "FLOW":
                    flows = config.get("integration_flows", ["所有"])
                    selected_flow = "所有"
                    
                    if content.isdigit() and 1 <= int(content) <= len(flows):
                        selected_flow = flows[int(content) - 1]
                    else:
                        selected_flow = content
                    
                    session["data"]["integration_flow"] = selected_flow
                    user_params = session["data"]
                    del wechat_sessions[user_id]
                    
                    utils.send_text(user_id, f"✅ 设定完毕！参数打包成功！引擎正在以此规则为您拉起无头浏览器...")
                    background_tasks.add_task(run_inspection_and_reply, user_id, user_params)
            else:
                if content in ["开始巡检", "巡检", "run"]:
                    # 开启新的会话状态
                    wechat_sessions[user_id] = {"step": "START_DATE", "retries": 0, "data": {}}
                    utils.send_text(user_id, "收到指令。请回复您需要查询的「开始日期」(支持格式如 2026/02/12, 2026-02-12, 或 20260212)。若不需要指定开始日期(查询当天)，请回复「无」或「跳过」:")
                else:
                    utils.send_text(user_id, "⚠️ 未在巡检向导中。请向我发送「巡检」、「开始巡检」或「run」中的任意一个指令以启动交互向导。")
        
    return "success"

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8080))
    print(f"微信回调服务已启动在端口: {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)
