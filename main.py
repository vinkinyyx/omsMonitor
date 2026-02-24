import os
import json
import io
import pandas as pd
from playwright.sync_api import sync_playwright
from datetime import datetime

def load_config() -> dict:
    conf = {}
    try:
        with open("config.json", "r", encoding="utf-8") as f:
            conf = json.load(f)
    except Exception as e:
        print(f"读取配置文件 config.json 失败 (或不存在)，将使用默认值: {e}")

    dy_params = os.getenv("DYNAMIC_PARAMS")
    if dy_params:
        try:
            conf.update(json.loads(dy_params))
        except Exception as e:
            print(f"解析 DYNAMIC_PARAMS 取覆盖配置失败: {e}")
            
    return conf

def scrape_logs(config: dict) -> str:
    """
    使用 Playwright 抓取异常日志文本
    """
    print("[PROGRESS] 正在启动浏览器环境...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        try:
            # 1. 打开登录页
            url = config.get("url")
            if not url:
                raise ValueError("请在 config.json 或环境变量中配置 url")
            page.goto(url, wait_until="networkidle")
            
            # 2. 登录流程
            print("[PROGRESS] 系统登录中...")
            
            # 等待 iframe 元素出现
            page.wait_for_selector("#yonbip_login_id, iframe[name='yonbip_login_id']", state="attached", timeout=30000)
            
            # 获取对应的 frame
            login_frame = page.frame(name="yonbip_login_id")
            if not login_frame:
                raise Exception("无法找到名为 yonbip_login_id 的 iframe")
                
            # 检查是否有普通登录可点击
            try:
                if login_frame.locator("li#toNormalLogin").is_visible():
                    login_frame.locator("li#toNormalLogin").click()
            except Exception:
                pass
                
            username = config.get("username")
            password = config.get("password")
            
            if not username or not password:
                raise ValueError("请在 config.json 或环境变量中配置 username 和 password")
            
            login_frame.locator("input#username").fill(username)
            login_frame.locator("input#password").fill(password)
            login_frame.locator("input#submit_btn_login").click()
            
            # 3. 进入系统主页并使用全局搜索
            print("[PROGRESS] 登录成功，正在进入系统主页...")
            search_input = page.locator("input.searchInput--1dPNm")
            search_input.wait_for(state="visible", timeout=30000)
            
            print("[PROGRESS] 正在搜索‘华瑭接口集成流日志’功能...")
            search_input.click()
            search_input.fill("华瑭接口集成流日志")
            
            page.wait_for_timeout(2000)
            page.keyboard.press("Enter")
            
            import time
            print("寻找搜索结果并点击...")
            clicked_result = False
            start_time = time.time()
            while time.time() - start_time < 60:
                for frame in page.frames:
                    try:
                        loc = frame.get_by_text("华瑭接口集成流日志").last
                        if loc.is_visible():
                            loc.click()
                            clicked_result = True
                            break
                    except:
                        continue
                if clicked_result:
                    break
                page.wait_for_timeout(1000)
            
            if not clicked_result:
                raise Exception("等待60秒仍无法在任一 iframe 中找到搜索结果文本。")

            # 4. 进入日志系统页
            print("[PROGRESS] 已进入日志页面，正在按配置筛选目标数据...")
            active_frame = None
            start_time = time.time()
            while time.time() - start_time < 60:
                for frame in page.frames:
                    try:
                        flow_loc = frame.locator("label[title='集成流'] + div input")
                        if flow_loc.count() > 0 and flow_loc.first.is_visible():
                            active_frame = frame
                            break
                    except:
                        continue
                if active_frame:
                    break
                page.wait_for_timeout(1000)
            
            if not active_frame:
                raise Exception("等待60秒仍无法在任一 iframe 中找到筛选输入框，加载超时。可能页面转圈时间过长。")
            
            # 等待确保主渲染区 iframe 加载完成后的动画/事件绑定
            page.wait_for_timeout(3000)
            
            intercepted_data = None
            def handle_route(route, request):
                if "report/refresh" in request.url:
                    current_url = request.url
                    print(f"--- 捕捉到数据请求 URL: {current_url[:150]}...")
                    
                    # 尝试强制改写分页参数
                    new_url = current_url
                    if "currPageSize=" in current_url:
                        # 替换现有的
                        import re
                        new_url = re.sub(r'currPageSize=\d+', 'currPageSize=1000', current_url)
                    else:
                        # 追加新的
                        connector = "&" if "?" in current_url else "?"
                        new_url = f"{current_url}{connector}currPageSize=1000"
                    
                    if new_url != current_url:
                        print(f"--- 拦截成功：已将分页规模调整为 1000")
                        route.continue_(url=new_url)
                    else:
                        route.continue_()
                else:
                    route.continue_()

            def handle_response(response):
                nonlocal intercepted_data
                if "report/refresh" in response.url:
                    status = response.status
                    print(f"收到 API 响应 (HTTP {status}): {response.url[:80]}...")
                    if status >= 400:
                        print(f"警告：API 请求失败，状态码 {status}。可能是由于修改 URL 参数导致签名失效。")
                        
                    if "application/json" in response.headers.get("content-type", ""):
                        try:
                            # 只有成功响应才尝试获取 JSON
                            if status == 200:
                                data = response.json()
                                # 只要包含 data 字段就视为潜在有效包
                                if "data" in data:
                                    intercepted_data = data
                                    print(f"[PROGRESS] 抓包验证：成功拦截 API 数据包", flush=True)
                        except Exception as e:
                            print(f"解析 JSON 响应出错: {e}")
            
            page.route("**/*", handle_route)
            page.on("response", handle_response)
            
            # --- 处理“创建时间”日期过滤 ---
            start_date = config.get("start_date")
            end_date = config.get("end_date")
            if start_date or end_date:
                try:
                    date_inputs = active_frame.locator("label[title='创建时间'] + div input")
                    if date_inputs.count() >= 2:
                        def set_date_robustly(input_locator, date_str, label):
                            input_locator.click()
                            page.wait_for_timeout(300)
                            # 全选并删除
                            page.keyboard.press("Control+A")
                            page.wait_for_timeout(100)
                            page.keyboard.press("Backspace")
                            page.wait_for_timeout(100)
                            # 逐字输入或直接 type (type 比 fill 更能触发布发事件)
                            input_locator.type(date_str, delay=50)
                            page.wait_for_timeout(300)
                            # 关键：必须回车以同步内部 UI State 到 “已选条件”
                            page.keyboard.press("Enter")
                            page.wait_for_timeout(500)
                            print(f"[PROGRESS] 网页验证：已填写{label}日期 {date_str}", flush=True)

                        if start_date:
                            set_date_robustly(date_inputs.nth(0), start_date, "开始")
                        if end_date:
                            set_date_robustly(date_inputs.nth(1), end_date, "结束")
                except Exception as e:
                    print(f"处理创建时间出错: {e}")
            
            # --- 处理“集成流”过滤 ---
            integration_flow = config.get("integration_flow", "所有")
            try:
                flow_input = active_frame.locator("label[title='集成流'] + div").locator("input").first
                flow_input.click()
                page.wait_for_timeout(500)
                
                if integration_flow == "所有":
                    flow_input.fill("")
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(500)
                else:
                    flow_input.fill(integration_flow)
                    page.wait_for_timeout(1000)
                    
                    # 尝试点击下拉选框内精确匹配的文本
                    item_flow = active_frame.locator("li.wui-select-item").get_by_text(integration_flow, exact=True)
                    if item_flow.count() > 0 and item_flow.first.is_visible():
                        item_flow.first.click()
                    else:
                        # 兜底
                        page.keyboard.press("Enter")
                    page.wait_for_timeout(500)
            except Exception as e:
                print(f"处理集成流配置出错: {e}")

            # 根据传入的动态参数设置需要抓取的状态，默认 2（全部）
            target_status = str(config.get("status", "2"))
            try:
                status_input = active_frame.locator("label[title='状态'] + div").locator("input").first
                status_input.click()
                page.wait_for_timeout(500)
                
                if str(target_status) == "2":
                    status_input.fill("")
                    page.keyboard.press("Enter")
                    page.wait_for_timeout(500)
                else:
                    status_input.fill(str(target_status))
                    page.wait_for_timeout(500)
                    
                     # 精确获取状态项（由于可能有0, 1）避免选择错误
                    item_st = active_frame.locator("li.wui-select-item").get_by_text(str(target_status), exact=True)
                    if item_st.count() > 0 and item_st.first.is_visible():
                        item_st.first.click()
                    else:
                        # 对于部分位于父级body的下拉框的特殊兼容
                        item_st2 = page.locator("li.wui-select-item").get_by_text(str(target_status), exact=True)
                        if item_st2.count() > 0 and item_st2.first.is_visible():
                            item_st2.first.click()
                        else:
                            page.keyboard.press("Enter")
                            
                    page.wait_for_timeout(500)
                    
                # 在正式点击查询前，尝试从 UI 层面也拉满分页（双重保险）
                try:
                    # 寻找分页下拉框（通常在表格底部）
                    pagination_selector = active_frame.locator("div.wui-select-selection").last
                    if pagination_selector.count() > 0:
                        pagination_selector.click()
                        page.wait_for_timeout(500)
                        # 尝试点击 1000 或 500
                        option_1000 = active_frame.locator("li.wui-select-item").get_by_text("1000", exact=True)
                        if option_1000.count() > 0:
                            option_1000.first.click()
                            print("[PROGRESS] UI 验证：已手动选择‘1000’分页", flush=True)
                        else:
                            # 尝试 500
                            option_500 = active_frame.locator("li.wui-select-item").get_by_text("500", exact=True)
                            if option_500.count() > 0:
                                option_500.first.click()
                                print("[PROGRESS] UI 验证：已手动选择‘500’分页", flush=True)
                        page.wait_for_timeout(500)
                except:
                    pass

                # 在点击之前重置历史数据抓包（避免读取到首次预加载包）
                intercepted_data = None
                print("[PROGRESS] 触发同步，正在请求后端 API 数据...")
                active_frame.locator("button.button-search").click()
            except Exception as e:
                print(f"选择状态或查询出错: {e}")
            
            # 等待网络拦截对象填装
            print("[PROGRESS] 数据传输中，正在获取全部分页结果...")
            start_wait = time.time()
            while time.time() - start_wait < 60:
                if intercepted_data is not None:
                    break
                page.wait_for_timeout(500)
                
            if intercepted_data is None:
                print("在 60 秒内未获取到 API 返回！抓取失败。正在生成截图...")
                page.screenshot(path="error_screenshot.png", full_page=True)
                
                # 尝试最后的保底方案：直接抓取 DOM 表格
                try:
                    wt_holder = active_frame.locator("div.wtHolder")
                    if wt_holder.count() > 0 and wt_holder.first.is_visible():
                        logs_text = wt_holder.first.inner_text()
                        print(f"成功进入保底方案：抓取到 DOM 文本 (约 {len(logs_text)} 字符)")
                        return f"FALLBACK_TEXT:{logs_text}"
                except:
                    pass
                    
                return ""
                
            return json.dumps(intercepted_data, ensure_ascii=False)
            
            # 强制硬等待 8s，确保系统接口返回且表格完全重绘
            # 如果不等待，会抓取到默认的“状态为 0”的旧日志
            page.wait_for_timeout(8000)
            
            wt_holder = active_frame.locator("div.wtHolder")
            if wt_holder.count() > 0 and wt_holder.first.is_visible():
                logs_text = wt_holder.first.inner_text()
            else:
                logs_text = "未找到包裹日志的容器 wtHolder，可能今日无数据或页面结构有变。"
            
            print(f"抓取到日志内容长度: {len(logs_text)}")
            return logs_text

        except Exception as e:
            try:
                page.screenshot(path="error_screenshot.png")
                with open("dom.txt", "w", encoding="utf-8") as f:
                    f.write(page.content())
                print("已保存错误截图至 error_screenshot.png，DOM 至 dom.txt")
            except Exception as inner_e:
                print(f"保存调试信息失败: {inner_e}")
            print(f"网页抓取过程发生异常: {e}")
            return f"网页抓取失败: {e}"
            
        finally:
            browser.close()


def process_and_save_data(logs_text: str, config: dict):
    # ================= 1. 清理旧文件 =================
    # 每次开始处理前，先强制删除旧文件，防止程序中途报错导致发送上一次的“幽灵文件”
    for old_file in ["error_logs.txt", "error_logs.xlsx", "report_summary.md"]:
        if os.path.exists(old_file):
            try:
                os.remove(old_file)
            except Exception as e:
                print(f"清理旧文件 {old_file} 失败: {e}")

    def save_empty_result(msg):
        df_empty = pd.DataFrame([{"巡检结果": msg}])
        df_empty.to_csv("error_logs.txt", sep='\t', index=False, encoding='utf-8-sig')
        df_empty.to_excel("error_logs.xlsx", index=False)

        # 👇 补上这三行：生成全绿色的成功卡片文案
        with open("report_summary.md", "w", encoding="utf-8") as f:
            f.write(f"🎉 **系统运行平稳，未发现异常。**\n\n*(附加说明：{msg})*")

        print(f"[PROGRESS] 提示：{msg}。已为您生成说明文件。", flush=True)

    if not logs_text or logs_text.strip() == "":
        save_empty_result("没有抓取到异常日志或对应结构为空")
        return

    # ================= 2. 识别数据类型 =================
    if logs_text.startswith("网页抓取失败:"):
        save_empty_result(f"浏览器端拦截数据时发生错误, 详情参见日志截屏")
        return

    if logs_text.startswith("FALLBACK_TEXT:"):
        raw_text = logs_text.replace("FALLBACK_TEXT:", "")
        lines = [l.strip() for l in raw_text.split('\n') if l.strip()]
        if not lines:
            save_empty_result("进入了保底方案，但页面上未找到任何文本内容")
            return

        df = pd.DataFrame(lines, columns=["原始数据行(保底方案输出)"])
        df.to_csv("error_logs.txt", sep='\t', index=False, encoding='utf-8-sig')
        df.to_excel("error_logs.xlsx", index=False)
        print("[PROGRESS] ✅ 注意：由于 API 拦截失败，当前导出的是页面可见部分的原始文本（保底方案）。")
        return

    print("\n=== 成功拦截到 API 数据，正在提取并处理... ===")
    try:
        data = json.loads(logs_text)
    except Exception as e:
        save_empty_result(f"数据解析失败，返回的不是合法的 JSON 格式")
        return

    try:
        # ================= 3. 解析 JSON 树 =================
        def find_best_cells(obj):
            best_cells = []
            if isinstance(obj, dict):
                if 'cells' in obj and isinstance(obj['cells'], list):
                    if len(obj['cells']) > len(best_cells):
                        best_cells = obj['cells']
                for k, v in obj.items():
                    res = find_best_cells(v)
                    if len(res) > len(best_cells):
                        best_cells = res
            elif isinstance(obj, list):
                for item in obj:
                    res = find_best_cells(item)
                    if len(res) > len(best_cells):
                        best_cells = res
            return best_cells

        cells = find_best_cells(data)

        if not cells or len(cells) < 2:
            save_empty_result("返回的数据结构中未找到可用的表格数据或数据为空")
            return

        headers = []
        header_row = cells[0]
        for col in header_row:
            if isinstance(col, list) and len(col) > 5:
                headers.append(str(col[0]))
            elif isinstance(col, dict) and 'v' in col:
                headers.append(str(col['v']))
            else:
                headers.append(str(col))

        extracted = []
        for row in cells[1:]:
            row_data = []
            for col in row:
                if isinstance(col, list) and len(col) > 5:
                    row_data.append(str(col[0]))
                elif isinstance(col, dict) and 'v' in col:
                    row_data.append(str(col['v']))
                else:
                    if col is None:
                        row_data.append('')
                    else:
                        row_data.append(str(col))
            extracted.append(row_data)

        if not extracted:
            save_empty_result("JSON 数据提取完成，但未发现任何行级原始日志记录")
            return

        # ================= 4. 初始化 DataFrame =================
        df = pd.DataFrame(extracted, columns=headers)
        print(f"--- 调试信息：原始抓取到 {len(df)} 条记录 ---")

        # 🚨 核心修复1：剔除重复列名与垃圾列（解决 'str' 报错问题）
        df = df.loc[:, ~df.columns.duplicated()]
        df = df.loc[:, ~df.columns.isin(['', 'None', 'nan', 'NaN'])]

        # ================= 5. 数据深度清洗 =================
        # 🚨 核心修复2：降维打击，强制全表转为纯字符串，抹平数字与浮点数
        for col in df.columns:
            df[col] = df[col].fillna('').astype(str).str.strip()
            # 清理 Pandas 误认整数为浮点数带入的 .0 尾巴
            if df[col].str.endswith('.0').any():
                df[col] = df[col].str.replace(r'\.0$', '', regex=True)

        # 移除无用列
        cols_to_drop = [c for c in ['主键', '编码'] if c in df.columns]
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)

        # 白名单过滤
        whitelist = config.get("whitelist", [])
        msg_col = next((c for c in df.columns if '消息' in c), None)
        if whitelist and msg_col:
            pre_len = len(df)
            for w in whitelist:
                df = df[~df[msg_col].str.contains(w, na=False, regex=False)]
            dropped = pre_len - len(df)
            if dropped > 0:
                print(f"[PROGRESS] 🧹 白名单过滤：命中排除词汇，剔除 {dropped} 条，剩余 {len(df)} 条", flush=True)

        # 专属屏蔽：“华夏”相关集成流直接强制抛弃
        flow_col = next((c for c in df.columns if '集成流' in c), None)
        if flow_col and not df.empty:
            pre_len = len(df)
            df = df[~df[flow_col].str.contains("华夏", na=False, regex=False)]
            dropped = pre_len - len(df)
            if dropped > 0:
                print(f"[PROGRESS] 🧹 系统过滤：强制屏蔽“华厦”相关流，剔除 {dropped} 条，剩余 {len(df)} 条", flush=True)

        # 日期区间过滤
        time_col = next((c for c in df.columns if '创建时间' in c), None)
        if time_col and not df.empty:
            start_date = config.get("start_date")
            end_date = config.get("end_date")
            if start_date or end_date:
                pre_len = len(df)
                df_dates = pd.to_datetime(df[time_col], errors='coerce')

                if start_date:
                    s_date = pd.to_datetime(str(start_date))
                    df = df[df_dates >= s_date]
                    df_dates = df_dates[df_dates >= s_date]

                if end_date:
                    e_date = pd.to_datetime(str(end_date)).replace(hour=23, minute=59, second=59)
                    df = df[df_dates <= e_date]
                    
                dropped = pre_len - len(df)
                if dropped > 0:
                    print(f"[PROGRESS] 🧹 日期过滤：剔除超出范围记录 {dropped} 条，剩余 {len(df)} 条", flush=True)

        # 第一重去重：全局报文去重。必须放在剔除状态之前。
        # 这样如果同一条报文重复请求，且最后一次是成功的(0)，该成功的0会存留并干掉历史的报错(1)
        # 随后紧跟的状态清道夫再去清理0，就能把这种“已自愈”的报错斩草除根。
        dup_columns = []
        for col_kw in ['集成流', '请求报文']:
            found_col = next((c for c in df.columns if col_kw in c), None)
            if found_col:
                dup_columns.append(found_col)

        if dup_columns and time_col and not df.empty:
            pre_len = len(df)
            df = df.sort_values(by=dup_columns + [time_col], ascending=[True] * len(dup_columns) + [False])
            df = df.drop_duplicates(subset=dup_columns, keep='first')
            dropped = pre_len - len(df)
            print(f"[PROGRESS] 🧹 时序去重：合并重复重试报文 {dropped} 条，剩余 {len(df)} 条", flush=True)

        # 状态过滤：强制剔除所有成功的请求记录 (0)，无论前端拉取了什么
        status_col = next((c for c in df.columns if '状态' in c), None)
        if status_col and not df.empty:
            pre_len = len(df)
            # 保留所有去除空格后 != '0' 和 != '0.0' 的记录，绝对防漏
            df = df[~df[status_col].astype(str).str.strip().isin(['0', '0.0'])]
            dropped = pre_len - len(df)
            print(f"[PROGRESS] 🧹 状态清洗：强制剔除成功记录 {dropped} 条，剩余 {len(df)} 条报错", flush=True)

        # 消息相似度去重 (用正则将动态数字全变为 '*')
        dup_columns_msg = []
        for col_kw in ['集成流', '消息']:
            found_col = next((c for c in df.columns if col_kw in c), None)
            if found_col:
                dup_columns_msg.append(found_col)

        if dup_columns_msg and time_col and not df.empty:
            pre_len = len(df)
            df['_clean_msg'] = df[dup_columns_msg[1]].str.replace(r'\d+', '*', regex=True)
            sort_cols = [dup_columns_msg[0], '_clean_msg', time_col]
            df = df.sort_values(by=sort_cols, ascending=[True, True, False])
            df = df.drop_duplicates(subset=[dup_columns_msg[0], '_clean_msg'], keep='first')
            df.drop(columns=['_clean_msg'], inplace=True)
            dropped = pre_len - len(df)
            if dropped > 0:
                print(f"[PROGRESS] 🧹 掩码去重：合并同类项噪音 {dropped} 条，最终剩余 {len(df)} 条报错清单", flush=True)

        # ================= 6. 最终排序与保存 =================
        if time_col and not df.empty:
            df = df.sort_values(by=time_col, ascending=False)

        if df.empty:
            save_empty_result("经过白名单消息和日期深度过滤后，当前时间段内无符合条件的报错")
            return

        print(f"\n[PROGRESS] ✅ 过滤完成！最终留存的报错记录条数: {len(df)} 条\n", flush=True)

        # ================= 7. 生成移动端直推富文本摘要战报 =================
        try:
            flow_col = next((c for c in df.columns if '集成流' in c), None)
            msg_col = next((c for c in df.columns if '消息' in c), None)
            time_col_final = next((c for c in df.columns if '创建时间' in c), None)
            
            report_lines = []
            report_lines.append("🚨 **日志巡检异常战报**")
            report_lines.append(f"🕒 巡检时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            report_lines.append(f"📊 总计抓获真实报错：**{len(df)}** 条")
            report_lines.append("──────────────")
            
            if flow_col and msg_col and time_col_final:
                # 按照时间从新到旧，最多直接展示前 10 条具体异常留观
                display_df = df.head(10)
                
                for idx, row in display_df.iterrows():
                    flow_name = str(row[flow_col])
                    create_time = str(row[time_col_final])
                    sample_msg = str(row[msg_col])
                    
                    # 强截断防止内容过长撑爆屏幕导致截断飞书不发
                    if len(sample_msg) > 60:
                        sample_msg = sample_msg[:57] + "..."
                        
                    report_lines.append(f"🔴 **{flow_name}**")
                    report_lines.append(f"👉 *时间*: {create_time}")
                    report_lines.append(f"└ *消息*: {sample_msg}")
                    report_lines.append("") # 空行分隔
                
                if len(df) > 10:
                    report_lines.append(f"...及其他 {len(df)-10} 条隐藏折叠。")
            
            report_lines.append("──────────────")
            report_lines.append("💡 *详细全量排查请查收随后的 Excel 附件。*")
            
            summary_text = "\n".join(report_lines)
            with open("report_summary.md", "w", encoding="utf-8") as rf:
                rf.write(summary_text)
            
            # 使用特定前缀让服务端知道需要提取整段作为 Markdown 发送
            print(f"[PROGRESS] ✅ 生成简报完成，已准备卡片投递...", flush=True)
            
        except Exception as e:
            print(f"生成摘要战报时出错: {e}")

        # 写入 txt 时使用 utf-8-sig，防止在 Windows 系统中乱码
        df.to_csv("error_logs.txt", sep='\t', index=False, encoding='utf-8-sig')
        df.to_excel("error_logs.xlsx", index=False)
        print("[PROGRESS] 巡检处理成功！已生成干净的 Excel 报表...")

    except Exception as e:
        print(f"处理或保存异常日志时出错: {e}")

if __name__ == "__main__":
    config_dict = load_config()
    logs = scrape_logs(config_dict)
    process_and_save_data(logs, config_dict)
