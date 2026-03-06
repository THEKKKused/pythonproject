import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import serial
import serial.tools.list_ports
import threading
import time
import re
import crcmod.predefined


# ================= 配置与工具函数 =================

def modbus_crc(data: bytes) -> bytes:
    """Modbus RTU CRC16 (低字节在前)"""
    crc16 = crcmod.predefined.mkPredefinedCrcFun('modbus')
    return crc16(data).to_bytes(2, 'little')


def create_command(body_list):
    """生成带 CRC 的完整指令"""
    body = bytes(body_list)
    return body + modbus_crc(body)


def format_bytes(data: bytes) -> str:
    """把 bytes 格式化为 '01 1E 00 ...'"""
    return ' '.join(f'{b:02X}' for b in data)


def parse_hex_input(text: str) -> bytes:
    """
    解析用户输入的 HEX 指令：
    - 支持空格/逗号分隔：01 1E 00
    - 支持连续输入：011E00
    - 支持 0x 前缀：0x01 0x1E 0x00
    """
    s = (text or "").strip()
    if not s:
        raise ValueError("HEX 输入为空。")

    # 先尝试按分隔符切分
    tokens = re.split(r'[\s,]+', s)
    tokens = [t for t in tokens if t]

    # 只有一个 token 且是纯 hex：可能是连续输入
    if len(tokens) == 1:
        t = tokens[0].lower()
        if t.startswith("0x"):
            t = t[2:]
        if re.fullmatch(r'[0-9a-f]+', t or ""):
            if len(t) % 2 != 0:
                raise ValueError("连续 HEX 字符串长度必须为偶数，例如 011E00。")
            return bytes.fromhex(t)

    # 多 token：逐个清洗再 fromhex
    cleaned = []
    for t in tokens:
        t = t.strip()
        if not t:
            continue
        if t.lower().startswith("0x"):
            t = t[2:]
        if not re.fullmatch(r'[0-9a-fA-F]{1,2}', t):
            raise ValueError(f"非法 HEX 字节: {t}")
        # 允许 1 位 hex（补 0）
        if len(t) == 1:
            t = "0" + t
        cleaned.append(t)

    if not cleaned:
        raise ValueError("HEX 输入为空。")

    return bytes.fromhex(' '.join(cleaned))


# ================= 单个传感器的工作逻辑 =================

class SensorTab:
    """
    负责单个串口的连接、指令发送、数据读取和界面显示
    """

    def __init__(self, parent_notebook, port_name, baudrate=115200):
        self.port_name = port_name
        self.baudrate = baudrate
        self.ser = None
        self.running = False
        self.thread = None
        self.write_lock = threading.Lock()

        # --- UI 构建 ---
        self.frame = ttk.Frame(parent_notebook)
        parent_notebook.add(self.frame, text=port_name)

        # 顶部工具栏
        tool_frame = ttk.Frame(self.frame)
        tool_frame.pack(fill=tk.X, padx=5, pady=2)

        ttk.Label(tool_frame, text=f"端口: {port_name}").pack(side=tk.LEFT)
        self.status_lbl = ttk.Label(tool_frame, text="状态: 待机", foreground="gray")
        self.status_lbl.pack(side=tk.LEFT, padx=10)

        clean_btn = ttk.Button(tool_frame, text="清屏", command=self.clear_text)
        clean_btn.pack(side=tk.RIGHT)

        # 文本显示区 (带滚动条)
        self.text_area = scrolledtext.ScrolledText(self.frame, state='disabled', height=20, font=("Consolas", 10))
        self.text_area.pack(expand=True, fill=tk.BOTH, padx=5, pady=5)

        # 定义文本颜色标签
        self.text_area.tag_config('rx', foreground='green')  # 接收到的数据
        self.text_area.tag_config('tx', foreground='blue')   # 发送的指令
        self.text_area.tag_config('err', foreground='red')   # 错误信息
        self.text_area.tag_config('sys', foreground='gray')  # 系统信息

        # --- 命令区 ---
        cmd_group = ttk.LabelFrame(self.frame, text="命令发送")
        cmd_group.pack(fill=tk.X, padx=5, pady=5)

        btn_row = ttk.Frame(cmd_group)
        btn_row.pack(fill=tk.X, padx=5, pady=3)

        ttk.Button(btn_row, text="进入用户模式 (01 1E 00)", command=self.send_user_mode).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_row, text="传感器复位 (01 21 00)", command=self.send_sensor_reset).pack(side=tk.LEFT, padx=2)

        custom_row = ttk.Frame(cmd_group)
        custom_row.pack(fill=tk.X, padx=5, pady=3)

        ttk.Label(custom_row, text="HEX:").pack(side=tk.LEFT)
        self.hex_entry = ttk.Entry(custom_row)
        self.hex_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=5)

        self.append_crc_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(custom_row, text="自动补CRC(Modbus)", variable=self.append_crc_var).pack(side=tk.LEFT, padx=5)

        ttk.Button(custom_row, text="发送", command=self.send_custom_hex).pack(side=tk.RIGHT)

        hint = ttk.Label(cmd_group, text="提示：可输入“01 1E 00”或“011E00”，可选自动补CRC。", foreground="gray")
        hint.pack(anchor='w', padx=8, pady=(0, 5))

    def log(self, msg, tag='sys'):
        """向文本框追加内容"""
        def _append():
            try:
                self.text_area.config(state='normal')
                self.text_area.insert(tk.END, msg + "\n", tag)
                self.text_area.see(tk.END)  # 自动滚动到底部
                self.text_area.config(state='disabled')
            except Exception:
                pass

        # 确保在主线程更新UI
        self.text_area.after(0, _append)

    def clear_text(self):
        self.text_area.config(state='normal')
        self.text_area.delete(1.0, tk.END)
        self.text_area.config(state='disabled')

    def _is_open(self) -> bool:
        return bool(self.ser and self.ser.is_open)

    def send_bytes(self, data: bytes, desc: str = "发送指令"):
        """安全发送（带日志）"""
        if not self._is_open():
            self.log(f"{desc}失败：串口未打开。", "err")
            return

        try:
            with self.write_lock:
                self.ser.write(data)
            self.log(f"{desc}: {format_bytes(data)}", "tx")
        except Exception as e:
            self.log(f"{desc}失败: {e}", "err")

    def send_user_mode(self):
        # 进入用户模式：01 1E 00 + CRC
        cmd = create_command([0x01, 0x1E, 0x00])
        self.send_bytes(cmd, "进入用户模式")

    def send_sensor_reset(self):
        # 传感器复位：01 21 00 + CRC
        cmd = create_command([0x01, 0x21, 0x00])
        self.send_bytes(cmd, "传感器复位")

    def send_custom_hex(self):
        """发送自由输入 HEX 指令，可选补 CRC"""
        text = self.hex_entry.get()
        try:
            raw = parse_hex_input(text)
        except ValueError as e:
            self.log(f"HEX 解析失败：{e}", "err")
            return

        data = raw
        if self.append_crc_var.get():
            data = raw + modbus_crc(raw)
            self.send_bytes(data, "发送HEX(自动补CRC)")
        else:
            self.send_bytes(data, "发送HEX(原样)")

    def start(self):
        """启动串口连接和读取线程"""
        if self.running and self._is_open():
            return True

        try:
            self.ser = serial.Serial(self.port_name, self.baudrate, timeout=1.0)
            self.running = True
            self.status_lbl.config(text="状态: 监控中", foreground="green")
            self.log(f"--- 已打开端口 {self.port_name} ---")

            # 发送进入调试模式指令: 01 20 00
            cmd = create_command([0x01, 0x20, 0x00])
            self.send_bytes(cmd, "进入调试模式")

            # 开启读取线程
            self.thread = threading.Thread(target=self._read_loop, daemon=True)
            self.thread.start()
            return True

        except serial.SerialException as e:
            self.log(f"打开失败: {e}", 'err')
            self.status_lbl.config(text="状态: 错误", foreground="red")
            return False

    def stop(self):
        """停止并断开"""
        self.running = False
        if self.ser and self.ser.is_open:
            try:
                self.ser.close()
            except Exception:
                pass
        self.status_lbl.config(text="状态: 已断开", foreground="gray")
        self.log("--- 端口已关闭 ---")

    def _read_loop(self):
        """后台读取循环"""
        while self.running and self.ser and self.ser.is_open:
            try:
                # 尝试读取一行 (以 \n 结尾)
                if self.ser.in_waiting:
                    raw_data = self.ser.readline()
                    if raw_data:
                        # 尝试解码 ASCII，忽略乱码
                        text_str = raw_data.decode('ascii', errors='replace').strip()
                        if text_str:
                            self.log(text_str, 'rx')
                else:
                    time.sleep(0.05)
            except Exception as e:
                self.log(f"读取错误: {e}", 'err')
                break


# ================= 主应用程序窗口 =================

class MainApp:
    def __init__(self, root):
        self.root = root
        self.root.title("多传感器调试工具 (GUI版)")
        self.root.geometry("1200x750")  # 稍微加宽一点主窗口

        self.active_sensors = {}

        # --- 布局 ---
        left_panel = ttk.Frame(root)
        left_panel.pack(side=tk.LEFT, fill=tk.Y, padx=5, pady=5)

        # 右侧标签页区域
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(side=tk.RIGHT, expand=True, fill=tk.BOTH, padx=5, pady=5)

        # --- 左侧控件 ---
        ttk.Label(left_panel, text="可用端口列表", font=("Arial", 12, "bold")).pack(pady=5)

        # === 列表框区域：增加横向滚动条 + 变宽 ===
        list_frame = ttk.Frame(left_panel)
        list_frame.pack(fill=tk.BOTH, expand=True)

        v_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL)
        h_scrollbar = ttk.Scrollbar(list_frame, orient=tk.HORIZONTAL)

        self.port_listbox = tk.Listbox(
            list_frame,
            selectmode=tk.EXTENDED,
            yscrollcommand=v_scrollbar.set,
            xscrollcommand=h_scrollbar.set,
            width=70,
            height=20
        )

        v_scrollbar.config(command=self.port_listbox.yview)
        h_scrollbar.config(command=self.port_listbox.xview)

        self.port_listbox.grid(row=0, column=0, sticky='nsew')
        v_scrollbar.grid(row=0, column=1, sticky='ns')
        h_scrollbar.grid(row=1, column=0, sticky='ew')

        list_frame.grid_rowconfigure(0, weight=1)
        list_frame.grid_columnconfigure(0, weight=1)
        # ==================================================

        # 按钮区
        btn_frame = ttk.Frame(left_panel)
        btn_frame.pack(fill=tk.X, pady=10)

        ttk.Button(btn_frame, text="1. 刷新端口", command=self.scan_ports).pack(fill=tk.X, pady=2)
        ttk.Button(btn_frame, text="全选端口", command=self.select_all).pack(fill=tk.X, pady=2)
        ttk.Separator(btn_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=5)

        self.btn_start = ttk.Button(btn_frame, text="2. 进入调试模式 (所选)", command=self.start_selected)
        self.btn_start.pack(fill=tk.X, pady=2)

        # 新增：广播命令到所有已连接端口
        ttk.Button(btn_frame, text="3. 进入用户模式 (所有已连接)", command=self.send_user_mode_all).pack(fill=tk.X, pady=2)
        ttk.Button(btn_frame, text="4. 传感器复位 (所有已连接)", command=self.send_reset_all).pack(fill=tk.X, pady=2)

        ttk.Separator(btn_frame, orient=tk.HORIZONTAL).pack(fill=tk.X, pady=5)

        self.btn_stop = ttk.Button(btn_frame, text="停止并断开所有", command=self.stop_all)
        self.btn_stop.pack(fill=tk.X, pady=2)

        # 说明
        info_lbl = tk.Label(
            left_panel,
            text="操作说明:\n1) 刷新并选择端口\n2) 点击进入调试模式（会自动发送 01 20 00+CRC）\n3) 每个标签页底部可单独发送“用户模式/复位/自定义HEX”\n4) 左侧也可对所有已连接端口广播命令",
            justify=tk.LEFT,
            fg="gray",
            wraplength=280
        )
        info_lbl.pack(side=tk.BOTTOM, pady=10)

        # 初始化扫描
        self.scan_ports()

    def scan_ports(self):
        self.port_listbox.delete(0, tk.END)
        ports = serial.tools.list_ports.comports()

        ports.sort(key=lambda x: x.device)

        if not ports:
            self.port_listbox.insert(tk.END, "未发现串口")
            return

        for p in ports:
            extra_info = ""
            if getattr(p, "location", None):
                extra_info = f" [Loc: {p.location}]"
            elif getattr(p, "serial_number", None):
                extra_info = f" [SN: {p.serial_number}]"
            else:
                if getattr(p, "hwid", "") and "VID" in p.hwid:
                    extra_info = f" [ID: {p.hwid.split(' ')[-1]}]"

            display_str = f"{p.device} - {p.description} {extra_info}"
            self.port_listbox.insert(tk.END, display_str)

    def select_all(self):
        self.port_listbox.select_set(0, tk.END)

    def start_selected(self):
        """开启选中的端口（并自动发送进入调试模式指令）"""
        selection = self.port_listbox.curselection()
        if not selection:
            messagebox.showwarning("提示", "请先在列表中选择至少一个端口。")
            return

        selected_ports = []
        for idx in selection:
            item = self.port_listbox.get(idx)
            if "未发现" in item:
                continue
            port_name = item.split(' ')[0]  # 取 COMx
            selected_ports.append(port_name)

        for p in selected_ports:
            if p in self.active_sensors:
                # 已存在标签页：如果已断开则重连
                tab = self.active_sensors[p]
                if not (tab.running and tab.ser and tab.ser.is_open):
                    if tab.start():
                        self.notebook.select(tab.frame)
                else:
                    self.notebook.select(tab.frame)
                continue

            tab = SensorTab(self.notebook, p)
            if tab.start():
                self.active_sensors[p] = tab
                self.notebook.select(tab.frame)
            else:
                self.notebook.forget(tab.frame)

    def _broadcast(self, fn_name: str, desc: str):
        if not self.active_sensors:
            messagebox.showinfo("提示", "当前没有已连接的端口。")
            return

        for tab in list(self.active_sensors.values()):
            try:
                fn = getattr(tab, fn_name, None)
                if callable(fn):
                    fn()
            except Exception:
                pass

        messagebox.showinfo("完成", f"已向所有已连接端口发送：{desc}")

    def send_user_mode_all(self):
        self._broadcast("send_user_mode", "进入用户模式 (01 1E 00)")

    def send_reset_all(self):
        self._broadcast("send_sensor_reset", "传感器复位 (01 21 00)")

    def stop_all(self):
        """关闭所有"""
        for port, sensor in list(self.active_sensors.items()):
            sensor.stop()
        messagebox.showinfo("完成", "已断开所有端口。")


if __name__ == "__main__":
    root = tk.Tk()

    # 设置主题样式
    try:
        style = ttk.Style()
        style.theme_use('clam')
    except Exception:
        pass

    app = MainApp(root)

    # 关闭窗口时清理串口
    def on_closing():
        try:
            app.stop_all()
        finally:
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)
    root.mainloop()
