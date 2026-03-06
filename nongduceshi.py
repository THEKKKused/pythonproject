"""
浓度标定 0x2A/0x2B 协议专用测试工具
用于排查写入/回读时 M 值错位的问题。
"""

import sys
import struct
import crcmod.predefined
import serial
from serial.tools import list_ports
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QGridLayout, QLabel, QLineEdit, QPushButton, QTextEdit,
    QComboBox, QGroupBox, QMessageBox
)


# --- 协议工具 ---
def modbus_crc(data: bytes) -> bytes:
    crc16 = crcmod.predefined.mkPredefinedCrcFun('modbus')
    return crc16(data).to_bytes(2, 'little')


def bytes_u16_be(v: int) -> bytes:
    return (int(v) & 0xFFFF).to_bytes(2, 'big')


def bytes_u24_be(v: int) -> bytes:
    return (int(v) & 0xFFFFFF).to_bytes(3, 'big')


def bytes_u32_be(v: int) -> bytes:
    return (int(v) & 0xFFFFFFFF).to_bytes(4, 'big')


def bytes_float_be(v: float) -> bytes:
    return struct.pack('>f', float(v))


# --- 串口线程 ---
class SerialWorker(QThread):
    log_signal = pyqtSignal(str)
    rx_signal = pyqtSignal(bytes)

    def __init__(self, port, baud):
        super().__init__()
        self.port = port
        self.baud = baud
        self.ser = None
        self.running = False
        self.tx_queue = []

    def open_port(self):
        try:
            self.ser = serial.Serial(self.port, self.baud, timeout=0.1)
            self.running = True
            return True
        except Exception as e:
            self.log_signal.emit(f"❌ 串口打开失败: {e}")
            return False

    def close_port(self):
        self.running = False
        if self.ser and self.ser.is_open:
            self.ser.close()

    def send(self, data: bytes):
        self.tx_queue.append(data)

    def run(self):
        while self.running and self.ser and self.ser.is_open:
            if self.tx_queue:
                data = self.tx_queue.pop(0)
                try:
                    self.ser.write(data)
                    hex_str = ' '.join(f'{b:02X}' for b in data)
                    self.log_signal.emit(f"<font color='blue'><b>TX:</b> {hex_str}</font>")
                except Exception as e:
                    self.log_signal.emit(f"发送异常: {e}")

            try:
                n = self.ser.in_waiting
                if n > 0:
                    rx = self.ser.read(n)
                    hex_str = ' '.join(f'{b:02X}' for b in rx)
                    self.log_signal.emit(f"<font color='green'><b>RX:</b> {hex_str}</font>")
                    self.rx_signal.emit(rx)
            except Exception:
                pass
            self.msleep(10)


# --- 主窗口 ---
class ProtocolTesterWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("0x2A / 0x2B 协议原始排查工具")
        self.resize(800, 600)
        self.worker = None

        layout = QVBoxLayout()

        # 1. 串口设置
        gb_port = QGroupBox("1. 串口连接")
        lo_port = QHBoxLayout()
        self.cb_ports = QComboBox()
        self.cb_ports.addItems([p.device for p in list_ports.comports()])
        self.cb_baud = QComboBox()
        self.cb_baud.addItems(["9600", "19200", "38400", "115200"])
        self.cb_baud.setCurrentText("115200")
        self.btn_open = QPushButton("打开串口")
        self.btn_open.clicked.connect(self.toggle_port)
        lo_port.addWidget(QLabel("端口:"))
        lo_port.addWidget(self.cb_ports)
        lo_port.addWidget(QLabel("波特率:"))
        lo_port.addWidget(self.cb_baud)
        lo_port.addWidget(self.btn_open)
        lo_port.addStretch()
        gb_port.setLayout(lo_port)
        layout.addWidget(gb_port)

        # 2. 模式与初始化
        gb_mode = QGroupBox("2. 基础指令")
        lo_mode = QHBoxLayout()
        btn_user = QPushButton("进入用户模式 (01 1E 00)")
        btn_user.clicked.connect(lambda: self.send_hex("01 1E 00"))
        btn_debug = QPushButton("进入调试模式 (01 20 00)")
        btn_debug.clicked.connect(lambda: self.send_hex("01 20 00"))
        btn_set3 = QPushButton("设置标定点数为 3 (0x2A 00 03)")
        btn_set3.clicked.connect(lambda: self.send_hex("01 2A 00 03"))
        btn_read_num = QPushButton("回读标定点数 (0x2B 00)")
        btn_read_num.clicked.connect(lambda: self.send_hex("01 2B 00"))

        lo_mode.addWidget(btn_user)
        lo_mode.addWidget(btn_debug)
        lo_mode.addWidget(btn_set3)
        lo_mode.addWidget(btn_read_num)
        gb_mode.setLayout(lo_mode)
        layout.addWidget(gb_mode)

        # 3. 0x2A 写入排查
        gb_2a = QGroupBox("3. 写入单点浓度 (0x2A 排查)")
        lo_2a = QGridLayout()
        self.ed_idx = QLineEdit("1")
        self.ed_ppm = QLineEdit("10100")
        self.ed_m = QLineEdit("969")

        self.cb_format = QComboBox()
        self.cb_format.addItems([
            "格式A: 3字节 PPM + 4字节 M (原代码格式)",
            "格式B: 4字节 PPM + 4字节 M (常见32位对齐)",
            "格式C: 2字节 PPM + 4字节 M (常见Modbus16位)",
            "格式D: 4字节 M + 4字节 PPM (顺序反转)",
            "格式E: 浮点数 PPM + 浮点数 M (IEEE754)"
        ])

        btn_send_2a = QPushButton("构造并发送 0x2A 写入指令")
        btn_send_2a.clicked.connect(self.send_0x2a)

        lo_2a.addWidget(QLabel("点索引(Idx):"), 0, 0)
        lo_2a.addWidget(self.ed_idx, 0, 1)
        lo_2a.addWidget(QLabel("PPM 浓度值:"), 0, 2)
        lo_2a.addWidget(self.ed_ppm, 0, 3)
        lo_2a.addWidget(QLabel("M 内码值:"), 0, 4)
        lo_2a.addWidget(self.ed_m, 0, 5)
        lo_2a.addWidget(QLabel("Payload 格式猜测:"), 1, 0)
        lo_2a.addWidget(self.cb_format, 1, 1, 1, 3)
        lo_2a.addWidget(btn_send_2a, 1, 4, 1, 2)
        gb_2a.setLayout(lo_2a)
        layout.addWidget(gb_2a)

        # 4. 0x2B 回读
        gb_2b = QGroupBox("4. 回读单点浓度 (0x2B)")
        lo_2b = QHBoxLayout()
        self.ed_read_idx = QLineEdit("1")
        btn_send_2b = QPushButton("发送 0x2B 回读指令 (观察RX原始HEX解析)")
        btn_send_2b.clicked.connect(lambda: self.send_hex(f"01 2B {int(self.ed_read_idx.text() or 1):02X}"))
        lo_2b.addWidget(QLabel("点索引(Idx):"))
        lo_2b.addWidget(self.ed_read_idx)
        lo_2b.addWidget(btn_send_2b)
        lo_2b.addStretch()
        gb_2b.setLayout(lo_2b)
        layout.addWidget(gb_2b)

        # 5. 自定义HEX与日志
        lo_raw = QHBoxLayout()
        self.ed_raw = QLineEdit()
        self.ed_raw.setPlaceholderText("在此输入任意 Hex (如 01 03 00 00 00 01)，自动补CRC")
        btn_raw = QPushButton("发送 HEX")
        btn_raw.clicked.connect(lambda: self.send_hex(self.ed_raw.text()))
        lo_raw.addWidget(self.ed_raw)
        lo_raw.addWidget(btn_raw)
        layout.addLayout(lo_raw)

        self.log_txt = QTextEdit()
        self.log_txt.setReadOnly(True)
        self.log_txt.setStyleSheet("background-color:#1e1e1e; color:#d4d4d4; font-family:Consolas; font-size:14px;")
        layout.addWidget(self.log_txt)

        btn_clear = QPushButton("清空日志")
        btn_clear.clicked.connect(self.log_txt.clear)
        layout.addWidget(btn_clear)

        cw = QWidget()
        cw.setLayout(layout)
        self.setCentralWidget(cw)

    def log(self, msg):
        self.log_txt.append(msg)

    def toggle_port(self):
        if self.worker and self.worker.running:
            self.worker.close_port()
            self.worker.wait()
            self.worker = None
            self.btn_open.setText("打开串口")
            self.log("串口已关闭。")
        else:
            port = self.cb_ports.currentText()
            if not port: return
            baud = int(self.cb_baud.currentText())
            self.worker = SerialWorker(port, baud)
            self.worker.log_signal.connect(self.log)
            if self.worker.open_port():
                self.worker.start()
                self.btn_open.setText("关闭串口")
                self.log(f"串口 {port} 已打开。")

    def send_hex(self, hex_str: str):
        if not self.worker or not self.worker.running:
            QMessageBox.warning(self, "错误", "请先打开串口")
            return
        try:
            clean = hex_str.replace(' ', '').replace(',', '')
            if not clean: return
            data = bytes.fromhex(clean)
            data_with_crc = data + modbus_crc(data)
            self.worker.send(data_with_crc)
        except Exception as e:
            QMessageBox.warning(self, "格式错误", str(e))

    def send_0x2a(self):
        if not self.worker or not self.worker.running:
            QMessageBox.warning(self, "错误", "请先打开串口")
            return

        try:
            idx = int(self.ed_idx.text()) & 0xFF
            ppm = float(self.ed_ppm.text())
            m = float(self.ed_m.text())
        except ValueError:
            QMessageBox.warning(self, "错误", "请输入有效的数字")
            return

        fmt = self.cb_format.currentText()
        payload = bytearray([0x01, 0x2A, idx])

        if "3字节 PPM" in fmt:
            payload += bytes_u24_be(int(ppm))
            payload += bytes_u32_be(int(m))
        elif "4字节 PPM" in fmt and "格式B" in fmt:
            payload += bytes_u32_be(int(ppm))
            payload += bytes_u32_be(int(m))
        elif "2字节 PPM" in fmt:
            payload += bytes_u16_be(int(ppm))
            payload += bytes_u32_be(int(m))
        elif "反转" in fmt:
            payload += bytes_u32_be(int(m))
            payload += bytes_u32_be(int(ppm))
        elif "浮点数" in fmt:
            payload += bytes_float_be(ppm)
            payload += bytes_float_be(m)

        self.log(f"--- 构造 0x2A [{fmt.split(':')[0]}] ---")
        self.worker.send(bytes(payload) + modbus_crc(payload))


if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = ProtocolTesterWindow()
    w.show()
    sys.exit(app.exec())