import sys
import time
import threading
from PyQt6.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QLabel, QLineEdit, QPushButton,
                             QTextEdit, QGroupBox, QGridLayout, QMessageBox)
from PyQt6.QtCore import QThread, pyqtSignal, Qt
import pymodbus
from pymodbus.client import ModbusTcpClient

# 全局常量
DEVICE_IP_DEFAULT = '192.168.50.2'
DEVICE_PORT_DEFAULT = 8000
SLAVE_ID = 1

# 寄存器地址
REG_SET_TEMP = 8100
REG_CUR_TEMP = 7991
COIL_START = 8000
COIL_STOP = 8001
REG_MODE = 8108

# 兼容性处理
ver = pymodbus.__version__
if ver.startswith('3.1') or ver.startswith('4.'):
    SLAVE_ARG = 'device_id'
elif ver.startswith('3.'):
    SLAVE_ARG = 'slave'
else:
    SLAVE_ARG = 'unit'


class WorkerThread(QThread):
    """后台工作线程，负责Modbus通信和逻辑控制"""
    log_signal = pyqtSignal(str)  # 发送日志到界面
    temp_signal = pyqtSignal(float)  # 发送当前温度到界面
    status_signal = pyqtSignal(str)  # 发送状态文字
    finished_signal = pyqtSignal()  # 任务结束信号

    def __init__(self, ip, port, temp_stages):
        super().__init__()
        self.ip = ip
        self.port = port
        self.temp_stages = temp_stages  # [temp1, temp2, temp3]
        self.is_running = True
        self.client = None

    def log(self, msg):
        self.log_signal.emit(msg)

    def send_command(self, func_name, address, value=None):
        """通用发送函数"""
        func = getattr(self.client, func_name)
        kwargs = {SLAVE_ARG: SLAVE_ID}
        if 'write' in func_name:
            return func(address, value, **kwargs)
        else:
            return func(address, count=1, **kwargs)

    def get_current_temp(self):
        """读取当前温度"""
        res = self.send_command('read_holding_registers', REG_CUR_TEMP)
        if not res.isError():
            raw = res.registers[0]
            if raw > 32767: raw -= 65536
            temp = raw / 10.0
            self.temp_signal.emit(temp)  # 更新界面温度
            return temp
        return None

    def set_target_temp(self, temp_val):
        """设定目标温度"""
        target_int = int(temp_val * 10)
        # 负数补码处理
        if target_int < 0:
            write_val = 65536 + target_int
        else:
            write_val = target_int

        res = self.send_command('write_register', REG_SET_TEMP, write_val)
        if not res.isError():
            self.log(f"✅ 目标温度已设为: {temp_val}℃ (Reg: {write_val})")
            return True
        else:
            self.log(f"❌ 设定温度失败: {res}")
            return False

    def wait_for_temp(self, target):
        """智能等待温度到达"""
        # 初次读取判断是升温还是降温
        curr = self.get_current_temp()
        if curr is None: return

        if curr > target:
            mode = 'cool'
            self.log(f"开始降温至 {target}℃...")
        else:
            mode = 'heat'
            self.log(f"开始升温至 {target}℃...")

        while self.is_running:
            curr = self.get_current_temp()
            if curr is not None:
                # 判断是否到达
                if mode == 'cool' and curr <= target:
                    self.log(f"已降温至 {target}℃！")
                    break
                elif mode == 'heat' and curr >= target:
                    self.log(f"已升温至 {target}℃！")
                    break
            else:
                self.log("⚠️ 读取错误，重试中...")

            # 线程休眠，避免频繁请求
            for _ in range(20):  # 2秒延迟，拆分成小段以便快速响应停止
                if not self.is_running: return
                time.sleep(0.1)

    def stop_device(self):
        """发送停止指令"""
        if self.client and self.client.connected:
            try:
                self.send_command('write_coil', COIL_STOP, True)
                self.log("🛑 发送停止指令成功")
            except Exception as e:
                self.log(f"发送停止指令失败: {e}")

    def run(self):
        self.client = ModbusTcpClient(self.ip, port=self.port)
        self.status_signal.emit("正在连接...")

        if not self.client.connect():
            self.log("❌ 无法连接设备")
            self.status_signal.emit("连接失败")
            self.finished_signal.emit()
            return

        try:
            self.log("✅ 设备已连接")
            self.status_signal.emit("运行中")

            # 1. 确保定值模式
            self.send_command('write_register', REG_MODE, 0)
            time.sleep(0.5)

            # 2. 启动设备
            self.send_command('write_coil', COIL_START, True)
            self.log("设备已启动")
            self.log("-" * 30)

            # === 执行三段逻辑 ===
            for i, target in enumerate(self.temp_stages):
                if not self.is_running: break

                stage_name = f"第 {i + 1} 阶段"
                self.status_signal.emit(f"{stage_name}: 目标 {target}℃")
                self.log(f"=== {stage_name} ===")

                if self.set_target_temp(target):
                    self.wait_for_temp(target)

                if not self.is_running: break
                self.log("⏳ 阶段缓冲 (2秒)...")
                time.sleep(2)

            if self.is_running:
                self.log("✅ 所有阶段任务完成！")
                self.status_signal.emit("任务完成")

        except Exception as e:
            self.log(f"❌ 运行错误: {e}")
        finally:
            self.stop_device()
            self.client.close()
            self.log("🔌 连接已关闭")
            if self.is_running:  # 如果是正常结束
                self.finished_signal.emit()

    def emergency_stop(self):
        """紧急停止被触发"""
        self.is_running = False
        self.log("正在执行紧急停止...")
        # 此时 run() 中的循环会中断，并进入 finally 块发送停止指令


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("恒温箱控制系统")
        self.resize(500, 600)

        self.worker = None
        self.init_ui()

    def init_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout()

        # 连接设置
        conn_group = QGroupBox("连接设置")
        conn_layout = QHBoxLayout()
        self.ip_input = QLineEdit(DEVICE_IP_DEFAULT)
        self.ip_input.setPlaceholderText("IP地址")
        self.port_input = QLineEdit(str(DEVICE_PORT_DEFAULT))
        self.port_input.setPlaceholderText("端口")
        self.port_input.setFixedWidth(60)
        conn_layout.addWidget(QLabel("IP:"))
        conn_layout.addWidget(self.ip_input)
        conn_layout.addWidget(QLabel("Port:"))
        conn_layout.addWidget(self.port_input)
        conn_group.setLayout(conn_layout)
        layout.addWidget(conn_group)

        # 实时状态
        status_group = QGroupBox("实时状态")
        status_layout = QGridLayout()

        self.lbl_temp = QLabel("--.-- ℃")
        self.lbl_temp.setStyleSheet("font-size: 32px; color: blue; font-weight: bold;")
        self.lbl_temp.setAlignment(Qt.AlignmentFlag.AlignCenter)

        self.lbl_status = QLabel("待机")
        self.lbl_status.setStyleSheet("font-size: 16px; color: gray;")
        self.lbl_status.setAlignment(Qt.AlignmentFlag.AlignCenter)

        status_layout.addWidget(QLabel("当前箱内温度:"), 0, 0)
        status_layout.addWidget(self.lbl_temp, 1, 0)
        status_layout.addWidget(QLabel("运行状态:"), 2, 0)
        status_layout.addWidget(self.lbl_status, 3, 0)
        status_group.setLayout(status_layout)
        layout.addWidget(status_group)

        # 阶段设置
        stage_group = QGroupBox("温度阶段设定")
        stage_layout = QGridLayout()

        self.stage_inputs = []
        defaults = [-35.0, 35.0, 70.0]

        for i in range(3):
            stage_layout.addWidget(QLabel(f"阶段 {i + 1} 目标 (°C):"), i, 0)
            inp = QLineEdit(str(defaults[i]))
            stage_layout.addWidget(inp, i, 1)
            self.stage_inputs.append(inp)

        stage_group.setLayout(stage_layout)
        layout.addWidget(stage_group)

        # 控制按钮
        btn_layout = QHBoxLayout()
        self.btn_start = QPushButton("启动程序")
        self.btn_start.setFixedHeight(50)
        self.btn_start.setStyleSheet("background-color: #4CAF50; color: white; font-size: 16px;")
        self.btn_start.clicked.connect(self.start_process)

        self.btn_stop = QPushButton("紧急停止")
        self.btn_stop.setFixedHeight(50)
        self.btn_stop.setStyleSheet("background-color: #f44336; color: white; font-size: 16px;")
        self.btn_stop.clicked.connect(self.stop_process)
        self.btn_stop.setEnabled(False)

        btn_layout.addWidget(self.btn_start)
        btn_layout.addWidget(self.btn_stop)
        layout.addLayout(btn_layout)

        # 日志窗口
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet("background-color: #f0f0f0; font-family: Consolas;")
        layout.addWidget(QLabel("运行日志:"))
        layout.addWidget(self.log_text)

        main_widget.setLayout(layout)

    def log(self, msg):
        self.log_text.append(msg)
        # 滚动到底部
        sb = self.log_text.verticalScrollBar()
        sb.setValue(sb.maximum())

    def update_temp(self, val):
        self.lbl_temp.setText(f"{val:.1f} ℃")

    def update_status(self, text):
        self.lbl_status.setText(text)

    def start_process(self):
        # 获取输入值
        ip = self.ip_input.text()
        try:
            port = int(self.port_input.text())
            temps = [float(inp.text()) for inp in self.stage_inputs]
        except ValueError:
            QMessageBox.warning(self, "错误", "端口或温度输入格式不正确")
            return

        # 锁定界面
        self.btn_start.setEnabled(False)
        self.btn_stop.setEnabled(True)
        for inp in self.stage_inputs: inp.setEnabled(False)
        self.ip_input.setEnabled(False)

        self.log("-" * 30)
        self.log("启动新任务")

        # 启动线程
        self.worker = WorkerThread(ip, port, temps)
        self.worker.log_signal.connect(self.log)
        self.worker.temp_signal.connect(self.update_temp)
        self.worker.status_signal.connect(self.update_status)
        self.worker.finished_signal.connect(self.on_process_finished)
        self.worker.finished.connect(self.on_thread_exit)  # 线程结束
        self.worker.start()

    def stop_process(self):
        if self.worker and self.worker.isRunning():
            self.worker.emergency_stop()
            self.btn_stop.setEnabled(False)  # 防止重复点击
            self.update_status("正在停止...")

    def on_process_finished(self):
        # 任务自然完成
        self.log("✅ 任务逻辑执行完毕")

    def on_thread_exit(self):
        # 线程清理工作
        self.btn_start.setEnabled(True)
        self.btn_stop.setEnabled(False)
        for inp in self.stage_inputs: inp.setEnabled(True)
        self.ip_input.setEnabled(True)
        self.update_status("待机")
        self.worker = None


if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())