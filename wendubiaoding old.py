"""温度标定软件（UI版，PyQt6）

功能（按你的需求做成“先能跑的版本”，后续再迭代稳定判定/报表等细节）：
1) 扫描串口：自动识别传感器端口（发送 01 1E 00，收到同头回包视为OK），读取当前SN。
2) 批量写入自定义序列号：支持按规则生成（前缀+递增数字）或逐个编辑；写入后自动回读校验。
3) 温度标定：
   - 可选启用恒温箱(Modbus TCP)自动控温（参考你 hengwenxiangui.py 的协议）
   - 进入调试模式(01 20 00)采集A~N并写入IoTDB（可选）
   - 三温点（默认 -30/35/70）稳定判定：箱温稳定 + 传感器A稳定窗口
   - 记录每个温点的 (ADC=A, T=箱温*10 两字节有符号) 后，进入用户模式写入 0x41
   - 读回 0x40 做比对

依赖：
  pip install pyserial crcmod pymodbus PyQt6 apache-iotdb==1.2.0

注意：
  - “A稳定阈值/稳定窗口/温度容差”等参数请根据你们实际传感器波动调整。
  - 若你们“写入温度”需要用传感器F而不是箱温，可在 CalibrationWorker 里切换记录策略。
"""

from __future__ import annotations

import sys
import time
import re
import queue
import math
import threading
from dataclasses import dataclass, field
from collections import deque
from typing import Dict, List, Optional, Tuple

import serial
import crcmod.predefined
from serial.tools import list_ports

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QIntValidator
from PyQt6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QGridLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QTextEdit,
    QGroupBox,
    QTabWidget,
    QTableWidget,
    QTableWidgetItem,
    QHeaderView,
    QMessageBox,
    QCheckBox,
    QFileDialog,
    QDialog,
    QDialogButtonBox,
)

# ---- IoTDB（可选）----
Session = None
TSDataType = None
TSEncoding = None
Compressor = None
try:
    from iotdb.Session import Session

    try:
        from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
    except Exception:
        from iotdb.utils import TSDataType, TSEncoding, Compressor
except Exception:
    Session = None

# ---- 恒温箱：Modbus TCP（可选）----
try:
    import pymodbus
    from pymodbus.client import ModbusTcpClient

    DEVICE_IP_DEFAULT = '192.168.50.2'
    DEVICE_PORT_DEFAULT = 8000
    SLAVE_ID = 1

    REG_SET_TEMP = 8100
    REG_CUR_TEMP = 7991
    COIL_START = 8000
    COIL_STOP = 8001
    REG_MODE = 8108

    ver = getattr(pymodbus, '__version__', '3')
    if ver.startswith('3.1') or ver.startswith('4.'):
        SLAVE_ARG = 'device_id'
    elif ver.startswith('3.'):
        SLAVE_ARG = 'slave'
    else:
        SLAVE_ARG = 'unit'

except Exception:
    pymodbus = None
    ModbusTcpClient = None
    DEVICE_IP_DEFAULT = '192.168.50.2'
    DEVICE_PORT_DEFAULT = 8000
    SLAVE_ID = 1
    REG_SET_TEMP = 8100
    REG_CUR_TEMP = 7991
    COIL_START = 8000
    COIL_STOP = 8001
    REG_MODE = 8108
    SLAVE_ARG = 'slave'


# -------------------- 通用工具 --------------------


def now_ms() -> int:
    return int(time.time() * 1000)


def sanitize_sn(sn_bytes: bytes) -> str:
    s = ''.join(chr(b) if 32 <= b <= 126 else '' for b in sn_bytes).replace('\x00', '').strip()
    s = re.sub(r'[^A-Za-z0-9_]', '_', s)
    if not s or not re.match(r'^[A-Za-z_]', s):
        s = 'd_' + s
    return s or 'd_unknown'


def fmt_sn_dual(sn_bytes: bytes) -> str:
    hex_part = ' '.join(f'{b:02X}' for b in sn_bytes)
    ascii_part = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in sn_bytes)
    return f"{hex_part}    (ASCII: {ascii_part})"


def parse_line_values(line: bytes) -> Dict[str, float]:
    text = line.decode('ascii', errors='ignore')
    pairs = re.findall(r'([A-N])=\s*([+-]?\d+(?:\.\d+)?)', text)
    vals: Dict[str, float] = {}
    for key, val in pairs:
        try:
            vals[key] = float(val)
        except ValueError:
            pass
    return vals


def u16_from_int16(v: int) -> int:
    return v & 0xFFFF


def int16_from_u16(v: int) -> int:
    return v - 65536 if v >= 32768 else v


def temp_c_to_u16_tenths(temp_c: float) -> int:
    t = int(round(temp_c * 10))
    return u16_from_int16(t)


def u16_tenths_to_temp_c(u16: int) -> float:
    return int16_from_u16(u16) / 10.0


def temp_c_to_u16_offset50(temp_c: float) -> int:
    """协议：T = (温度℃ + 50) 的16位整数（单位: 1℃）。"""
    return int(round(float(temp_c) + 50.0)) & 0xFFFF


def u16_offset50_to_temp_c(u16: int) -> float:
    return float(int(u16) & 0xFFFF) - 50.0


def adc_to_u24(adc: int) -> int:
    if adc < 0:
        adc = 0
    return adc & 0xFFFFFF


# -------------------- 传感器串口封装 --------------------


class UniversalSensor:
    def __init__(self, port: str, baudrate: int = 115200, timeout: float = 0.5):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser: Optional[serial.Serial] = None
        self.lock = threading.Lock()

    @staticmethod
    def modbus_crc(data: bytes) -> bytes:
        crc16 = crcmod.predefined.mkPredefinedCrcFun('modbus')
        return crc16(data).to_bytes(2, 'little')

    def open(self) -> bool:
        try:
            self.ser = serial.Serial(self.port, self.baudrate, timeout=self.timeout)
            return True
        except Exception:
            return False

    def close(self):
        try:
            if self.ser and self.ser.is_open:
                self.ser.close()
        except Exception:
            pass

    def send_command(self, body: List[int], wait_s: float = 1.0) -> Optional[bytes]:
        """发送指令主体（不含CRC），自动补CRC并读取回包。"""
        if not self.ser or not self.ser.is_open:
            return None
        payload = bytes(body)
        full = payload + self.modbus_crc(payload)
        with self.lock:
            try:
                self.ser.reset_input_buffer()
                self.ser.write(full)
                deadline = time.time() + wait_s
                buf = bytearray()
                while time.time() < deadline:
                    n = self.ser.in_waiting
                    if n:
                        buf += self.ser.read(n)
                    else:
                        time.sleep(0.02)
                    if len(buf) >= 5:
                        break
                return bytes(buf) if buf else None
            except Exception:
                return None

    # --- SN ---
    def read_sn(self) -> Optional[bytes]:
        resp = self.send_command([0x01, 0x03, 0x00, 0x07, 0x00, 0x06], wait_s=1.0)
        if not resp or len(resp) < 3 + 12:
            return None
        if resp[0] != 0x01 or resp[1] != 0x03 or resp[2] != 0x0C:
            return None
        return resp[3:3 + 12]

    def write_sn(self, sn_bytes: bytes) -> bool:
        if len(sn_bytes) != 12:
            raise ValueError('SN 必须 12 字节')
        body = [0x01, 0x10, 0x00, 0x07, 0x00, 0x06, 0x0C] + list(sn_bytes)
        _ = self.send_command(body, wait_s=1.0)
        time.sleep(0.25)
        verify = self.read_sn()
        return verify == sn_bytes


@dataclass
class SensorDevice:
    port: str
    sensor: UniversalSensor
    current_sn: Optional[bytes] = None
    selected: bool = True
    new_sn_text: str = ''
    last_vals: Dict[str, float] = field(default_factory=dict)
    history: deque = field(default_factory=lambda: deque(maxlen=5000))  # (ts_ms, vals)
    # A值稳定判定专用缓冲：按固定间隔采样A，避免串口输出频率较高导致10分钟窗口样本被挤掉
    a_hist: deque = field(default_factory=lambda: deque(maxlen=12000))  # (ts_ms, A_float)
    last_a_hist_ms: int = 0
    status: str = 'Idle'


# -------------------- IoTDB 写入线程（单Session） --------------------


class IoTDBWriterThread(QThread):
    log_signal = pyqtSignal(str)
    status_signal = pyqtSignal(str, int, bool, str)  # device_id, ts_ms, ok, msg

    def __init__(self, host: str, port: int, user: str, password: str, storage_group: str, enable: bool = True):
        super().__init__()
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.sg = storage_group
        self.enable = enable
        self._q: "queue.Queue[Tuple[str,int,Dict[str,float]]]" = queue.Queue(maxsize=20000)
        self._stop = threading.Event()
        self._rescan = threading.Event()
        self._current_mode = 'idle'  # 'debug' | 'user'
        self.session = None
        self._known_devices = set()

    def submit(self, device_id: str, ts_ms: int, values: Dict[str, float]):
        if not self.enable:
            return
        try:
            self._q.put_nowait((device_id, ts_ms, values))
        except queue.Full:
            # 丢弃最旧（为了不把UI卡死），同时打一次日志
            try:
                _ = self._q.get_nowait()
            except Exception:
                pass
            try:
                self._q.put_nowait((device_id, ts_ms, values))
            except Exception:
                pass

    def stop(self):
        self._stop.set()

    def request_rescan(self):
        """UI触发：接触不良时请求重新扫描/重连（线程会在循环点处理）。"""
        self._rescan.set()

    def _open(self) -> bool:
        if not self.enable:
            return True
        if Session is None:
            self.log_signal.emit('⚠️ 未安装 apache-iotdb==1.2.0，已关闭入库。')
            self.enable = False
            return True
        try:
            self.session = Session(self.host, self.port, self.user, self.password)
            try:
                try:
                    self.session.open(False)
                except TypeError:
                    self.session.open()
            except Exception:
                self.session.open()
            try:
                self.session.set_storage_group(self.sg)
            except Exception:
                pass
            return True
        except Exception as e:
            self.log_signal.emit(f'❌ IoTDB 连接失败: {e}，已关闭入库。')
            self.enable = False
            return False

    def _ensure_device_schema(self, device_id: str, measurements: List[str]):
        # 如果服务器已开启自动建模，这里无需create_timeseries。
        # 为了兼容一些环境，我们只做“尽力而为”。
        if not self.enable or not self.session:
            return
        if device_id in self._known_devices:
            return
        self._known_devices.add(device_id)
        try:
            for m in measurements:
                path = f"{device_id}.{m}"
                try:
                    self.session.create_timeseries(path, TSDataType.DOUBLE, TSEncoding.GORILLA, Compressor.SNAPPY)
                except Exception:
                    pass
        except Exception:
            pass

    def run(self):
        self._open()
        last_flush = time.time()
        batch: List[Tuple[str, int, Dict[str, float]]] = []
        while not self._stop.is_set():
            try:
                item = self._q.get(timeout=0.2)
                batch.append(item)
            except queue.Empty:
                pass

            # 简单批量：0.5s 或 batch>=200
            if (time.time() - last_flush) >= 0.5 or len(batch) >= 200:
                if self.enable and self.session and batch:
                    for device_id, ts_ms, values in batch:
                        if not values:
                            continue
                        measurements = sorted(values.keys())
                        self._ensure_device_schema(device_id, measurements)
                        vals = [float(values[m]) for m in measurements]
                        dtypes = [TSDataType.DOUBLE] * len(measurements)
                        try:
                            self.session.insert_record(device_id, ts_ms, measurements, dtypes, vals)
                            try:
                                self.status_signal.emit(device_id, ts_ms, True, 'OK')
                            except Exception:
                                pass
                        except Exception as e:
                            try:
                                self.status_signal.emit(device_id, ts_ms, False, str(e))
                            except Exception:
                                pass
                            # 不要刷屏
                            self.log_signal.emit(f"⚠️ IoTDB 写入失败(将继续): {e}")
                            time.sleep(0.2)
                            break
                batch.clear()
                last_flush = time.time()

        # 退出前 flush
        try:
            if self.enable and self.session and batch:
                for device_id, ts_ms, values in batch:
                    measurements = sorted(values.keys())
                    vals = [float(values[m]) for m in measurements]
                    dtypes = [TSDataType.DOUBLE] * len(measurements)
                    self.session.insert_record(device_id, ts_ms, measurements, dtypes, vals)
                    try:
                        self.status_signal.emit(device_id, ts_ms, True, 'OK')
                    except Exception:
                        pass
        except Exception:
            pass
        try:
            if self.session:
                self.session.close()
        except Exception:
            pass


# -------------------- 传感器实时采集线程（每个传感器一个） --------------------


class SensorStreamThread(QThread):
    log_signal = pyqtSignal(str)
    sample_signal = pyqtSignal(str, dict)  # port, vals

    def __init__(self, device: SensorDevice, iotdb_writer: Optional[IoTDBWriterThread], storage_group: str,
                 downsample_ms: int = 1000):
        super().__init__()
        self.device = device
        self.iotdb_writer = iotdb_writer
        self.sg = storage_group
        self.downsample_ms = max(200, int(downsample_ms))
        self._stop = threading.Event()
        self._last_write_ms = 0

    def stop(self):
        self._stop.set()

    def run(self):
        s = self.device.sensor
        if not s.ser or not s.ser.is_open:
            self.log_signal.emit(f"[{self.device.port}] ❌ 串口未打开，无法采集")
            return
        buf = bytearray()
        while not self._stop.is_set():
            try:
                n = s.ser.in_waiting
                chunk = s.ser.read(n or 1)
                if not chunk:
                    continue
                buf.extend(chunk)
                while True:
                    idx = buf.find(b'\r\n')
                    if idx == -1:
                        break
                    line = bytes(buf[:idx])
                    buf = buf[idx + 2:]
                    vals = parse_line_values(line)
                    if not vals:
                        continue
                    ts = now_ms()

                    # 更新 device 的缓存
                    self.device.last_vals = vals
                    try:
                        self.device.history.append((ts, vals))
                    except Exception:
                        pass

                    # A值稳定判定专用：按固定间隔(默认1000ms)采样A，避免输出频率过高导致窗口样本被挤掉
                    try:
                        if 'A' in vals:
                            if (ts - int(getattr(self.device, 'last_a_hist_ms', 0))) >= 1000:
                                self.device.a_hist.append((ts, float(vals['A'])))
                                self.device.last_a_hist_ms = ts
                    except Exception:
                        pass

                    self.sample_signal.emit(self.device.port, vals)

                    # 入库（降采样）
                    if self.iotdb_writer and (ts - self._last_write_ms) >= self.downsample_ms:
                        sn = self.device.current_sn or b''
                        sn_str = sanitize_sn(sn)
                        device_id = f"{self.sg}.{sn_str}"
                        self.iotdb_writer.submit(device_id, ts, vals)
                        self._last_write_ms = ts

            except Exception as e:
                self.log_signal.emit(f"[{self.device.port}] ⚠️ 采集异常: {e}")
                time.sleep(0.2)


# -------------------- 扫描线程 --------------------


class PortScanWorker(QThread):
    log_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(list)  # List[Tuple[port, sn_bytes]]

    def __init__(self, baudrate: int = 115200):
        super().__init__()
        self.baudrate = baudrate

    def run(self):
        ports = [p.device for p in list_ports.comports()]
        found: List[Tuple[str, Optional[bytes]]] = []
        if not ports:
            self.log_signal.emit('未发现任何串口。')
            self.finished_signal.emit(found)
            return
        self.log_signal.emit('发现串口：' + ', '.join(ports))
        for dev in ports:
            s = UniversalSensor(dev, self.baudrate, timeout=0.5)
            if not s.open():
                continue
            try:
                resp = s.send_command([0x01, 0x1E, 0x00], wait_s=1.0)
                ok = bool(resp and len(resp) >= 3 and resp[0] == 0x01 and resp[1] == 0x1E and resp[2] == 0x00)
                if not ok:
                    s.close()
                    continue
                sn = s.read_sn()
                found.append((dev, sn))
                self.log_signal.emit(f'[{dev}] ✅ 识别为传感器端口; SN=' + (fmt_sn_dual(sn) if sn else 'None'))
            except Exception:
                pass
            finally:
                s.close()
        self.finished_signal.emit(found)


# -------------------- 写SN线程 --------------------


class WriteSNWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(str, str)  # port, status
    finished_signal = pyqtSignal()

    def __init__(self, devices: List[SensorDevice], baudrate: int = 115200):
        super().__init__()
        self.devices = [d for d in devices if d.selected]
        self.baudrate = baudrate
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    @staticmethod
    def _sn_text_to_12b(text: str) -> bytes:
        b = text.encode('ascii', errors='ignore')[:12]
        if len(b) < 12:
            b += b'\x00' * (12 - len(b))
        return b

    def run(self):
        if not self.devices:
            self.log_signal.emit('没有选中的传感器。')
            self.finished_signal.emit()
            return
        for d in self.devices:
            if self._stop.is_set():
                break
            if not d.new_sn_text.strip():
                self.progress_signal.emit(d.port, '跳过：未填写新SN')
                continue
            self.progress_signal.emit(d.port, '写入中...')
            s = UniversalSensor(d.port, self.baudrate, timeout=0.8)
            if not s.open():
                self.progress_signal.emit(d.port, '❌ 串口打开失败')
                continue
            try:
                # 用户模式
                _ = s.send_command([0x01, 0x1E, 0x00], wait_s=1.0)
                old = s.read_sn()
                newb = self._sn_text_to_12b(d.new_sn_text.strip())
                ok = s.write_sn(newb)
                if ok:
                    d.current_sn = newb
                    self.progress_signal.emit(d.port, '✅ 写入成功')
                    self.log_signal.emit(
                        f'[{d.port}] 写入SN OK：{fmt_sn_dual(newb)} (旧SN={fmt_sn_dual(old) if old else "None"})')
                else:
                    verify = s.read_sn()
                    self.progress_signal.emit(d.port, '❌ 校验失败')
                    self.log_signal.emit(
                        f'[{d.port}] 写入SN失败：期望={fmt_sn_dual(newb)} 实际={fmt_sn_dual(verify) if verify else "None"}')
            except Exception as e:
                self.progress_signal.emit(d.port, f'❌ 异常:{e}')
            finally:
                s.close()
        self.finished_signal.emit()


# -------------------- 恢复出厂线程 --------------------


class FactoryResetWorker(QThread):
    """一键恢复出厂（温度-Rt 或 温度-AD）。

    - 温度—Rt 标定后恢复出厂：01 3F 00
    - 温度—AD 标定后恢复出厂：01 42 00
    设备在用户模式下写入/读出，因此会先发送 01 1E 00。
    """

    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(str, str)  # port, status
    finished_signal = pyqtSignal()

    def __init__(self, devices: List[SensorDevice], baudrate: int = 115200, kind: str = 'rt'):
        super().__init__()
        self.devices = [d for d in devices if d.selected]
        self.baudrate = baudrate
        self.kind = (kind or 'rt').lower().strip()  # 'rt' | 'ad'
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def _cmd_body(self) -> List[int]:
        if self.kind == 'ad':
            return [0x01, 0x42, 0x00]
        return [0x01, 0x3F, 0x00]

    def _cmd_name(self) -> str:
        return '温度-AD恢复出厂(0x42)' if self.kind == 'ad' else '温度-Rt恢复出厂(0x3F)'

    def run(self):
        if not self.devices:
            self.log_signal.emit('没有选中的传感器。')
            self.finished_signal.emit()
            return

        cmd = self._cmd_body()
        name = self._cmd_name()
        self.log_signal.emit(f'开始执行：{name} ...')

        for d in self.devices:
            if self._stop.is_set():
                break
            self.progress_signal.emit(d.port, '恢复出厂中...')
            s = UniversalSensor(d.port, self.baudrate, timeout=0.8)
            if not s.open():
                self.progress_signal.emit(d.port, '❌ 串口打开失败')
                continue
            try:
                # 进入用户模式（用于写入/读出）
                _ = s.send_command([0x01, 0x1E, 0x00], wait_s=1.0)
                time.sleep(0.05)
                ack = s.send_command(cmd, wait_s=1.0)
                ok = bool(ack and len(ack) >= 3 and ack[0] == 0x01 and ack[1] == cmd[1] and ack[2] == 0x00)
                if ok:
                    self.progress_signal.emit(d.port, '✅ 已恢复出厂')
                else:
                    self.progress_signal.emit(d.port, '❌ 未收到正确回包')
                    self.log_signal.emit(f'[{d.port}] {name} 回包异常: {ack.hex(" ") if ack else "None"}')
            except Exception as e:
                self.progress_signal.emit(d.port, f'❌ 异常:{e}')
            finally:
                s.close()

        self.log_signal.emit(f'{name} 流程完成。')
        self.finished_signal.emit()


# -------------------- 标定线程（总控） --------------------


class CalibrationWorker(QThread):
    log_signal = pyqtSignal(str)
    stage_signal = pyqtSignal(str)
    sensor_status_signal = pyqtSignal(str, str)  # port, status
    sample_signal = pyqtSignal(str, dict)  # port, vals
    iotdb_status_signal = pyqtSignal(str, int, bool, str)  # device_id, ts_ms, ok, msg
    finished_signal = pyqtSignal(dict)  # report

    def __init__(
            self,
            devices: List[SensorDevice],
            baudrate: int,
            # chamber
            use_chamber: bool,
            chamber_ip: str,
            chamber_port: int,
            stage_temps: List[float],
            # stability
            temp_tol: float,
            chamber_stable_s: int,
            sensor_stable_s: int,
            a_range_thresh: float,
            min_samples: int,
            # iotdb
            iotdb_enable: bool,
            iotdb_host: str,
            iotdb_port: int,
            iotdb_user: str,
            iotdb_pass: str,
            iotdb_sg: str,
            iotdb_downsample_ms: int,
            # verify
            do_60c_verify: bool,
            verify_temp: float,
            verify_wait_s: int,
            # manual points (可选)：{sn_str: [(adc, temp), ...]} 若提供则跳过三温点采集，直接写入&校验
            manual_points_by_sn: Optional[Dict[str, List[Tuple[int, float]]]] = None,
    ):
        super().__init__()
        self.devices = [d for d in devices if d.selected]
        self.baudrate = baudrate
        self.use_chamber = use_chamber and ModbusTcpClient is not None
        self.chamber_ip = chamber_ip
        self.chamber_port = chamber_port
        self.stage_temps = stage_temps
        self.chamber_tol = float(temp_tol)
        self.verify_tol = float(temp_tol)
        self.chamber_stable_s = int(chamber_stable_s)
        self.sensor_stable_s = int(sensor_stable_s)
        self.a_range_thresh = float(a_range_thresh)
        self.min_samples = int(min_samples)
        self.iotdb_enable = bool(iotdb_enable)
        self.iotdb_host = iotdb_host
        self.iotdb_port = int(iotdb_port)
        self.iotdb_user = iotdb_user
        self.iotdb_pass = iotdb_pass
        self.iotdb_sg = iotdb_sg
        self.iotdb_downsample_ms = int(iotdb_downsample_ms)
        self.do_60c_verify = bool(do_60c_verify)
        self.verify_temp = float(verify_temp)
        self.verify_wait_s = int(verify_wait_s)
        self.manual_points_by_sn = manual_points_by_sn or None
        self._stop = threading.Event()

        # UI触发：接触不良时请求重新扫描/重连（不中断流程）
        self._rescan = threading.Event()
        # 当前模式：'idle' | 'debug' | 'user'
        self._current_mode = 'idle'

        self._streams: Dict[str, SensorStreamThread] = {}
        self._iotdb_writer: Optional[IoTDBWriterThread] = None
        self._chamber_client = None
        self._ch_lock = threading.Lock()
        self._stage_start_ms = 0

        # report: sn -> info
        self.report: Dict[str, dict] = {}

    def stop(self):
        self._stop.set()

    def request_rescan(self):
        """UI触发：接触不良时请求重新扫描/重连（线程会在循环点处理）。"""
        self._rescan.set()

    # ---- chamber helpers ----
    def _ch_connect(self) -> bool:
        """(Re)connect Modbus TCP client. Return True if connected."""
        if ModbusTcpClient is None:
            return False
        try:
            if self._chamber_client:
                try:
                    self._chamber_client.close()
                except Exception:
                    pass
            # timeout helps avoid long hangs if device drops connection
            try:
                self._chamber_client = ModbusTcpClient(self.chamber_ip, port=self.chamber_port, timeout=3)
            except TypeError:
                self._chamber_client = ModbusTcpClient(self.chamber_ip, port=self.chamber_port)
            return bool(self._chamber_client.connect())
        except Exception as e:
            try:
                self.log_signal.emit(f'❌ 恒温箱连接异常: {e}')
            except Exception:
                pass
            return False

    def _ch_send(self, func_name, address, value=None):
        """Send one Modbus request, auto-reconnect on connection drop."""
        if not self._chamber_client:
            raise RuntimeError("Modbus client not initialized")
        kwargs = {SLAVE_ARG: SLAVE_ID}
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                with self._ch_lock:
                    func = getattr(self._chamber_client, func_name)
                    if 'write' in func_name:
                        return func(address, value, **kwargs)
                    return func(address, count=1, **kwargs)
            except Exception as e:
                last_err = e
                try:
                    self.log_signal.emit(f'⚠️ 恒温箱通讯异常，尝试重连({attempt + 1}/3): {e}')
                except Exception:
                    pass
                time.sleep(0.4)
                self._ch_connect()
                time.sleep(0.4)
        raise RuntimeError(f"Modbus Error: [Connection] {last_err}")

    def _ch_get_temp(self) -> Optional[float]:
        try:
            res = self._ch_send('read_holding_registers', REG_CUR_TEMP)
        except Exception:
            return None
        if res and not res.isError():
            raw = res.registers[0]
            if raw > 32767:
                raw -= 65536
            return raw / 10.0
        return None

    def _ch_set_target(self, t: float) -> bool:
        target_int = int(round(t * 10))
        write_val = 65536 + target_int if target_int < 0 else target_int
        try:
            res = self._ch_send('write_register', REG_SET_TEMP, write_val)
            return bool(res and not res.isError())
        except Exception:
            return False

    def _ch_start(self):
        # 定值模式 + start
        try:
            _ = self._ch_send('write_register', REG_MODE, 0)
            time.sleep(0.2)
            _ = self._ch_send('write_coil', COIL_START, True)
        except Exception as e:
            self.log_signal.emit(f'⚠️ 恒温箱启动指令异常(将继续): {e}')

    def _ch_stop(self):
        try:
            _ = self._ch_send('write_coil', COIL_STOP, True)
        except Exception:
            pass

    def _wait_chamber_stable(self, target: float) -> Optional[float]:
        """等待箱温进入 target±tol，并持续 chamber_stable_s 秒。"""
        if not self.use_chamber:
            return target
        self.stage_signal.emit(f'恒温箱 → 目标 {target}℃')
        if not self._ch_set_target(target):
            self.log_signal.emit('❌ 设置恒温箱目标温度失败')
            return None

        stable_start: Optional[float] = None
        last_log = 0.0
        none_streak = 0
        while not self._stop.is_set():
            if self._rescan.is_set():
                # 重新扫描主要用于传感器串口重连；箱温等待期间也允许触发
                self._maybe_rescan(reset_stage_timer=False)
            cur = self._ch_get_temp()
            if cur is None:
                none_streak += 1
                if none_streak >= 30:
                    self.log_signal.emit('⚠️ 恒温箱连续30秒读温失败：将切换为“无恒温箱模式”继续（请人工确认箱温已稳定）。')
                    self.use_chamber = False
                    return target
                time.sleep(1.0)
                continue
            none_streak = 0
            if (time.time() - last_log) > 2.0:
                self.log_signal.emit(f'箱温: {cur:.2f}℃ / 目标: {target}℃')
                last_log = time.time()

            if abs(cur - target) <= self.chamber_tol:
                if stable_start is None:
                    stable_start = time.time()
                if (time.time() - stable_start) >= self.chamber_stable_s:
                    self.log_signal.emit(f'✅ 箱温稳定：{cur:.2f}℃ (持续 {self.chamber_stable_s}s)')
                    return cur
            else:
                stable_start = None
            time.sleep(1.0)
        return None

    # ---- sensors ----
    def _open_sensors(self) -> bool:
        for d in self.devices:
            d.sensor = UniversalSensor(d.port, self.baudrate, timeout=0.6)
            if not d.sensor.open():
                self.sensor_status_signal.emit(d.port, '❌ 串口打开失败')
                return False
        return True

    def _close_sensors(self):
        for d in self.devices:
            try:
                d.sensor.close()
            except Exception:
                pass

    def _enter_debug_mode_all(self):
        self._current_mode = 'debug'
        for d in self.devices:
            _ = d.sensor.send_command([0x01, 0x20, 0x00], wait_s=1.0)
            self.sensor_status_signal.emit(d.port, '调试模式')

    def _enter_user_mode_all(self):
        self._current_mode = 'user'
        for d in self.devices:
            _ = d.sensor.send_command([0x01, 0x1E, 0x00], wait_s=1.0)
            self.sensor_status_signal.emit(d.port, '用户模式')

    def _refresh_sn_all(self):
        for d in self.devices:
            sn = d.sensor.read_sn()
            d.current_sn = sn

    def _maybe_rescan(self, reset_stage_timer: bool = True) -> bool:
        """若用户点了“接触不良，重新扫描”，在不中断流程的情况下：
        1) 扫描串口确认原端口仍存在且 SN 一致
        2) 重新打开串口，按当前模式(debug/user)恢复
        3) 若当前在采集阶段，则重新启动采集/入库（满足“包括重新入库”的需求）
        注意：为满足你提出的“端口与SN需一致才继续”，这里只做“同端口+同SN”匹配。
        """
        if not self._rescan.is_set() or self._stop.is_set():
            return False
        self._rescan.clear()
        self.log_signal.emit('🔄 接触不良：开始重新扫描并重连...')

        # 记录当前是否在采集（debug+streams）
        was_streaming = bool(self._streams)
        try:
            if was_streaming:
                self._stop_streams()
        except Exception:
            pass
        try:
            self._close_sensors()
        except Exception:
            pass

        # 扫描所有串口，找出能响应 01 1E 00 的设备并读SN
        ports = [p.device for p in list_ports.comports()]
        found: Dict[str, Optional[bytes]] = {}
        for dev in ports:
            s2 = UniversalSensor(dev, self.baudrate, timeout=0.6)
            if not s2.open():
                continue
            try:
                resp = s2.send_command([0x01, 0x1E, 0x00], wait_s=0.9)
                ok = bool(resp and len(resp) >= 3 and resp[0] == 0x01 and resp[1] == 0x1E and resp[2] == 0x00)
                if not ok:
                    continue
                sn = s2.read_sn()
                found[dev] = sn
            except Exception:
                pass
            finally:
                s2.close()

        # 校验：必须同端口且SN一致
        all_ok = True
        for d in self.devices:
            sn_expected = d.current_sn
            sn_found = found.get(d.port)
            ok = bool(sn_expected is not None and sn_found is not None and sn_found == sn_expected)
            if ok:
                self.sensor_status_signal.emit(d.port, '✅ 重新识别OK')
            else:
                all_ok = False
                self.sensor_status_signal.emit(d.port, '❌ 重新识别失败(端口/SN不匹配)')

        if not all_ok:
            self.log_signal.emit('❌ 重新扫描未能匹配全部传感器（请检查插拔/端口是否变化），将继续等待。')
            return False

        # 重新打开串口
        if not self._open_sensors():
            self.log_signal.emit('❌ 重连后串口打开失败，将继续等待。')
            return False

        # 恢复当前模式
        if self._current_mode == 'user':
            self._enter_user_mode_all()
        else:
            self._enter_debug_mode_all()
            # 采集阶段需恢复采集/入库/显示
            self._start_iotdb_and_streams()
            if reset_stage_timer:
                self._stage_start_ms = now_ms()
                for d in self.devices:
                    try:
                        d.history.clear()
                        try:
                            d.a_hist.clear()
                            d.last_a_hist_ms = 0
                        except Exception:
                            pass
                    except Exception:
                        pass

        self.log_signal.emit('✅ 重连完成，继续流程。')
        return True

    # ---- 写入阶段：接触不良处理（恢复出厂 + 使用已保存三点重写） ----
    # 说明：
    # - 三点采集完成后进入写入(0x3E/0x41)阶段时，也可能因为接触不良导致写入失败。
    # - 用户点击“接触不良，重新扫描”后，在写入阶段将执行：
    #   1) 重新扫描并确认端口/SN与之前一致（_maybe_rescan）
    #   2) 对全部传感器恢复温度-Rt与温度-AD出厂设置（01 3F 00 / 01 42 00）
    #   3) 使用本次已保存的三点数据对全部传感器重新写入（并回读校验）
    # - 这样可以避免“部分写入成功、部分失败”的中间状态，便于后续继续复核/出报告。

    def _factory_reset_rt(self, d: SensorDevice) -> bool:
        """温度—Rt 标定恢复出厂：01 3F 00"""
        try:
            r = d.sensor.send_command([0x01, 0x3F, 0x00], wait_s=1.2)
            return bool(r and len(r) >= 3 and r[0] == 0x01 and r[1] == 0x3F and r[2] == 0x00)
        except Exception:
            return False

    def _factory_reset_ad(self, d: SensorDevice) -> bool:
        """温度—AD 标定恢复出厂：01 42 00"""
        try:
            r = d.sensor.send_command([0x01, 0x42, 0x00], wait_s=1.2)
            return bool(r and len(r) >= 3 and r[0] == 0x01 and r[1] == 0x42 and r[2] == 0x00)
        except Exception:
            return False

    def _recover_write_stage_after_bad_contact(
            self,
            rt_points_by_port: Dict[str, List[Tuple[int, float]]],
            ad_points_by_port: Dict[str, List[Tuple[int, float]]],
    ) -> bool:
        """写入阶段接触不良处理：重新扫描(SN一致) -> 恢复出厂(Rt+AD) -> 用已保存三点重写(Rt+AD) -> 回读校验。

        返回 True 表示“恢复+重写”已完成（写入阶段可直接继续后续复核/结束）。
        返回 False 表示未完成（例如重新扫描未匹配全量设备），需要用户调整后再点一次“接触不良”。
        """
        if self._stop.is_set():
            return False

        # 1) 重新扫描并重连（要求端口/SN一致）
        ok_scan = self._maybe_rescan(reset_stage_timer=False)
        if not ok_scan:
            self.log_signal.emit('⚠️ 写入阶段接触不良：重新扫描未匹配全部设备（端口/SN不一致），请检查插拔/线束后再点一次“接触不良”。')
            return False

        # 2) 确保用户模式
        self._enter_user_mode_all()

        # 在报告里记录一次恢复事件（便于追溯）
        ts = time.strftime('%Y-%m-%d %H:%M:%S')
        for d in self.devices:
            sn_key = sanitize_sn(d.current_sn or b'')
            try:
                self.report[sn_key].setdefault('events', []).append(
                    {'ts': ts, 'type': 'WRITE_STAGE_CONTACT_RECOVERY', 'detail': 'factory_reset_rt_ad_then_rewrite'})
            except Exception:
                pass

        # 3) 恢复出厂（Rt + AD）
        self.log_signal.emit('🧰 写入阶段接触不良：开始对全部传感器恢复出厂（温度-Rt + 温度-AD）...')
        for d in self.devices:
            if self._stop.is_set():
                break
            ok_rt = self._factory_reset_rt(d)
            ok_ad = self._factory_reset_ad(d)
            if ok_rt and ok_ad:
                self.sensor_status_signal.emit(d.port, '✅ 已恢复出厂(Rt+AD)')
            else:
                self.sensor_status_signal.emit(d.port, f'⚠️ 恢复出厂异常(Rt={ok_rt},AD={ok_ad})')
            self.log_signal.emit(f'[{d.port}] 恢复出厂：Rt={"OK" if ok_rt else "FAIL"}，AD={"OK" if ok_ad else "FAIL"}')
            time.sleep(0.05)

        # 4) 使用已保存三点重写 Rt，并回读校验
        self.log_signal.emit('📝 写入阶段接触不良：使用已保存三点重新写入 Rt(0x3E) 并回读(0x3D)...')
        self.stage_signal.emit('接触不良处理：重写Rt并回读校验...')
        for d in self.devices:
            if self._stop.is_set():
                break
            sn_key = sanitize_sn(d.current_sn or b'')
            rt_pts = rt_points_by_port.get(d.port, [])
            if len(rt_pts) != 3:
                self.sensor_status_signal.emit(d.port, '⚠️ Rt点不足(缺E)，跳过Rt重写')
                self.report[sn_key]['rt_write_ok'] = False
                if self.report[sn_key].get('final') in (None, 'UNKNOWN'):
                    self.report[sn_key]['final'] = 'RT点不足(缺E)'
                continue

            rt_sorted = sorted(rt_pts, key=lambda x: x[1])
            # 记录“将要写入”的Rt标定点
            try:
                self.report[sn_key]['rt_write_points'] = [
                    {
                        'idx': i,
                        'temp_c': float(t),
                        't_u16_offset50': int(temp_c_to_u16_offset50(t)),
                        'rt_u24': int(rt) & 0xFFFFFF,
                        'e': (float(int(rt) & 0xFFFFFF) / 10.0),
                    }
                    for i, (rt, t) in enumerate(rt_sorted, start=1)
                ]
            except Exception:
                self.report[sn_key]['rt_write_points'] = None

            ok_write = self._write_rt_points(d, rt_sorted)
            if not ok_write:
                self.sensor_status_signal.emit(d.port, '❌ Rt重写失败')
                self.report[sn_key]['rt_write_ok'] = False
                self.report[sn_key]['final'] = 'FAIL_RT_WRITE'
                continue

            num, rb = self._read_rt_points(d)
            self.report[sn_key]['rt_write_ok'] = True
            self.report[sn_key]['rt_readback'] = {'num': num, 'points': rb}

            match = True
            rb3 = rb[:3]
            for (rt_w, t_w), (rt_r, t_r) in zip(rt_sorted, rb3):
                if int(rt_w) != int(rt_r) or int(round(t_w)) != int(round(t_r)):
                    match = False
                    break
            if match and num >= 3:
                self.sensor_status_signal.emit(d.port, '✅ Rt重写&回读一致')
            else:
                self.sensor_status_signal.emit(d.port, '⚠️ Rt回读不一致')
                self.report[sn_key]['final'] = 'FAIL_RT_READBACK'

        # 5) 使用已保存三点重写 AD，并回读校验
        self.log_signal.emit('📝 写入阶段接触不良：使用已保存三点重新写入 AD(0x41) 并回读(0x40)...')
        self.stage_signal.emit('接触不良处理：重写AD并回读校验...')
        for d in self.devices:
            if self._stop.is_set():
                break
            sn_key = sanitize_sn(d.current_sn or b'')
            ad_pts = ad_points_by_port.get(d.port, [])
            if len(ad_pts) != 3:
                self.sensor_status_signal.emit(d.port, '❌ AD点不足(未记录满3点)')
                self.report[sn_key]['ad_write_ok'] = False
                if self.report[sn_key].get('final') in (None, 'UNKNOWN'):
                    self.report[sn_key]['final'] = 'FAIL_AD_DATA'
                continue

            ad_sorted = sorted(ad_pts, key=lambda x: x[1])
            try:
                self.report[sn_key]['ad_write_points'] = [
                    {
                        'idx': i,
                        'temp_c': float(t),
                        't_u16_offset50': int(temp_c_to_u16_offset50(t)),
                        'adc': int(adc),
                        'adc_u24': int(adc_to_u24(int(adc))),
                    }
                    for i, (adc, t) in enumerate(ad_sorted, start=1)
                ]
            except Exception:
                self.report[sn_key]['ad_write_points'] = None

            ok_write = self._write_cal_points(d, ad_sorted)
            if not ok_write:
                self.sensor_status_signal.emit(d.port, '❌ AD重写失败')
                self.report[sn_key]['ad_write_ok'] = False
                self.report[sn_key]['final'] = 'FAIL_AD_WRITE'
                continue

            num, rb = self._read_cal_points(d)
            self.report[sn_key]['ad_write_ok'] = True
            self.report[sn_key]['ad_readback'] = {'num': num, 'points': rb}

            match = True
            rb3 = rb[:3]
            for (adc_w, t_w), (adc_r, t_r) in zip(ad_sorted, rb3):
                if int(adc_w) != int(adc_r) or int(round(t_w)) != int(round(t_r)):
                    match = False
                    break
            if match and num >= 3:
                self.sensor_status_signal.emit(d.port, '✅ AD重写&回读一致')
            else:
                self.sensor_status_signal.emit(d.port, '⚠️ AD回读不一致')
                if self.report[sn_key].get('final') in (None, 'UNKNOWN'):
                    self.report[sn_key]['final'] = 'FAIL_AD_READBACK'

        self.log_signal.emit('✅ 写入阶段接触不良处理完成：已恢复出厂并使用已保存三点重写。将继续后续流程。')
        return True

    def _start_iotdb_and_streams(self):
        if self.iotdb_enable:
            self._iotdb_writer = IoTDBWriterThread(
                self.iotdb_host, self.iotdb_port, self.iotdb_user, self.iotdb_pass, self.iotdb_sg, enable=True
            )
            self._iotdb_writer.log_signal.connect(self.log_signal.emit)
            try:
                self._iotdb_writer.status_signal.connect(self.iotdb_status_signal)
            except Exception:
                pass
            self._iotdb_writer.start()
        else:
            self._iotdb_writer = None

        self._streams.clear()
        for d in self.devices:
            st = SensorStreamThread(d, self._iotdb_writer, self.iotdb_sg, downsample_ms=self.iotdb_downsample_ms)
            st.log_signal.connect(self.log_signal.emit)
            try:
                st.sample_signal.connect(self.sample_signal)
            except Exception:
                pass
            st.start()
            self._streams[d.port] = st

    def _stop_streams(self):
        for st in self._streams.values():
            try:
                st.stop()
            except Exception:
                pass
        for st in self._streams.values():
            try:
                st.wait(1500)
            except Exception:
                pass
        self._streams.clear()

        if self._iotdb_writer:
            try:
                self._iotdb_writer.stop()
                self._iotdb_writer.wait(2000)
            except Exception:
                pass
            self._iotdb_writer = None

    def _get_sensor_window(self, d: SensorDevice, window_s: int) -> List[Tuple[int, Dict[str, float]]]:
        cutoff = now_ms() - int(window_s * 1000)
        items = [x for x in list(d.history) if x[0] >= cutoff]
        return items

    def _get_a_delta_info(self, d: SensorDevice) -> dict:
        """用于排查稳定判定：返回当前窗口ΔA等信息。
        使用 device.a_hist（按1000ms采样A），避免串口输出频率较高时 history(maxlen=5000) 不足以覆盖10分钟窗口。
        """
        hold_ms = int(self.sensor_stable_s * 1000)

        # 优先使用 a_hist；如不存在则回退到 history
        seq = []
        try:
            seq = list(getattr(d, 'a_hist', []))
        except Exception:
            seq = []
        if not seq:
            # fallback: 从 history 提取 A
            try:
                for ts, vals in d.history:
                    if 'A' in vals:
                        seq.append((ts, float(vals['A'])))
            except Exception:
                seq = []

        if not seq:
            return {'available': False, 'reason': 'NO_A'}

        stage_start = int(self._stage_start_ms or 0)

        # 当前A：取最新且不早于 stage_start 的样本
        cur_ts, cur_a = None, None
        for ts, a in reversed(seq):
            if stage_start and ts < stage_start:
                break
            cur_ts, cur_a = int(ts), float(a)
            break
        if cur_ts is None or cur_a is None:
            return {'available': False, 'reason': 'NO_CUR_A'}

        if stage_start and cur_ts < stage_start:
            return {'available': False, 'reason': 'BEFORE_STAGE'}

        # 统计当前已累计的A历史时长（用于提示“窗口不够”）
        first_ts = None
        for ts, _a in seq:
            if (not stage_start) or ts >= stage_start:
                first_ts = int(ts)
                break
        if first_ts is None:
            first_ts = int(seq[0][0])

        have_s = max(0.0, (cur_ts - first_ts) / 1000.0)

        # 窗口还没满
        if have_s < float(self.sensor_stable_s) - 0.5:
            return {'available': False, 'reason': 'WINDOW_NOT_REACHED', 'have_s': have_s, 'cur_a': cur_a}

        past_target = cur_ts - hold_ms
        if stage_start and past_target < stage_start:
            return {'available': False, 'reason': 'WINDOW_NOT_REACHED', 'have_s': have_s, 'cur_a': cur_a}

        past_ts, past_a = None, None
        for ts, a in seq:
            ts_i = int(ts)
            if ts_i <= past_target and ((not stage_start) or ts_i >= stage_start):
                past_ts, past_a = ts_i, float(a)
            if ts_i > past_target:
                break

        if past_a is None or past_ts is None:
            return {'available': False, 'reason': 'PAST_MISSING', 'have_s': have_s, 'cur_a': cur_a}

        delta = abs(float(cur_a) - float(past_a))
        return {
            'available': True,
            'delta': float(delta),
            'cur_a': float(cur_a),
            'past_a': float(past_a),
            'cur_ts': int(cur_ts),
            'past_ts': int(past_ts),
            'have_s': float(have_s),
        }

    def _judge_sensor_a_stable(self, d: SensorDevice) -> Optional[int]:
        """A稳定判定：窗口ΔA<=阈值，返回用于写入的ADC(A)。"""
        info = self._get_a_delta_info(d)
        if not info.get('available', False):
            return None
        if float(info.get('delta', 1e18)) <= float(self.a_range_thresh):
            return int(round(float(info.get('cur_a', 0.0))))
        return None

    # ---- 写入/回读标定点 ----

    # ---- 写入/回读标定点 ----
    # 温度-Rt标定（写：0x3E；读：0x3D）
    def _write_rt_points(self, d: SensorDevice, points: List[Tuple[int, float]]) -> bool:
        # points: [(rt_u24_int, temp_c), ...] length=3, low->high
        ack = d.sensor.send_command([0x01, 0x3E, 0x00, 0x03], wait_s=1.0)
        if not ack or ack[:3] != bytes([0x01, 0x3E, 0x00]):
            return False
        time.sleep(0.08)
        for idx, (rt_u24, temp_c) in enumerate(points, start=1):
            rt_u24 = int(rt_u24) & 0xFFFFFF
            rH = (rt_u24 >> 16) & 0xFF
            rM = (rt_u24 >> 8) & 0xFF
            rL = rt_u24 & 0xFF
            t_u16 = temp_c_to_u16_offset50(temp_c)
            tH = (t_u16 >> 8) & 0xFF
            tL = t_u16 & 0xFF
            body = [0x01, 0x3E, idx, rH, rM, rL, tH, tL]
            ack2 = d.sensor.send_command(body, wait_s=1.0)
            if not ack2 or ack2[:3] != bytes([0x01, 0x3E, idx]):
                return False
            time.sleep(0.08)
        return True

    def _read_rt_points(self, d: SensorDevice) -> Tuple[int, List[Tuple[int, float]]]:
        num = 0
        pts: List[Tuple[int, float]] = []
        r0 = d.sensor.send_command([0x01, 0x3D, 0x00], wait_s=1.0)
        if r0 and len(r0) >= 6 and r0[0] == 0x01 and r0[1] == 0x3D and r0[2] == 0x00:
            num = int(r0[3])
        for idx in range(1, 4):
            r = d.sensor.send_command([0x01, 0x3D, idx], wait_s=1.0)
            if not r or len(r) < 8:
                continue
            if r[0] != 0x01 or r[1] != 0x3D or r[2] != idx:
                continue
            rt_u24 = (r[3] << 16) | (r[4] << 8) | r[5]
            t_u16 = (r[6] << 8) | r[7]
            pts.append((rt_u24, u16_offset50_to_temp_c(t_u16)))
        return num, pts

    # 温度-AD标定（写：0x41；读：0x40）
    def _write_cal_points(self, d: SensorDevice, points: List[Tuple[int, float]]) -> bool:
        # points: [(adc_int(A), temp_c), ...] length=3, low->high
        ack = d.sensor.send_command([0x01, 0x41, 0x00, 0x03], wait_s=1.0)
        if not ack or ack[:3] != bytes([0x01, 0x41, 0x00]):
            return False
        time.sleep(0.08)
        for idx, (adc, temp_c) in enumerate(points, start=1):
            adc_u24 = adc_to_u24(int(adc))
            aH = (adc_u24 >> 16) & 0xFF
            aM = (adc_u24 >> 8) & 0xFF
            aL = adc_u24 & 0xFF
            # 重要：新要求温度输入为 (箱温节点 + 50) 的16位整数
            t_u16 = temp_c_to_u16_offset50(temp_c)
            tH = (t_u16 >> 8) & 0xFF
            tL = t_u16 & 0xFF
            body = [0x01, 0x41, idx, aH, aM, aL, tH, tL]
            ack2 = d.sensor.send_command(body, wait_s=1.0)
            if not ack2 or ack2[:3] != bytes([0x01, 0x41, idx]):
                return False
            time.sleep(0.08)
        return True

    def _read_cal_points(self, d: SensorDevice) -> Tuple[int, List[Tuple[int, float]]]:
        num = 0
        pts: List[Tuple[int, float]] = []
        r0 = d.sensor.send_command([0x01, 0x40, 0x00], wait_s=1.0)
        if r0 and len(r0) >= 5 and r0[0] == 0x01 and r0[1] == 0x40 and r0[2] == 0x00:
            num = int(r0[3])
        for idx in range(1, 4):
            r = d.sensor.send_command([0x01, 0x40, idx], wait_s=1.0)
            if not r or len(r) < 8:
                continue
            if r[0] != 0x01 or r[1] != 0x40 or r[2] != idx:
                continue
            adc_u24 = (r[3] << 16) | (r[4] << 8) | r[5]
            t_u16 = (r[6] << 8) | r[7]
            pts.append((adc_u24, u16_offset50_to_temp_c(t_u16)))
        return num, pts

    # ---- main ----
    def run(self):
        if not self.devices:
            self.log_signal.emit('没有选中的传感器，无法标定。')
            self.finished_signal.emit({})
            return

        try:
            # 1) 连接恒温箱（可选）
            if self.use_chamber:
                if not self._ch_connect():
                    self.log_signal.emit('❌ 恒温箱连接失败，将以“无恒温箱模式”继续（需要你手动把箱温调到目标并稳定）。')
                    self.use_chamber = False
                else:
                    self.log_signal.emit('✅ 恒温箱已连接')
                    self._ch_start()
            else:
                self.log_signal.emit('ℹ️ 未启用恒温箱自动控制（或未安装pymodbus），将按目标温度进行判定。')

            # 2) 打开传感器串口
            if not self._open_sensors():
                self.log_signal.emit('❌ 串口打开失败，标定中止。')
                self.finished_signal.emit({})
                return

            # 3) 用户模式读取SN
            self.stage_signal.emit('读取SN...')
            self._enter_user_mode_all()
            self._refresh_sn_all()
            for d in self.devices:
                if not d.current_sn:
                    self.sensor_status_signal.emit(d.port, '⚠️ SN读取失败')
                else:
                    self.sensor_status_signal.emit(d.port, 'SN=' + sanitize_sn(d.current_sn))

            # 初始化报告
            for d in self.devices:
                sn_key = sanitize_sn(d.current_sn or b'')
                self.report[sn_key] = {
                    'port': d.port,
                    'sn_hex_ascii': fmt_sn_dual(d.current_sn) if d.current_sn else 'None',
                    # 点位采集信息（P1/P2/P3）：每点包含 temp/adc/e/rt 等
                    'points': {},
                    # 温度-Rt 标定（0x3E/0x3D）
                    'rt_write_ok': False,
                    'rt_readback': None,
                    'rt_write_points': None,  # 写入的Rt标定点(永久化导出用)
                    # 温度-AD 标定（0x41/0x40）
                    'ad_write_ok': False,
                    'ad_readback': None,
                    'ad_write_points': None,  # 写入的AD标定点(永久化导出用)
                    'verify_60c': None,
                    'final': 'UNKNOWN',
                }

                # 4) 准备标定点容器（支持：手动补1~2个点后，继续自动采集剩余点）
            # points_by_port[port][i] = {'adc':..., 'temp':..., 'e':..., 'rt':..., 'source': 'AUTO'/'MANUAL'}
            points_by_port: Dict[str, Dict[int, dict]] = {d.port: {} for d in self.devices}

            # 4.0 若提供手动补点：先载入已提供的点（可不足3点）
            if self.manual_points_by_sn:
                self.stage_signal.emit('手动补点：载入已提供点位，其余点将继续自动采集...')
                for d in self.devices:
                    sn_key = sanitize_sn(d.current_sn or b'')
                    pts_raw = None
                    try:
                        pts_raw = self.manual_points_by_sn.get(sn_key)
                        if (not pts_raw) and isinstance(sn_key, str):
                            pts_raw = self.manual_points_by_sn.get(sn_key.strip())
                    except Exception:
                        pts_raw = None

                    pt_map: Dict[int, dict] = {}

                    # 支持新格式：{P1:{adc,temp,e}, P2:{...}}；也兼容旧格式 list/tuple
                    if isinstance(pts_raw, dict):
                        for i in range(1, 4):
                            p = pts_raw.get(f'P{i}') or pts_raw.get(i) or pts_raw.get(str(i))
                            if not p:
                                continue
                            try:
                                a = int(float(p.get('adc')))
                                t = float(p.get('temp'))
                                e = p.get('e', None)
                                e = float(e) if e is not None and str(e) != '' else None
                                if e is None:
                                    continue
                                pt_map[i] = {'adc': a, 'temp': t, 'e': e, 'rt': int(round(e * 10.0)),
                                             'source': 'MANUAL'}
                            except Exception:
                                continue
                    elif isinstance(pts_raw, (list, tuple)):
                        norm: List[Tuple[int, float, float]] = []
                        for it in list(pts_raw)[:3]:
                            try:
                                if isinstance(it, dict):
                                    a = int(float(it.get('adc')))
                                    t = float(it.get('temp'))
                                    e = it.get('e', None)
                                    e = float(e) if e is not None and str(e) != '' else None
                                else:
                                    a = int(float(it[0]))
                                    t = float(it[1])
                                    e = None
                                    if len(it) >= 3:
                                        e = it[2]
                                        e = float(e) if e is not None and str(e) != '' else None
                                if e is None:
                                    continue
                                norm.append((a, t, e))
                            except Exception:
                                continue
                        if norm:
                            # 旧格式没有显式P1/P2/P3：按温度升序映射为P1..P3
                            norm_sorted = sorted(norm, key=lambda x: x[1])
                            for i, (a, t, e) in enumerate(norm_sorted, start=1):
                                pt_map[i] = {'adc': a, 'temp': t, 'e': e, 'rt': int(round(e * 10.0)),
                                             'source': 'MANUAL'}

                    if pt_map:
                        points_by_port[d.port].update(pt_map)
                        # 写入报告（按P1/P2/P3）
                        for i, pd in pt_map.items():
                            self.report[sn_key]['points'][f'P{i}'] = {
                                'target': None,
                                'chamber': float(pd['temp']),
                                'adc': int(pd['adc']),
                                'temp': float(pd['temp']),
                                'e': float(pd['e']),
                                'rt': int(pd['rt']),
                                'source': 'MANUAL',
                            }
                        self.sensor_status_signal.emit(d.port, f'✅ 手动点已载入: {sorted(pt_map.keys())}')
                    else:
                        self.sensor_status_signal.emit(d.port, 'ℹ️ 未提供手动点：将自动采集缺失点')

            # 4.1 判断是否仍有缺失点需要继续自动采集
            need_collect = any(len(points_by_port.get(d.port, {})) < 3 for d in self.devices)

            # 4.2 预留给写入阶段的点位列表（采集完成后统一生成，确保低温->高温顺序）
            ad_points_by_port: Dict[str, List[Tuple[int, float]]] = {d.port: [] for d in self.devices}
            rt_points_by_port: Dict[str, List[Tuple[int, float]]] = {d.port: [] for d in self.devices}  # (Rt_u24, temp)

            if need_collect:
                # 4) 进入调试模式，启动采集与入库
                self.stage_signal.emit('进入调试模式 & 开始采集...')
                self._enter_debug_mode_all()
                self._start_iotdb_and_streams()
                time.sleep(0.3)

                # 5) 三温点循环
                for si, tgt in enumerate(self.stage_temps, start=1):
                    if self._stop.is_set():
                        break
                    self.stage_signal.emit(f'温度点 {si}/3：目标 {tgt}℃')
                    missing_ports = [dd.port for dd in self.devices if si not in points_by_port.get(dd.port, {})]
                    if not missing_ports:
                        self.log_signal.emit(f'温度点{si}：所有传感器已具备点位，跳过采集。')
                        continue
                    chamber_temp = tgt
                    if self.use_chamber:
                        got = self._wait_chamber_stable(tgt)
                        if got is None:
                            self.log_signal.emit('❌ 等待恒温箱稳定失败/被停止')
                            break
                        chamber_temp = got
                    else:
                        # 无恒温箱：直接用目标温度做判定（你需要保证环境已稳定）
                        self.log_signal.emit(f'⚠️ 无恒温箱自动控制：请确保环境已稳定在 {tgt}℃ 左右')
                        chamber_temp = tgt

                    # 在本温度点开始时清空历史（避免10分钟窗口跨温度点）
                    self._stage_start_ms = now_ms()
                    for d in self.devices:
                        try:
                            d.history.clear()
                            try:
                                d.a_hist.clear()
                                d.last_a_hist_ms = 0
                            except Exception:
                                pass
                        except Exception:
                            pass

                    # 等待每个传感器：窗口达到 sensor_stable_s 后，|A(now)-A(now-窗口)| <= 阈值
                    pending = {p: next(dd for dd in self.devices if dd.port == p) for p in missing_ports}
                    start_wait = time.time()
                    last_hint = 0.0
                    last_detail = 0.0  # 超过稳定时间仍未通过时，输出未通过传感器的ΔA详情
                    last_long_warn = 0.0
                    last_ch_keepalive = 0.0
                    chamber_live = chamber_temp
                    while pending and not self._stop.is_set():
                        if self._rescan.is_set():
                            self.stage_signal.emit('重新扫描/重连中...')
                            self._maybe_rescan(reset_stage_timer=True)
                            # 重连后继续本温度点（已记录的不会回滚）
                        for port, d in list(pending.items()):
                            adc = self._judge_sensor_a_stable(d)
                            if adc is None:
                                continue

                            # 同时抓取 E（用于温度-Rt 标定：Rt = E*10）
                            e_val = d.last_vals.get('E', None)
                            if e_val is None:
                                for _ts, _vals in reversed(d.history):
                                    if 'E' in _vals:
                                        e_val = _vals.get('E')
                                        break
                            try:
                                e_val_f = float(e_val) if e_val is not None else None
                            except Exception:
                                e_val_f = None
                            rt_u24 = int(round(e_val_f * 10.0)) if e_val_f is not None else None

                            points_by_port[port][si] = {
                                'adc': int(adc),
                                'temp': float(chamber_temp),
                                'e': float(e_val_f) if e_val_f is not None else None,
                                'rt': int(rt_u24) if rt_u24 is not None else None,
                                'source': 'AUTO',
                            }

                            self.sensor_status_signal.emit(
                                port,
                                f'点{si}已记录: A={adc}  E={e_val_f if e_val_f is not None else "NA"}  T={chamber_temp:.1f}℃'
                            )
                            sn_key = sanitize_sn(d.current_sn or b'')
                            self.report[sn_key]['points'][f'P{si}'] = {
                                'target': tgt,
                                'chamber': chamber_temp,
                                'adc': int(adc),
                                'temp': float(chamber_temp),
                                'e': e_val_f,
                                'rt': rt_u24,
                                'source': 'AUTO',
                            }
                            pending.pop(port, None)

                        if pending and (time.time() - last_hint) > 10.0:
                            elapsed = time.time() - start_wait
                            self.log_signal.emit(f'温度点{si}：仍有 {len(pending)} 个传感器未满足ΔA阈值（已等待 {elapsed:.0f}s）')
                            last_hint = time.time()

                        # 超过稳定时间仍未通过：输出未通过传感器当前ΔA，便于判断是“确实漂移”还是“窗口样本不足”
                        if pending:
                            elapsed = time.time() - start_wait
                            if elapsed >= float(self.sensor_stable_s) and (time.time() - last_detail) > 10.0:
                                for _port, _d in list(pending.items())[:16]:
                                    snk = sanitize_sn(_d.current_sn or b'')
                                    info = self._get_a_delta_info(_d)
                                    if info.get('available', False):
                                        self.log_signal.emit(
                                            f'  └─ 未稳定[{snk}] ΔA={info.get("delta", 0.0):.2f}  (A_now={info.get("cur_a", 0.0):.2f}, A_{int(self.sensor_stable_s)}s_ago={info.get("past_a", 0.0):.2f})'
                                        )
                                    else:
                                        rsn = info.get('reason', 'UNKNOWN')
                                        hs = info.get('have_s', None)
                                        if hs is not None:
                                            self.log_signal.emit(f'  └─ 未稳定[{snk}] 无ΔA({rsn})  已累计A历史 {float(hs):.1f}s')
                                        else:
                                            self.log_signal.emit(f'  └─ 未稳定[{snk}] 无ΔA({rsn})')
                                last_detail = time.time()

                        if pending and (time.time() - start_wait) > (self.sensor_stable_s + 300) and (
                                time.time() - last_long_warn) > 300:
                            self.log_signal.emit(f'⚠️ 温度点{si} 等待较久（仍有 {len(pending)} 个传感器未稳定），建议检查阈值/热接触/分布。')
                            last_long_warn = time.time()

                        # 恒温箱 keep-alive（避免某些设备10分钟无请求会断开TCP）
                        if self.use_chamber and (time.time() - last_ch_keepalive) > 5.0:
                            ct = self._ch_get_temp()
                            if ct is not None:
                                chamber_live = ct
                            last_ch_keepalive = time.time()

                        time.sleep(0.5)

                # 6) 停止采集/入库
                self.stage_signal.emit('停止采集...')
                self._stop_streams()
            # 7) 用户模式写入标定点 + 回读校验

            # 7) 用户模式：先温度-Rt标定，再温度-AD标定（新流程）
            # 6.5) 统一生成写入列表（确保低温->高温顺序，且兼容“先手动补点、后自动采集剩余点”）
            for d in self.devices:
                pm = points_by_port.get(d.port, {}) or {}
                # 需要P1/P2/P3齐全才能写入
                if len(pm) < 3:
                    continue
                items = [(i, pm[i]) for i in (1, 2, 3) if i in pm]
                # 按温度升序写入（协议要求低温->高温）
                items = sorted(items, key=lambda kv: float(kv[1].get('temp', 0.0)))
                ad_points_by_port[d.port] = [(int(v.get('adc', 0)), float(v.get('temp', 0.0))) for _i, v in items]
                if all((v.get('rt', None) is not None) for _i, v in items):
                    rt_points_by_port[d.port] = [(int(v.get('rt', 0)), float(v.get('temp', 0.0))) for _i, v in items]

            self.stage_signal.emit('写入温度-Rt(0x3E) & 回读校验(0x3D)...')
            self.log_signal.emit('=== 温度-Rt 写入&回读校验 ===')
            self._enter_user_mode_all()

            # 7.1 温度-Rt：Rt = E*10（24bit）；T = (箱温节点 + 50)（16bit）
            write_stage_recovered = False
            # 若用户在写入阶段发生接触不良并点击按钮，则执行：重新扫描(SN一致)+恢复出厂(Rt+AD)+用已保存三点重写
            if self._rescan.is_set():
                self.stage_signal.emit('写入阶段：接触不良处理（恢复出厂+重写）...')
                if self._recover_write_stage_after_bad_contact(rt_points_by_port, ad_points_by_port):
                    write_stage_recovered = True

            if not write_stage_recovered:
                for d in self.devices:
                    if self._stop.is_set():
                        break
                    if self._rescan.is_set():
                        self.stage_signal.emit('写入阶段：接触不良处理（恢复出厂+重写）...')
                        if self._recover_write_stage_after_bad_contact(rt_points_by_port, ad_points_by_port):
                            write_stage_recovered = True
                            break
                    sn_key = sanitize_sn(d.current_sn or b'')
                    rt_pts = rt_points_by_port.get(d.port, [])
                    if len(rt_pts) != 3:
                        # 没有E（或中途断连导致E没采到）时，会缺Rt点
                        self.sensor_status_signal.emit(d.port, '⚠️ Rt点不足(缺E)，将跳过Rt写入')
                        self.report[sn_key]['rt_write_ok'] = False
                        self.report[sn_key]['rt_readback'] = None
                        if self.report[sn_key].get('final') in (None, 'UNKNOWN'):
                            self.report[sn_key]['final'] = 'RT点不足(缺E)'
                        continue

                    rt_sorted = sorted(rt_pts, key=lambda x: x[1])
                    # 记录“将要写入”的Rt标定点，便于结束后导出永久保存
                    try:
                        self.report[sn_key]['rt_write_points'] = [
                            {
                                'idx': i,
                                'temp_c': float(t),
                                't_u16_offset50': int(temp_c_to_u16_offset50(t)),
                                'rt_u24': int(rt) & 0xFFFFFF,
                                'e': (float(int(rt) & 0xFFFFFF) / 10.0),
                            }
                            for i, (rt, t) in enumerate(rt_sorted, start=1)
                        ]
                    except Exception:
                        self.report[sn_key]['rt_write_points'] = None
                    ok_write = self._write_rt_points(d, rt_sorted)
                    if not ok_write:
                        self.sensor_status_signal.emit(d.port, '❌ Rt写入失败')
                        self.report[sn_key]['rt_write_ok'] = False
                        self.report[sn_key]['final'] = 'FAIL_RT_WRITE'
                        continue

                    num, rb = self._read_rt_points(d)
                    self.report[sn_key]['rt_write_ok'] = True
                    self.report[sn_key]['rt_readback'] = {'num': num, 'points': rb}

                    # 比对：Rt必须一致，T按整数℃(offset50)一致
                    match = True
                    rb3 = rb[:3]
                    w_str = ', '.join([f'({t:.1f}℃,Rt={rt})' for rt, t in rt_sorted])
                    r_str = ', '.join([f'({t:.1f}℃,Rt={rt})' for rt, t in rb3])
                    for (rt_w, t_w), (rt_r, t_r) in zip(rt_sorted, rb3):
                        if int(rt_w) != int(rt_r):
                            match = False
                            break
                        if int(round(t_w)) != int(round(t_r)):
                            match = False
                            break
                    if match and num >= 3:
                        self.sensor_status_signal.emit(d.port, '✅ Rt写入&回读一致')
                        self.log_signal.emit(f'[{d.port}] Rt写入: {w_str} | 回读: {r_str} -> 一致')
                    else:
                        self.sensor_status_signal.emit(d.port, '⚠️ Rt回读不一致')
                        self.log_signal.emit(f'[{d.port}] Rt写入: {w_str} | 回读: {r_str} -> 不一致')
                        self.report[sn_key]['final'] = 'FAIL_RT_READBACK'



            else:
                self.log_signal.emit('ℹ️ 写入阶段已通过“接触不良处理”完成Rt+AD重写与回读，跳过常规Rt写入。')

            # 7.2 温度-AD：ADC=A（24bit）；T = (箱温节点 + 50)（16bit）
            if not write_stage_recovered:
                self.stage_signal.emit('写入温度-AD(0x41) & 回读校验(0x40)...')
                self.log_signal.emit('=== 温度-AD 写入&回读校验 ===')
                for d in self.devices:
                    if self._stop.is_set():
                        break
                    if self._rescan.is_set():
                        self.stage_signal.emit('写入阶段：接触不良处理（恢复出厂+重写）...')
                        if self._recover_write_stage_after_bad_contact(rt_points_by_port, ad_points_by_port):
                            write_stage_recovered = True
                            break
                    sn_key = sanitize_sn(d.current_sn or b'')
                    ad_pts = ad_points_by_port.get(d.port, [])
                    if len(ad_pts) != 3:
                        self.sensor_status_signal.emit(d.port, '❌ AD点不足(未记录满3点)')
                        self.report[sn_key]['ad_write_ok'] = False
                        self.report[sn_key]['final'] = self.report[sn_key].get('final') or 'FAIL_AD_DATA'
                        continue

                    ad_sorted = sorted(ad_pts, key=lambda x: x[1])
                    # 记录“将要写入”的AD标定点，便于结束后导出永久保存
                    try:
                        self.report[sn_key]['ad_write_points'] = [
                            {
                                'idx': i,
                                'temp_c': float(t),
                                't_u16_offset50': int(temp_c_to_u16_offset50(t)),
                                'adc': int(adc),
                                'adc_u24': int(adc_to_u24(int(adc))),
                            }
                            for i, (adc, t) in enumerate(ad_sorted, start=1)
                        ]
                    except Exception:
                        self.report[sn_key]['ad_write_points'] = None
                    ok_write = self._write_cal_points(d, ad_sorted)
                    if not ok_write:
                        self.sensor_status_signal.emit(d.port, '❌ AD写入失败')
                        self.report[sn_key]['ad_write_ok'] = False
                        self.report[sn_key]['final'] = 'FAIL_AD_WRITE'
                        continue

                    num, rb = self._read_cal_points(d)
                    self.report[sn_key]['ad_write_ok'] = True
                    self.report[sn_key]['ad_readback'] = {'num': num, 'points': rb}

                    match = True
                    rb3 = rb[:3]
                    w_str = ', '.join([f'({t:.1f}℃,AD={adc})' for adc, t in ad_sorted])
                    r_str = ', '.join([f'({t:.1f}℃,AD={adc})' for adc, t in rb3])
                    for (adc_w, t_w), (adc_r, t_r) in zip(ad_sorted, rb3):
                        if int(adc_w) != int(adc_r):
                            match = False
                            break
                        if int(round(t_w)) != int(round(t_r)):
                            match = False
                            break
                    if match and num >= 3:
                        self.sensor_status_signal.emit(d.port, '✅ AD写入&回读一致')
                        self.log_signal.emit(f'[{d.port}] AD写入: {w_str} | 回读: {r_str} -> 一致')
                    else:
                        self.sensor_status_signal.emit(d.port, '⚠️ AD回读不一致')
                        self.log_signal.emit(f'[{d.port}] AD写入: {w_str} | 回读: {r_str} -> 不一致')
                        if self.report[sn_key].get('final') in (None, 'UNKNOWN'):
                            self.report[sn_key]['final'] = 'FAIL_AD_READBACK'



            else:
                self.log_signal.emit('ℹ️ 写入阶段已通过“接触不良处理”完成Rt+AD重写与回读，跳过常规AD写入。')

            # 8) 60℃复核（可选）——箱温稳定后等待指定时间，再只在结束时判断一次
            if self.do_60c_verify and not self._stop.is_set():
                wait_s = max(0, int(self.verify_wait_s))
                self.stage_signal.emit(f'{self.verify_temp}℃ 复核（箱温稳定后等待 {wait_s}s）...')
                if self.use_chamber:
                    # 重要：从“最后一个温度点 → 60℃”的过程中也保持调试模式采集/入库/显示A~N
                    self._enter_debug_mode_all()
                    self._start_iotdb_and_streams()

                    got = self._wait_chamber_stable(self.verify_temp)
                    if got is None:
                        self.log_signal.emit('⚠️ 60℃复核：恒温箱等待失败，跳过复核。')
                        self._stop_streams()
                    else:
                        chamber_temp = float(got)
                        self.log_signal.emit(f'✅ 60℃复核：箱温已稳定({chamber_temp:.2f}℃)，开始倒计时 {wait_s}s...')

                        # 清空历史，避免跨阶段
                        self._stage_start_ms = now_ms()
                        for d in self.devices:
                            try:
                                d.history.clear()
                                try:
                                    d.a_hist.clear()
                                    d.last_a_hist_ms = 0
                                except Exception:
                                    pass
                            except Exception:
                                pass

                        # 为“结束时判定”收集最近几秒的 F/箱温，减少瞬时抖动
                        f_hist: Dict[str, deque] = {d.port: deque(maxlen=5) for d in self.devices}
                        chamber_hist: deque = deque(maxlen=10)

                        verify_start = time.time()
                        last_ch_keepalive = 0.0
                        chamber_live = chamber_temp
                        last_stage_emit = 0.0
                        last_log_emit = 0.0

                        def _avg(vals):
                            try:
                                return sum(vals) / float(len(vals)) if vals else None
                            except Exception:
                                return None

                        while not self._stop.is_set() and (time.time() - verify_start) < wait_s:
                            if self._rescan.is_set():
                                self.stage_signal.emit('60℃复核：重新扫描/重连中...')
                                okr = self._maybe_rescan(reset_stage_timer=True)
                                if okr:
                                    # 重连后重新计时，确保稳定后等待时间连续
                                    verify_start = time.time()
                                    last_ch_keepalive = 0.0
                                    chamber_live = chamber_temp
                                    chamber_hist.clear()
                                    for _p in f_hist:
                                        f_hist[_p].clear()
                                    self.log_signal.emit('🔁 60℃复核：重连完成，倒计时重新开始。')

                            # 周期性读取恒温箱温度
                            if self.use_chamber and (time.time() - last_ch_keepalive) > 5.0:
                                ct = self._ch_get_temp()
                                if ct is not None:
                                    chamber_live = float(ct)
                                    chamber_hist.append(chamber_live)
                                last_ch_keepalive = time.time()

                            # 收集各传感器最新 F（F为真实温度的100倍，复核比较用 F/100）
                            for d in self.devices:
                                f_raw = d.last_vals.get('F', None)
                                if f_raw is None:
                                    continue
                                try:
                                    f_c = float(f_raw) / 100.0
                                except Exception:
                                    continue
                                f_hist[d.port].append(f_c)

                            elapsed = int(time.time() - verify_start)
                            remain = max(0, int(wait_s) - elapsed)

                            # UI显示：每秒更新一次剩余时间
                            if (time.time() - last_stage_emit) >= 1.0:
                                mm = remain // 60
                                ss = remain % 60
                                self.stage_signal.emit(f'60℃复核倒计时：{mm:02d}:{ss:02d}（箱温 {chamber_live:.2f}℃）')
                                last_stage_emit = time.time()

                            # 日志：每10秒输出一次，避免刷屏
                            if (time.time() - last_log_emit) >= 10.0:
                                self.log_signal.emit(f'60℃复核：剩余{remain}s，箱温 {chamber_live:.2f}℃')
                                last_log_emit = time.time()

                            time.sleep(1.0)

                        # 倒计时结束，仅在此刻做一次判定
                        self.stage_signal.emit('60℃复核：倒计时结束，判定中...')
                        chamber_final = _avg(list(chamber_hist)) if len(chamber_hist) else float(chamber_live)

                        for d in self.devices:
                            sn_key = sanitize_sn(d.current_sn or b'')
                            f_final = _avg(list(f_hist[d.port]))
                            if f_final is None:
                                self.report[sn_key]['verify_60c'] = {
                                    'chamber_stable': chamber_temp,
                                    'chamber_final': chamber_final,
                                    'f_final': None,
                                    'final_err': None,
                                    'ok': False,
                                    'tol': self.verify_tol,
                                    'wait_s': wait_s,
                                    'mode': 'end_only',
                                }
                                self.report[sn_key]['final'] = '不合格，需重新标定'
                                self.sensor_status_signal.emit(d.port, '❌ 60℃无F数据')
                                self.log_signal.emit(f'[{d.port}] 60℃复核：无F数据 -> FAIL')
                                continue

                            err = float(f_final) - float(chamber_final)
                            ok = abs(err) <= self.verify_tol
                            self.report[sn_key]['verify_60c'] = {
                                'chamber_stable': chamber_temp,
                                'chamber_final': chamber_final,
                                'f_final': float(f_final),
                                'final_err': float(err),
                                'ok': bool(ok),
                                'tol': self.verify_tol,
                                'wait_s': wait_s,
                                'mode': 'end_only',
                            }
                            self.log_signal.emit(
                                f'[{d.port}] 60℃复核：F={f_final:.2f}℃ 箱温={chamber_final:.2f}℃ 误差={err:+.2f}℃ -> {"OK" if ok else "FAIL"}'
                            )
                            if ok:
                                if self.report[sn_key].get('final') in ('UNKNOWN', None, 'PASS_NO_60C'):
                                    self.report[sn_key]['final'] = 'PASS'
                                self.sensor_status_signal.emit(d.port, f'✅ 60℃复核OK (err {err:+.2f}℃)')
                            else:
                                self.report[sn_key]['final'] = '不合格，需重新标定'
                                self.sensor_status_signal.emit(d.port, f'❌ 60℃复核FAIL (err {err:+.2f}℃)')

                        self._stop_streams()
                else:
                    self.log_signal.emit('⚠️ 未启用恒温箱，60℃复核跳过。')
            # finalize
            for k, v in self.report.items():
                if v.get('final') in (None, 'UNKNOWN'):
                    rt_ok = bool(v.get('rt_write_ok') and v.get('rt_readback'))
                    ad_ok = bool(v.get('ad_write_ok') and v.get('ad_readback'))
                    if rt_ok and ad_ok:
                        v['final'] = 'PASS_NO_60C'
                    elif ad_ok and not rt_ok:
                        # Rt未做/失败但AD成功：给出明确标记，方便你们定位原因（通常会影响F输出）
                        v['final'] = 'PASS_AD_ONLY(RT缺失/失败)'
                    else:
                        v['final'] = v.get('final') or 'UNKNOWN'

            self.stage_signal.emit('完成')
            self.finished_signal.emit(self.report)

        except Exception as e:
            self.log_signal.emit(f'❌ 标定线程异常: {e}')
            self.finished_signal.emit(self.report)
        finally:
            try:
                self._stop_streams()
            except Exception:
                pass
            try:
                self._close_sensors()
            except Exception:
                pass
            try:
                if self._chamber_client:
                    self._ch_stop()
                    self._chamber_client.close()
            except Exception:
                pass


# -------------------- 实时监控弹窗 --------------------


class RealtimeMonitorDialog(QDialog):
    """显示每个传感器 A~N 实时值 + IoTDB 入库状态（尽量不让主UI臃肿）。"""

    def __init__(self, parent: QWidget, devices: List[SensorDevice], storage_group: str):
        super().__init__(parent)
        self.setWindowTitle("实时监控（A~N + IoTDB）")
        self.resize(1050, 520)
        self.devices = [d for d in devices if d.selected]
        self.sg = storage_group
        self._row_by_port: Dict[str, int] = {}
        self._row_by_device_id: Dict[str, int] = {}

        layout = QVBoxLayout()
        self.lbl = QLabel("采集中：—")
        layout.addWidget(self.lbl)

        cols = ["端口", "SN", "Last(ms)"] + [chr(c) for c in range(ord('A'), ord('N') + 1)] + ["IoTDB(最后)", "IoTDB状态"]
        self.tbl = QTableWidget(0, len(cols))
        self.tbl.setHorizontalHeaderLabels(cols)
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.tbl.horizontalHeader().setStretchLastSection(True)
        self.tbl.setAlternatingRowColors(True)
        layout.addWidget(self.tbl)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setMaximumHeight(140)
        self.log.setStyleSheet('background-color:#f6f6f6; font-family:Consolas;')
        layout.addWidget(self.log)

        self.setLayout(layout)
        self._init_rows()

    def _init_rows(self):
        self.tbl.setRowCount(0)
        self._row_by_port.clear()
        self._row_by_device_id.clear()
        for d in self.devices:
            r = self.tbl.rowCount()
            self.tbl.insertRow(r)
            self.tbl.setItem(r, 0, QTableWidgetItem(d.port))
            sn_str = sanitize_sn(d.current_sn or b'')
            self.tbl.setItem(r, 1, QTableWidgetItem(sn_str))
            self.tbl.setItem(r, 2, QTableWidgetItem(""))
            for ci, k in enumerate([chr(c) for c in range(ord('A'), ord('N') + 1)], start=3):
                self.tbl.setItem(r, ci, QTableWidgetItem(""))
            self.tbl.setItem(r, 3 + 14, QTableWidgetItem(""))
            self.tbl.setItem(r, 3 + 15, QTableWidgetItem(""))

            self._row_by_port[d.port] = r
            self._row_by_device_id[f"{self.sg}.{sn_str}"] = r

    def set_stage(self, text: str):
        self.lbl.setText("采集中：" + text)

    def append_log(self, msg: str):
        self.log.append(msg)
        sb = self.log.verticalScrollBar()
        sb.setValue(sb.maximum())

    def on_sample(self, port: str, vals: dict):
        r = self._row_by_port.get(port)
        if r is None:
            return
        self.tbl.item(r, 2).setText(str(now_ms()))
        for ci, k in enumerate([chr(c) for c in range(ord('A'), ord('N') + 1)], start=3):
            if k in vals:
                try:
                    v = vals[k]
                    if isinstance(v, float):
                        if k == 'A':
                            self.tbl.item(r, ci).setText(f"{v:.0f}")
                        else:
                            self.tbl.item(r, ci).setText(f"{v:.3f}")
                    else:
                        self.tbl.item(r, ci).setText(str(v))
                except Exception:
                    self.tbl.item(r, ci).setText(str(vals[k]))

    def on_iotdb_status(self, device_id: str, ts_ms: int, ok: bool, msg: str):
        r = self._row_by_device_id.get(device_id)
        if r is None:
            return
        self.tbl.item(r, 3 + 14).setText(str(ts_ms))
        self.tbl.item(r, 3 + 15).setText("OK" if ok else ("FAIL: " + (msg[:40] if msg else "")))
        if not ok:
            self.append_log(f"[IoTDB] {device_id} @ {ts_ms} FAIL: {msg}")


class Verify60Worker(QThread):
    """独立的 60℃（或自定义温度）复核线程。

    目的：允许在“不做温度标定”的情况下，直接对成品进行二次复核：
    - 调试模式采集A~N（以及F）并可选写入IoTDB
    - 恒温箱到达并稳定在目标温度
    - 箱温稳定后等待 verify_window_s 秒（默认10分钟），然后只在结束时比较一次：|F/100 - 箱温| <= ±tol
    """

    log_signal = pyqtSignal(str)
    stage_signal = pyqtSignal(str)
    sensor_status_signal = pyqtSignal(str, str)  # port, status
    sample_signal = pyqtSignal(str, dict)  # port, vals
    iotdb_status_signal = pyqtSignal(str, int, bool, str)  # device_id, ts_ms, ok, msg
    finished_signal = pyqtSignal(dict)  # report

    def __init__(
            self,
            devices: List[SensorDevice],
            baudrate: int,
            use_chamber: bool,
            chamber_ip: str,
            chamber_port: int,
            temp_tol: float,
            chamber_stable_s: int,
            verify_window_s: int,
            verify_temp: float,
            # iotdb
            iotdb_enable: bool,
            iotdb_host: str,
            iotdb_port: int,
            iotdb_user: str,
            iotdb_pass: str,
            iotdb_sg: str,
            iotdb_downsample_ms: int,
    ):
        super().__init__()
        self.devices = [d for d in devices if d.selected]
        self.baudrate = int(baudrate)
        self.use_chamber = bool(use_chamber) and ModbusTcpClient is not None
        self.chamber_ip = chamber_ip
        self.chamber_port = int(chamber_port)
        self.verify_tol = float(temp_tol)
        self.chamber_stable_s = int(chamber_stable_s)
        self.verify_window_s = int(verify_window_s)
        self.verify_temp = float(verify_temp)

        self.iotdb_enable = bool(iotdb_enable)
        self.iotdb_host = iotdb_host
        self.iotdb_port = int(iotdb_port)
        self.iotdb_user = iotdb_user
        self.iotdb_pass = iotdb_pass
        self.iotdb_sg = iotdb_sg
        self.iotdb_downsample_ms = int(iotdb_downsample_ms)

        self._stop = threading.Event()
        self._rescan = threading.Event()
        self._streams: Dict[str, SensorStreamThread] = {}
        self._iotdb_writer: Optional[IoTDBWriterThread] = None
        self._chamber_client = None
        self._ch_lock = threading.Lock()
        self._stage_start_ms = 0

        self.report: Dict[str, dict] = {}

    def stop(self):
        self._stop.set()

    def request_rescan(self):
        self._rescan.set()

    # ---- chamber helpers (copy from CalibrationWorker) ----
    def _ch_connect(self) -> bool:
        if ModbusTcpClient is None:
            return False
        try:
            if self._chamber_client:
                try:
                    self._chamber_client.close()
                except Exception:
                    pass
            try:
                self._chamber_client = ModbusTcpClient(self.chamber_ip, port=self.chamber_port, timeout=3)
            except TypeError:
                self._chamber_client = ModbusTcpClient(self.chamber_ip, port=self.chamber_port)
            return bool(self._chamber_client.connect())
        except Exception as e:
            try:
                self.log_signal.emit(f'❌ 恒温箱连接异常: {e}')
            except Exception:
                pass
            return False

    def _ch_send(self, func_name, address, value=None):
        if not self._chamber_client:
            raise RuntimeError("Modbus client not initialized")
        kwargs = {SLAVE_ARG: SLAVE_ID}
        last_err: Optional[Exception] = None
        for attempt in range(3):
            try:
                with self._ch_lock:
                    func = getattr(self._chamber_client, func_name)
                    if 'write' in func_name:
                        return func(address, value, **kwargs)
                    return func(address, count=1, **kwargs)
            except Exception as e:
                last_err = e
                try:
                    self.log_signal.emit(f'⚠️ 恒温箱通讯异常，尝试重连({attempt + 1}/3): {e}')
                except Exception:
                    pass
                time.sleep(0.4)
                self._ch_connect()
                time.sleep(0.4)
        raise RuntimeError(f"Modbus Error: [Connection] {last_err}")

    def _ch_get_temp(self) -> Optional[float]:
        try:
            res = self._ch_send('read_holding_registers', REG_CUR_TEMP)
        except Exception:
            return None
        if res and not res.isError():
            raw = res.registers[0]
            if raw > 32767:
                raw -= 65536
            return raw / 10.0
        return None

    def _ch_set_target(self, t: float) -> bool:
        target_int = int(round(t * 10))
        write_val = 65536 + target_int if target_int < 0 else target_int
        try:
            res = self._ch_send('write_register', REG_SET_TEMP, write_val)
            return bool(res and not res.isError())
        except Exception:
            return False

    def _ch_start(self):
        try:
            _ = self._ch_send('write_register', REG_MODE, 0)
            time.sleep(0.2)
            _ = self._ch_send('write_coil', COIL_START, True)
        except Exception as e:
            self.log_signal.emit(f'⚠️ 恒温箱启动指令异常(将继续): {e}')

    def _ch_stop(self):
        try:
            _ = self._ch_send('write_coil', COIL_STOP, True)
        except Exception:
            pass

    def _wait_chamber_stable(self, target: float) -> Optional[float]:
        if not self.use_chamber:
            return target
        self.stage_signal.emit(f'恒温箱 → 目标 {target}℃')
        if not self._ch_set_target(target):
            self.log_signal.emit('❌ 设置恒温箱目标温度失败')
            return None

        stable_start: Optional[float] = None
        last_log = 0.0
        none_streak = 0
        while not self._stop.is_set():
            if self._rescan.is_set():
                # 复核期间也允许重连传感器
                self._maybe_rescan(reset_timer=False)
            cur = self._ch_get_temp()
            if cur is None:
                none_streak += 1
                if none_streak >= 30:
                    self.log_signal.emit('⚠️ 恒温箱连续30秒读温失败：将切换为“无恒温箱模式”继续（请人工确认箱温已稳定）。')
                    self.use_chamber = False
                    return target
                time.sleep(1.0)
                continue
            none_streak = 0
            if (time.time() - last_log) > 2.0:
                self.log_signal.emit(f'箱温: {cur:.2f}℃ / 目标: {target}℃')
                last_log = time.time()

            if abs(cur - target) <= self.verify_tol:
                if stable_start is None:
                    stable_start = time.time()
                if (time.time() - stable_start) >= self.chamber_stable_s:
                    self.log_signal.emit(f'✅ 箱温稳定：{cur:.2f}℃ (持续 {self.chamber_stable_s}s)')
                    return cur
            else:
                stable_start = None
            time.sleep(1.0)
        return None

    # ---- sensors & streams ----
    def _open_sensors(self) -> bool:
        for d in self.devices:
            d.sensor = UniversalSensor(d.port, self.baudrate, timeout=0.6)
            if not d.sensor.open():
                self.sensor_status_signal.emit(d.port, '❌ 串口打开失败')
                return False
        return True

    def _close_sensors(self):
        for d in self.devices:
            try:
                d.sensor.close()
            except Exception:
                pass

    def _enter_debug_mode_all(self):
        for d in self.devices:
            _ = d.sensor.send_command([0x01, 0x20, 0x00], wait_s=1.0)
            self.sensor_status_signal.emit(d.port, '调试模式')

    def _start_iotdb_and_streams(self):
        if self.iotdb_enable:
            self._iotdb_writer = IoTDBWriterThread(
                self.iotdb_host, self.iotdb_port, self.iotdb_user, self.iotdb_pass, self.iotdb_sg, enable=True
            )
            self._iotdb_writer.log_signal.connect(self.log_signal.emit)
            try:
                self._iotdb_writer.status_signal.connect(self.iotdb_status_signal)
            except Exception:
                pass
            self._iotdb_writer.start()
        else:
            self._iotdb_writer = None

        self._streams.clear()
        for d in self.devices:
            st = SensorStreamThread(d, self._iotdb_writer, self.iotdb_sg, downsample_ms=self.iotdb_downsample_ms)
            st.log_signal.connect(self.log_signal.emit)
            try:
                st.sample_signal.connect(self.sample_signal)
            except Exception:
                pass
            st.start()
            self._streams[d.port] = st

    def _stop_streams(self):
        for st in self._streams.values():
            try:
                st.stop()
            except Exception:
                pass
        for st in self._streams.values():
            try:
                st.wait(1500)
            except Exception:
                pass
        self._streams.clear()

        if self._iotdb_writer:
            try:
                self._iotdb_writer.stop()
                self._iotdb_writer.wait(2000)
            except Exception:
                pass
            self._iotdb_writer = None

    def _maybe_rescan(self, reset_timer: bool = True) -> bool:
        if not self._rescan.is_set() or self._stop.is_set():
            return False
        self._rescan.clear()
        self.log_signal.emit('🔄 复核：开始重新扫描并重连...')

        was_streaming = bool(self._streams)
        try:
            if was_streaming:
                self._stop_streams()
        except Exception:
            pass
        try:
            self._close_sensors()
        except Exception:
            pass

        ports = [p.device for p in list_ports.comports()]
        found: Dict[str, Optional[bytes]] = {}
        for dev in ports:
            s2 = UniversalSensor(dev, self.baudrate, timeout=0.6)
            if not s2.open():
                continue
            try:
                resp = s2.send_command([0x01, 0x1E, 0x00], wait_s=0.9)
                ok = bool(resp and len(resp) >= 3 and resp[0] == 0x01 and resp[1] == 0x1E and resp[2] == 0x00)
                if not ok:
                    continue
                sn = s2.read_sn()
                found[dev] = sn
            except Exception:
                pass
            finally:
                s2.close()

        all_ok = True
        for d in self.devices:
            sn_expected = d.current_sn
            sn_found = found.get(d.port)
            ok = bool(sn_expected is not None and sn_found is not None and sn_found == sn_expected)
            if ok:
                self.sensor_status_signal.emit(d.port, '✅ 重新识别OK')
            else:
                all_ok = False
                self.sensor_status_signal.emit(d.port, '❌ 重新识别失败(端口/SN不匹配)')

        if not all_ok:
            self.log_signal.emit('❌ 重新扫描未能匹配全部传感器（请检查插拔/端口是否变化），将继续等待。')
            return False

        if not self._open_sensors():
            self.log_signal.emit('❌ 重连后串口打开失败，将继续等待。')
            return False

        self._enter_debug_mode_all()
        self._start_iotdb_and_streams()

        if reset_timer:
            self._stage_start_ms = now_ms()
            for d in self.devices:
                try:
                    d.history.clear()
                    try:
                        d.a_hist.clear()
                        d.last_a_hist_ms = 0
                    except Exception:
                        pass
                except Exception:
                    pass

        self.log_signal.emit('✅ 重连完成，继续复核。')
        return True

    def run(self):
        if not self.devices:
            self.log_signal.emit('没有选中的传感器。')
            self.finished_signal.emit({})
            return

        try:
            # open sensors
            if not self._open_sensors():
                self.finished_signal.emit({})
                return

            # refresh SN for report key
            for d in self.devices:
                try:
                    d.current_sn = d.sensor.read_sn()
                except Exception:
                    pass
                sn_key = sanitize_sn(d.current_sn or b'')
                self.report[sn_key] = {
                    'port': d.port,
                    'sn_hex': fmt_sn_dual(d.current_sn) if d.current_sn else '',
                    'verify_60c': None,
                    'final': 'UNKNOWN',
                }

            # chamber connect
            if self.use_chamber:
                if not self._ch_connect():
                    self.log_signal.emit('❌ 恒温箱连接失败，将切换为“无恒温箱模式”继续。')
                    self.use_chamber = False
                else:
                    self._ch_start()

            # debug + streams
            self.stage_signal.emit('进入调试模式并开始采集/入库...')
            self._enter_debug_mode_all()
            self._start_iotdb_and_streams()

            # wait chamber stable
            got = self._wait_chamber_stable(self.verify_temp)
            if got is None:
                self.log_signal.emit('❌ 复核：恒温箱等待失败，终止复核。')
                self._stop_streams()
                self.finished_signal.emit(self.report)
                return
            chamber_temp = got

            # clear history to avoid cross-stage
            self._stage_start_ms = now_ms()
            for d in self.devices:
                try:
                    d.history.clear()
                    try:
                        d.a_hist.clear()
                        d.last_a_hist_ms = 0
                    except Exception:
                        pass
                except Exception:
                    pass

            wait_s = max(0, int(self.verify_window_s))
            self.log_signal.emit(f'✅ 复核：箱温已稳定({chamber_temp:.2f}℃)，开始倒计时 {wait_s}s...')
            verify_start = time.time()
            last_ch_keepalive = 0.0
            chamber_live = chamber_temp
            last_stage_emit = 0.0
            last_log_emit = 0.0

            # 为“结束时判定”收集最近几秒的 F/箱温
            f_hist: Dict[str, deque] = {d.port: deque(maxlen=5) for d in self.devices}
            chamber_hist: deque = deque(maxlen=10)

            def _avg(vals):
                try:
                    return sum(vals) / float(len(vals)) if vals else None
                except Exception:
                    return None

            self.stage_signal.emit(f'{self.verify_temp}℃ 复核倒计时开始（{wait_s}s）...')

            while not self._stop.is_set() and (time.time() - verify_start) < wait_s:
                if self._rescan.is_set():
                    self.stage_signal.emit('复核：重新扫描/重连中...')
                    okr = self._maybe_rescan(reset_timer=True)
                    if okr:
                        verify_start = time.time()
                        last_ch_keepalive = 0.0
                        chamber_live = chamber_temp
                        chamber_hist.clear()
                        for _p in f_hist:
                            f_hist[_p].clear()
                        self.log_signal.emit('🔁 复核：重连完成，倒计时重新开始。')

                if self.use_chamber and (time.time() - last_ch_keepalive) > 5.0:
                    ct = self._ch_get_temp()
                    if ct is not None:
                        chamber_live = float(ct)
                        chamber_hist.append(chamber_live)
                    last_ch_keepalive = time.time()

                for d in self.devices:
                    f_raw = d.last_vals.get('F', None)
                    if f_raw is None:
                        continue
                    try:
                        f_c = float(f_raw) / 100.0
                    except Exception:
                        continue
                    f_hist[d.port].append(f_c)

                elapsed = int(time.time() - verify_start)
                remain = max(0, int(wait_s) - elapsed)

                if (time.time() - last_stage_emit) >= 1.0:
                    mm = remain // 60
                    ss = remain % 60
                    self.stage_signal.emit(f'复核倒计时：{mm:02d}:{ss:02d}（箱温 {chamber_live:.2f}℃）')
                    last_stage_emit = time.time()

                if (time.time() - last_log_emit) >= 10.0:
                    self.log_signal.emit(f'复核：剩余{remain}s，箱温 {chamber_live:.2f}℃')
                    last_log_emit = time.time()

                time.sleep(1.0)

            self.stage_signal.emit('复核：倒计时结束，判定中...')
            chamber_final = _avg(list(chamber_hist)) if len(chamber_hist) else float(chamber_live)

            for d in self.devices:
                sn_key = sanitize_sn(d.current_sn or b'')
                f_final = _avg(list(f_hist[d.port]))
                if f_final is None:
                    self.report[sn_key]['verify_60c'] = {
                        'chamber_stable': float(chamber_temp),
                        'chamber_final': float(chamber_final),
                        'f_final': None,
                        'final_err': None,
                        'ok': False,
                        'tol': self.verify_tol,
                        'wait_s': wait_s,
                        'mode': 'end_only',
                    }
                    self.report[sn_key]['final'] = '不合格，需重新标定'
                    self.sensor_status_signal.emit(d.port, '❌ 60℃无F数据')
                    self.log_signal.emit(f'[{d.port}] 复核：无F数据 -> FAIL')
                    continue

                err = float(f_final) - float(chamber_final)
                ok = abs(err) <= self.verify_tol
                self.report[sn_key]['verify_60c'] = {
                    'chamber_stable': float(chamber_temp),
                    'chamber_final': float(chamber_final),
                    'f_final': float(f_final),
                    'final_err': float(err),
                    'ok': bool(ok),
                    'tol': self.verify_tol,
                    'wait_s': wait_s,
                    'mode': 'end_only',
                }
                self.log_signal.emit(
                    f'[{d.port}] 复核：F={f_final:.2f}℃ 箱温={chamber_final:.2f}℃ 误差={err:+.2f}℃ -> {"OK" if ok else "FAIL"}'
                )
                if ok:
                    self.report[sn_key]['final'] = 'PASS'
                    self.sensor_status_signal.emit(d.port, f'✅ 60℃复核OK (err {err:+.2f}℃)')
                else:
                    self.report[sn_key]['final'] = '不合格，需重新标定'
                    self.sensor_status_signal.emit(d.port, f'❌ 60℃复核FAIL (err {err:+.2f}℃)')

            self.stage_signal.emit('完成')
            self.finished_signal.emit(self.report)

        except Exception as e:
            self.log_signal.emit(f'❌ 60℃复核线程异常: {e}')
            self.finished_signal.emit(self.report)
        finally:
            try:
                self._stop_streams()
            except Exception:
                pass
            try:
                self._close_sensors()
            except Exception:
                pass
            try:
                if self._chamber_client:
                    self._ch_stop()
                    self._chamber_client.close()
            except Exception:
                pass


# -------------------- UI 主窗口 --------------------


class ManualPointsDialog(QDialog):
    """手动补点：为每个传感器录入/修正三温点 (ADC=A, 温度=℃)。
    支持：部分点来自上次自动结果（预填），其余手动补齐；也支持全手动。
    """

    def __init__(self, parent: QWidget, devices: List[SensorDevice], stage_temps: List[float],
                 last_report: Optional[dict] = None, cache_points: Optional[dict] = None):
        super().__init__(parent)
        self.setWindowTitle("手动补点 / 继续写入&复核")
        self.resize(980, 520)
        self.devices = [d for d in devices if d.selected]
        self.stage_temps = stage_temps[:] if stage_temps else [-30.0, 35.0, 70.0]
        self.last_report = last_report or {}
        self.cache_points = cache_points or {}
        self.points_by_sn: Dict[str, Dict[str, dict]] = {}

        layout = QVBoxLayout()
        tip = QLabel(
            "说明：每行一个传感器。你可以只补 1 个或 2 个点（未填写的点，软件会在后续温度点继续自动采集）。\n"
            "建议：先点“导入上次自动结果”/“填充温度”，然后只修改/补齐缺失点。\n"
            "注意：温度-Rt 写入需要 E 值（Rt=E*10），因此每个已填写的点请同时填写 ADC(A) 与 E。\n"
            "继续后流程：缺失点自动采集完成后 → 温度-Rt 写入&回读 → 温度-AD 写入&回读 → （可选）60℃复核。"
        )
        tip.setWordWrap(True)
        layout.addWidget(tip)

        btn_bar = QHBoxLayout()
        self.btn_fill_temp = QPushButton("填充温度=当前三温点")
        self.btn_import = QPushButton("导入上次自动结果")
        self.btn_fill_temp.clicked.connect(self._fill_temps)
        self.btn_import.clicked.connect(self._import_points)
        btn_bar.addWidget(self.btn_fill_temp)
        btn_bar.addWidget(self.btn_import)
        btn_bar.addStretch(1)
        layout.addLayout(btn_bar)

        cols = ["端口", "SN",
                "P1_T(℃)", "P1_ADC(A)", "P1_E",
                "P2_T(℃)", "P2_ADC(A)", "P2_E",
                "P3_T(℃)", "P3_ADC(A)", "P3_E"]
        self.tbl = QTableWidget(0, len(cols))
        self.tbl.setHorizontalHeaderLabels(cols)
        self.tbl.horizontalHeader().setStretchLastSection(True)
        self.tbl.setEditTriggers(QTableWidget.EditTrigger.DoubleClicked | QTableWidget.EditTrigger.EditKeyPressed)
        layout.addWidget(self.tbl)

        self._init_rows()
        self._fill_temps()
        self._import_points(silent=True)

        # buttons
        bb = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        bb.accepted.connect(self._on_ok)
        bb.rejected.connect(self.reject)
        layout.addWidget(bb)

        self.setLayout(layout)

    def _init_rows(self):
        self.tbl.setRowCount(0)
        for d in self.devices:
            r = self.tbl.rowCount()
            self.tbl.insertRow(r)
            self.tbl.setItem(r, 0, QTableWidgetItem(d.port))
            sn_key = sanitize_sn(d.current_sn or b'')
            it_sn = QTableWidgetItem(sn_key)
            it_sn.setFlags(it_sn.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.tbl.setItem(r, 1, it_sn)
            for c in range(2, 11):
                it = QTableWidgetItem("")
                self.tbl.setItem(r, c, it)

    def _fill_temps(self):
        temps = self.stage_temps[:3]
        while len(temps) < 3:
            temps.append(float(temps[-1]) if temps else 0.0)
        for r in range(self.tbl.rowCount()):
            self.tbl.item(r, 2).setText(f"{temps[0]:.1f}")
            self.tbl.item(r, 5).setText(f"{temps[1]:.1f}")
            self.tbl.item(r, 8).setText(f"{temps[2]:.1f}")

    def _import_points(self, silent: bool = False):
        """从上次报告 / 缓存预填 ADC、E 与温度。优先：last_report -> cache_points"""

        def get_pts_map(sn: str) -> Optional[Dict[int, Tuple[int, float, Optional[float]]]]:
            # 1) last_report
            info = self.last_report.get(sn)
            if info and isinstance(info, dict):
                pts = info.get('points') or {}
                out: Dict[int, Tuple[int, float, Optional[float]]] = {}
                for i in range(1, 4):
                    p = pts.get(f'P{i}')
                    if p and 'adc' in p and 'temp' in p:
                        e = p.get('e', None)
                        e_val = float(e) if e is not None and str(e) != '' else None
                        out[i] = (int(p['adc']), float(p['temp']), e_val)
                if out:
                    return out

            # 2) cache_points
            cp = self.cache_points.get(sn)
            if cp and isinstance(cp, dict):
                out: Dict[int, Tuple[int, float, Optional[float]]] = {}
                for i in range(1, 4):
                    p = cp.get(f'P{i}')
                    if p and 'adc' in p and 'temp' in p:
                        e = p.get('e', None)
                        e_val = float(e) if e is not None and str(e) != '' else None
                        out[i] = (int(p['adc']), float(p['temp']), e_val)
                if out:
                    return out
            return None

        imported = 0
        for r in range(self.tbl.rowCount()):
            sn = (self.tbl.item(r, 1).text() or "").strip()
            mp = get_pts_map(sn)
            if not mp:
                continue
            for i in sorted(mp.keys()):
                adc, temp, e = mp[i]
                tc = 2 + (i - 1) * 3
                ac = 3 + (i - 1) * 3
                ec = 4 + (i - 1) * 3
                if self.tbl.item(r, tc).text().strip() == "":
                    self.tbl.item(r, tc).setText(f"{float(temp):.1f}")
                if self.tbl.item(r, ac).text().strip() == "":
                    self.tbl.item(r, ac).setText(str(int(adc)))
                if e is not None and self.tbl.item(r, ec).text().strip() == "":
                    self.tbl.item(r, ec).setText(f"{float(e):.3f}")
            imported += 1

        if (not silent) and imported:
            QMessageBox.information(self, "提示", f"已为 {imported} 个传感器预填上次点位数据（如有）。")

    def _on_ok(self):
        # points[sn]['P1'/'P2'/'P3'] = {'adc': int, 'temp': float, 'e': float}
        points: Dict[str, Dict[str, dict]] = {}
        errors: List[str] = []
        for r in range(self.tbl.rowCount()):
            sn = (self.tbl.item(r, 1).text() or "").strip()
            row_map: Dict[str, dict] = {}
            for i in range(1, 4):
                tc = 2 + (i - 1) * 3
                ac = 3 + (i - 1) * 3
                ec = 4 + (i - 1) * 3
                t_txt = (self.tbl.item(r, tc).text() or "").strip()
                a_txt = (self.tbl.item(r, ac).text() or "").strip()
                e_txt = (self.tbl.item(r, ec).text() or "").strip()

                if t_txt == "" and a_txt == "" and e_txt == "":
                    continue

                # 点位填写不完整
                if t_txt == "" or a_txt == "" or e_txt == "":
                    errors.append(f"[{sn}] P{i} 点不完整（需同时填写 温度/ADC/E；未填写的点请全部留空交给软件自动采集）")
                    continue

                # parse
                try:
                    t = float(t_txt)
                except Exception:
                    errors.append(f"[{sn}] P{i} 温度格式错误")
                    continue
                try:
                    a = int(float(a_txt))
                except Exception:
                    errors.append(f"[{sn}] P{i} ADC格式错误")
                    continue
                if a < 0 or a > 99999:
                    errors.append(f"[{sn}] P{i} ADC超范围(0-99999)")
                    continue
                try:
                    e_val = float(e_txt)
                except Exception:
                    errors.append(f"[{sn}] P{i} E格式错误")
                    continue

                row_map[f'P{i}'] = {'adc': int(a), 'temp': float(t), 'e': float(e_val)}

            if len(row_map) == 0:
                errors.append(f"[{sn}] 未提供任何点位（可只补1个点，但不能全空）")
                continue

            points[sn] = row_map

        if errors:
            QMessageBox.warning(
                self,
                "输入有误",
                "请修正以下问题：\n" + "\n".join(errors[:25]) + ("" if len(errors) <= 25 else f"\n... 共 {len(errors)} 条")
            )
            return

        self.points_by_sn = points
        self.accept()

    def get_points(self) -> Dict[str, Dict[str, dict]]:
        return self.points_by_sn


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('温度标定软件（UI版 v10）')
        self.resize(1100, 760)

        self.devices: List[SensorDevice] = []
        self.baudrate = 115200

        self.scan_worker: Optional[PortScanWorker] = None
        self.sn_worker: Optional[WriteSNWorker] = None
        self.reset_worker: Optional[FactoryResetWorker] = None
        self.cal_worker: Optional[CalibrationWorker] = None
        self.verify_worker: Optional['Verify60Worker'] = None
        self._last_report: Optional[dict] = None
        self.points_cache: Dict[str, Dict[str, dict]] = {}
        self.monitor_dialog: Optional[RealtimeMonitorDialog] = None

        self._build_ui()

    # ---- UI helpers ----
    def log(self, msg: str):
        self.log_text.append(msg)
        sb = self.log_text.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout()

        # top controls
        top = QGroupBox('全局设置')
        top_l = QGridLayout()
        self.ed_baud = QLineEdit(str(self.baudrate))
        self.ed_baud.setFixedWidth(120)
        top_l.addWidget(QLabel('串口波特率:'), 0, 0)
        top_l.addWidget(self.ed_baud, 0, 1)
        self.btn_scan = QPushButton('扫描传感器端口')
        self.btn_scan.clicked.connect(self.on_scan)
        top_l.addWidget(self.btn_scan, 0, 2)
        self.lbl_found = QLabel('已识别: 0')
        top_l.addWidget(self.lbl_found, 0, 3)
        top.setLayout(top_l)
        layout.addWidget(top)

        tabs = QTabWidget()
        tabs.addTab(self._build_tab_sn(), '端口&序列号')
        tabs.addTab(self._build_tab_cal(), '温度标定')
        layout.addWidget(tabs)

        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setStyleSheet('background-color:#f0f0f0; font-family:Consolas;')
        layout.addWidget(QLabel('日志:'))
        layout.addWidget(self.log_text)

        root.setLayout(layout)

    def _build_tab_sn(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout()

        # table
        self.tbl = QTableWidget(0, 6)
        self.tbl.setHorizontalHeaderLabels(['选中', '端口', '当前SN(ASCII)', '当前SN(HEX)', '新SN(可编辑)', '状态'])
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.tbl.setAlternatingRowColors(True)
        v.addWidget(self.tbl)

        # SN generator
        gen = QGroupBox('批量生成/写入序列号（12字节ASCII，超长截断，不足\x00补齐）')
        gl = QGridLayout()
        self.ed_prefix = QLineEdit('SN')
        self.ed_start = QLineEdit('1')
        self.ed_width = QLineEdit('4')
        self.ed_step = QLineEdit('1')
        gl.addWidget(QLabel('前缀:'), 0, 0)
        gl.addWidget(self.ed_prefix, 0, 1)
        gl.addWidget(QLabel('起始号:'), 0, 2)
        gl.addWidget(self.ed_start, 0, 3)
        gl.addWidget(QLabel('数字宽度:'), 0, 4)
        gl.addWidget(self.ed_width, 0, 5)
        gl.addWidget(QLabel('步进:'), 0, 6)
        gl.addWidget(self.ed_step, 0, 7)
        self.btn_apply_sn = QPushButton('按规则填充新SN到表格')
        self.btn_apply_sn.clicked.connect(self.on_apply_sn_rule)
        gl.addWidget(self.btn_apply_sn, 1, 0, 1, 4)
        self.btn_write_sn = QPushButton('写入新SN并校验（选中项）')
        self.btn_write_sn.clicked.connect(self.on_write_sn)
        gl.addWidget(self.btn_write_sn, 1, 4, 1, 4)
        gen.setLayout(gl)
        v.addWidget(gen)

        w.setLayout(v)
        return w

    def _build_tab_cal(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout()

        # IoTDB
        gb_i = QGroupBox('IoTDB（你只需要保证IoTDB服务已启动；这里由软件负责连接和入库）')
        il = QGridLayout()
        self.cb_iotdb = QCheckBox('启用实时入库')
        self.cb_iotdb.setChecked(True)
        self.ed_i_host = QLineEdit('127.0.0.1')
        self.ed_i_port = QLineEdit('6667')
        self.ed_i_user = QLineEdit('root')
        self.ed_i_pass = QLineEdit('root')
        self.ed_i_sg = QLineEdit('root.h2')
        self.ed_i_down = QLineEdit('1000')
        il.addWidget(self.cb_iotdb, 0, 0)
        il.addWidget(QLabel('Host'), 0, 1)
        il.addWidget(self.ed_i_host, 0, 2)
        il.addWidget(QLabel('Port'), 0, 3)
        il.addWidget(self.ed_i_port, 0, 4)
        il.addWidget(QLabel('User'), 0, 5)
        il.addWidget(self.ed_i_user, 0, 6)
        il.addWidget(QLabel('Pass'), 0, 7)
        il.addWidget(self.ed_i_pass, 0, 8)
        il.addWidget(QLabel('StorageGroup'), 1, 1)
        il.addWidget(self.ed_i_sg, 1, 2, 1, 3)
        il.addWidget(QLabel('降采样ms'), 1, 5)
        il.addWidget(self.ed_i_down, 1, 6)
        gb_i.setLayout(il)
        v.addWidget(gb_i)

        # chamber
        gb_c = QGroupBox('恒温箱（Modbus TCP，可选）')
        cl = QGridLayout()
        self.cb_ch = QCheckBox('启用恒温箱自动控温')
        self.cb_ch.setChecked(True)
        self.ed_ch_ip = QLineEdit(DEVICE_IP_DEFAULT)
        self.ed_ch_port = QLineEdit(str(DEVICE_PORT_DEFAULT))
        cl.addWidget(self.cb_ch, 0, 0)
        cl.addWidget(QLabel('IP'), 0, 1)
        cl.addWidget(self.ed_ch_ip, 0, 2)
        cl.addWidget(QLabel('Port'), 0, 3)
        cl.addWidget(self.ed_ch_port, 0, 4)
        gb_c.setLayout(cl)
        v.addWidget(gb_c)

        # calibration params
        gb_p = QGroupBox('标定参数')
        pl = QGridLayout()
        self.ed_t1 = QLineEdit('-30')
        self.ed_t2 = QLineEdit('35')
        self.ed_t3 = QLineEdit('70')
        self.ed_tol = QLineEdit('2')
        self.ed_ch_stable = QLineEdit('120')
        self.ed_sen_stable = QLineEdit('600')
        self.ed_a_range = QLineEdit('500')
        self.ed_a_range.setValidator(QIntValidator(0, 99999, self))
        self.ed_min_samples = QLineEdit('30')
        self.cb_60 = QCheckBox('完成后做60℃复核')
        self.cb_60.setChecked(True)
        self.ed_60 = QLineEdit('60')
        self.ed_60_wait = QLineEdit('600')
        self.ed_60_wait.setValidator(QIntValidator(0, 86400, self))
        pl.addWidget(QLabel('温度点1'), 0, 0)
        pl.addWidget(self.ed_t1, 0, 1)
        pl.addWidget(QLabel('温度点2'), 0, 2)
        pl.addWidget(self.ed_t2, 0, 3)
        pl.addWidget(QLabel('温度点3'), 0, 4)
        pl.addWidget(self.ed_t3, 0, 5)
        pl.addWidget(QLabel('恒温箱目标容差/复核容差(℃)'), 1, 0)
        pl.addWidget(self.ed_tol, 1, 1)
        pl.addWidget(QLabel('箱温稳定判定(s)'), 1, 2)
        pl.addWidget(self.ed_ch_stable, 1, 3)
        pl.addWidget(QLabel('传感器稳定窗口(s)'), 1, 4)
        pl.addWidget(self.ed_sen_stable, 1, 5)
        pl.addWidget(QLabel('A值变化阈值(ΔA, 0-99999)'), 2, 0)
        pl.addWidget(self.ed_a_range, 2, 1)
        pl.addWidget(QLabel('最少样本数'), 2, 2)
        pl.addWidget(self.ed_min_samples, 2, 3)
        pl.addWidget(self.cb_60, 2, 4)
        pl.addWidget(QLabel('复核温度'), 2, 5)
        pl.addWidget(self.ed_60, 2, 6)
        pl.addWidget(QLabel('复核等待(s)'), 3, 5)
        pl.addWidget(self.ed_60_wait, 3, 6)
        gb_p.setLayout(pl)
        v.addWidget(gb_p)

        # factory reset
        gb_r = QGroupBox('恢复出厂（可选）')
        rl = QHBoxLayout()
        self.btn_reset_rt = QPushButton('恢复出厂：温度-Rt(0x3F)')
        self.btn_reset_ad = QPushButton('恢复出厂：温度-AD(0x42)')
        self.btn_reset_rt.clicked.connect(self.on_reset_rt)
        self.btn_reset_ad.clicked.connect(self.on_reset_ad)
        rl.addWidget(self.btn_reset_rt)
        rl.addWidget(self.btn_reset_ad)
        gb_r.setLayout(rl)
        v.addWidget(gb_r)

        # buttons
        btns = QHBoxLayout()
        self.btn_start_cal = QPushButton('开始标定（选中项）')
        self.btn_verify60 = QPushButton('仅60℃复核（选中项）')
        self.btn_stop_cal = QPushButton('停止')
        self.btn_rescan = QPushButton('接触不良，重新扫描')
        self.btn_export = QPushButton('导出报告(CSV)')
        self.btn_monitor = QPushButton('打开实时监控')
        self.btn_manual = QPushButton('手动补点/继续（写入&复核）')
        self.btn_stop_cal.setEnabled(False)
        self.btn_rescan.setEnabled(False)
        self.btn_export.setEnabled(False)
        self.btn_monitor.setEnabled(False)
        self.btn_manual.setEnabled(True)
        self.btn_start_cal.clicked.connect(self.on_start_cal)
        self.btn_verify60.clicked.connect(self.on_verify60)
        self.btn_stop_cal.clicked.connect(self.on_stop_cal)
        self.btn_rescan.clicked.connect(self.on_rescan)
        self.btn_export.clicked.connect(self.on_export)
        self.btn_monitor.clicked.connect(self.on_open_monitor)
        self.btn_manual.clicked.connect(self.on_manual_continue)
        btns.addWidget(self.btn_start_cal)
        btns.addWidget(self.btn_verify60)
        btns.addWidget(self.btn_stop_cal)
        btns.addWidget(self.btn_rescan)
        btns.addWidget(self.btn_export)
        btns.addWidget(self.btn_monitor)
        btns.addWidget(self.btn_manual)
        v.addLayout(btns)

        self.lbl_stage = QLabel('状态：待机')
        self.lbl_stage.setStyleSheet('font-size:16px;')
        v.addWidget(self.lbl_stage)

        w.setLayout(v)
        return w

    # ---- table utils ----
    def _sync_baud(self):
        try:
            self.baudrate = int(self.ed_baud.text().strip())
        except Exception:
            self.baudrate = 115200
            self.ed_baud.setText(str(self.baudrate))

    def _rebuild_table(self):
        self.tbl.setRowCount(0)
        for d in self.devices:
            self._add_row(d)
        self.lbl_found.setText(f'已识别: {len(self.devices)}')

    def _add_row(self, d: SensorDevice):
        r = self.tbl.rowCount()
        self.tbl.insertRow(r)
        # checkbox
        cb = QTableWidgetItem()
        cb.setFlags(Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled)
        cb.setCheckState(Qt.CheckState.Checked if d.selected else Qt.CheckState.Unchecked)
        self.tbl.setItem(r, 0, cb)
        self.tbl.setItem(r, 1, QTableWidgetItem(d.port))

        ascii_sn = ''
        hex_sn = ''
        if d.current_sn:
            ascii_sn = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in d.current_sn).rstrip('\x00')
            hex_sn = ' '.join(f'{b:02X}' for b in d.current_sn)
        self.tbl.setItem(r, 2, QTableWidgetItem(ascii_sn))
        self.tbl.setItem(r, 3, QTableWidgetItem(hex_sn))

        it_new = QTableWidgetItem(d.new_sn_text)
        it_new.setFlags(it_new.flags() | Qt.ItemFlag.ItemIsEditable)
        self.tbl.setItem(r, 4, it_new)
        self.tbl.setItem(r, 5, QTableWidgetItem(d.status))

    def _collect_table_to_devices(self):
        # sync selection + new_sn_text back to model
        for i, d in enumerate(self.devices):
            it_cb = self.tbl.item(i, 0)
            if it_cb:
                d.selected = (it_cb.checkState() == Qt.CheckState.Checked)
            it_new = self.tbl.item(i, 4)
            if it_new:
                d.new_sn_text = it_new.text()

    def _set_row_status(self, port: str, status: str):
        for i, d in enumerate(self.devices):
            if d.port == port:
                d.status = status
                it = self.tbl.item(i, 5)
                if it:
                    it.setText(status)
                break

    # ---- actions ----
    def on_scan(self):
        self._sync_baud()
        if self.scan_worker and self.scan_worker.isRunning():
            return
        self.log('开始扫描...')
        self.btn_scan.setEnabled(False)
        self.scan_worker = PortScanWorker(self.baudrate)
        self.scan_worker.log_signal.connect(self.log)
        self.scan_worker.finished_signal.connect(self.on_scan_finished)
        self.scan_worker.start()

    def on_scan_finished(self, found: list):
        self.btn_scan.setEnabled(True)
        self.devices.clear()
        for port, sn in found:
            dev = SensorDevice(port=port, sensor=UniversalSensor(port, self.baudrate, timeout=0.6), current_sn=sn)
            self.devices.append(dev)
        self._rebuild_table()
        self.log(f'扫描完成：识别 {len(self.devices)} 个传感器。')

    def on_apply_sn_rule(self):
        self._collect_table_to_devices()
        try:
            prefix = self.ed_prefix.text()
            start = int(self.ed_start.text().strip())
            width = int(self.ed_width.text().strip())
            step = int(self.ed_step.text().strip())
        except Exception:
            QMessageBox.warning(self, '错误', 'SN规则参数格式不正确')
            return
        n = start
        for i, d in enumerate(self.devices):
            if not d.selected:
                continue
            d.new_sn_text = f"{prefix}{n:0{width}d}"
            it = self.tbl.item(i, 4)
            if it:
                it.setText(d.new_sn_text)
            n += step
        self.log('已按规则填充新SN（你也可以直接在表格里改单个SN）。')

    def on_write_sn(self):
        self._sync_baud()
        self._collect_table_to_devices()
        if self.sn_worker and self.sn_worker.isRunning():
            return
        targets = [d for d in self.devices if d.selected]
        if not targets:
            QMessageBox.information(self, '提示', '没有选中的传感器')
            return
        self.btn_write_sn.setEnabled(False)
        self.sn_worker = WriteSNWorker(self.devices, baudrate=self.baudrate)
        self.sn_worker.log_signal.connect(self.log)
        self.sn_worker.progress_signal.connect(self._set_row_status)
        self.sn_worker.finished_signal.connect(self.on_write_sn_finished)
        self.sn_worker.start()

    def on_write_sn_finished(self):
        self.btn_write_sn.setEnabled(True)
        # 刷新表里的当前SN显示
        self._rebuild_table()
        self.log('SN写入流程完成。')

    # ---- factory reset actions ----
    def on_reset_rt(self):
        self._start_factory_reset('rt')

    def on_reset_ad(self):
        self._start_factory_reset('ad')

    def _start_factory_reset(self, kind: str):
        """对选中传感器执行恢复出厂（Rt 或 AD）。"""
        self._sync_baud()
        self._collect_table_to_devices()

        if self.cal_worker and self.cal_worker.isRunning():
            QMessageBox.information(self, '提示', '当前有标定任务正在运行，请先停止。')
            return
        if self.reset_worker and self.reset_worker.isRunning():
            return

        targets = [d for d in self.devices if d.selected]
        if not targets:
            QMessageBox.information(self, '提示', '没有选中的传感器')
            return

        title = '恢复出厂确认'
        tip = '温度-Rt 标定后恢复出厂(01 3F 00)' if (kind or '').lower().strip() != 'ad' else '温度-AD 标定后恢复出厂(01 42 00)'
        ret = QMessageBox.question(self, title, f'将对选中 {len(targets)} 个传感器执行：\n\n{tip}\n\n是否继续？')
        if ret != QMessageBox.StandardButton.Yes:
            return

        try:
            self.btn_reset_rt.setEnabled(False)
            self.btn_reset_ad.setEnabled(False)
        except Exception:
            pass

        self.reset_worker = FactoryResetWorker(self.devices, baudrate=self.baudrate, kind=kind)
        self.reset_worker.log_signal.connect(self.log)
        self.reset_worker.progress_signal.connect(self._set_row_status)
        self.reset_worker.finished_signal.connect(self.on_factory_reset_finished)
        self.reset_worker.start()

    def on_factory_reset_finished(self):
        try:
            self.btn_reset_rt.setEnabled(True)
            self.btn_reset_ad.setEnabled(True)
        except Exception:
            pass
        self.log('恢复出厂流程完成。')

    def on_start_cal(self):
        self._sync_baud()
        self._collect_table_to_devices()
        if self.cal_worker and self.cal_worker.isRunning():
            return
        targets = [d for d in self.devices if d.selected]
        if not targets:
            QMessageBox.information(self, '提示', '没有选中的传感器')
            return
        if any((d.current_sn is None) for d in targets):
            self.log('⚠️ 有传感器SN为空（仍可继续，但入库与报告会用占位名）。')
        try:
            stage_temps = [float(self.ed_t1.text()), float(self.ed_t2.text()), float(self.ed_t3.text())]
            temp_tol = float(self.ed_tol.text())
            ch_stable = int(float(self.ed_ch_stable.text()))
            sen_stable = int(float(self.ed_sen_stable.text()))
            a_range = float(self.ed_a_range.text())
            min_samples = int(float(self.ed_min_samples.text()))
            use_ch = self.cb_ch.isChecked()
            ch_ip = self.ed_ch_ip.text().strip()
            ch_port = int(float(self.ed_ch_port.text()))
            i_en = self.cb_iotdb.isChecked()
            i_host = self.ed_i_host.text().strip()
            i_port = int(float(self.ed_i_port.text()))
            i_user = self.ed_i_user.text().strip()
            i_pass = self.ed_i_pass.text().strip()
            i_sg = self.ed_i_sg.text().strip()
            i_down = int(float(self.ed_i_down.text()))
            do60 = self.cb_60.isChecked()
            t60 = float(self.ed_60.text())
            v60_wait = int(float(self.ed_60_wait.text()))
            # 记录本次运行参数（用于自动生成报告时写入meta）
            self._last_run_meta = {
                'mode': 'calibration',
                'stage_temps': stage_temps,
                'temp_tol': temp_tol,
                'chamber_stable_s': ch_stable,
                'sensor_stable_s': sen_stable,
                'a_range_thresh': a_range,
                'min_samples': min_samples,
                'use_chamber': bool(use_ch),
                'chamber_ip': ch_ip,
                'chamber_port': int(ch_port),
                'iotdb_enable': bool(i_en),
                'iotdb_host': i_host,
                'iotdb_port': int(i_port),
                'iotdb_sg': i_sg,
                'iotdb_downsample_ms': int(i_down),
                'do_60c_verify': bool(do60),
                'verify_temp': float(t60),
                'verify_wait_s': int(v60_wait),
                'started_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            }
        except Exception:
            QMessageBox.warning(self, '错误', '标定参数格式不正确')
            return

        self.btn_start_cal.setEnabled(False)
        self.btn_verify60.setEnabled(False)
        self.btn_stop_cal.setEnabled(True)
        self.btn_rescan.setEnabled(True)
        self.btn_export.setEnabled(False)
        try:
            self.btn_reset_rt.setEnabled(False)
            self.btn_reset_ad.setEnabled(False)
        except Exception:
            pass
        self.lbl_stage.setText('状态：启动中...')
        self._last_report = None

        # 打开实时监控弹窗（采集期间显示A~N和IoTDB入库情况）
        try:
            if self.monitor_dialog is None:
                self.monitor_dialog = RealtimeMonitorDialog(self, self.devices, i_sg)
            else:
                self.monitor_dialog.sg = i_sg
                self.monitor_dialog.devices = [d for d in self.devices if d.selected]
                self.monitor_dialog._init_rows()
            self.monitor_dialog.set_stage('启动中')
            self.monitor_dialog.show()
            self.monitor_dialog.raise_()
            self.btn_monitor.setEnabled(True)
        except Exception:
            pass

        self.cal_worker = CalibrationWorker(
            devices=self.devices,
            baudrate=self.baudrate,
            use_chamber=use_ch,
            chamber_ip=ch_ip,
            chamber_port=ch_port,
            stage_temps=stage_temps,
            temp_tol=temp_tol,
            chamber_stable_s=ch_stable,
            sensor_stable_s=sen_stable,
            a_range_thresh=a_range,
            min_samples=min_samples,
            iotdb_enable=i_en,
            iotdb_host=i_host,
            iotdb_port=i_port,
            iotdb_user=i_user,
            iotdb_pass=i_pass,
            iotdb_sg=i_sg,
            iotdb_downsample_ms=i_down,
            do_60c_verify=do60,
            verify_temp=t60,
            verify_wait_s=v60_wait,
        )
        self.cal_worker.log_signal.connect(self.log)
        self.cal_worker.stage_signal.connect(lambda s: self.lbl_stage.setText('状态：' + s))
        self.cal_worker.sensor_status_signal.connect(self._set_row_status)
        try:
            if self.monitor_dialog:
                self.cal_worker.sample_signal.connect(self.monitor_dialog.on_sample)
                self.cal_worker.iotdb_status_signal.connect(self.monitor_dialog.on_iotdb_status)
                self.cal_worker.stage_signal.connect(self.monitor_dialog.set_stage)
                self.cal_worker.log_signal.connect(self.monitor_dialog.append_log)
        except Exception:
            pass
        self.cal_worker.finished_signal.connect(self.on_cal_finished)
        self.cal_worker.start()

    def on_verify60(self):
        """不经过温度标定，直接做 60℃（或自定义复核温度）复核，用于成品二次复核。
        流程：调试模式采集(A~N)+可选入库 -> 恒温箱到达并稳定 -> 箱温稳定后等待复核时间，用 F/100 与箱温在结束时比对一次（±容差）。
        """
        self._sync_baud()
        self._collect_table_to_devices()
        if (self.cal_worker and self.cal_worker.isRunning()) or (self.verify_worker and self.verify_worker.isRunning()):
            return
        targets = [d for d in self.devices if d.selected]
        if not targets:
            QMessageBox.information(self, '提示', '没有选中的传感器')
            return

        try:
            temp_tol = float(self.ed_tol.text())
            ch_stable = int(float(self.ed_ch_stable.text()))
            sen_stable = int(float(self.ed_sen_stable.text()))
            v60_wait = int(float(self.ed_60_wait.text()))
            use_ch = self.cb_ch.isChecked()
            ch_ip = self.ed_ch_ip.text().strip()
            ch_port = int(float(self.ed_ch_port.text()))
            i_en = self.cb_iotdb.isChecked()
            i_host = self.ed_i_host.text().strip()
            i_port = int(float(self.ed_i_port.text()))
            i_user = self.ed_i_user.text().strip()
            i_pass = self.ed_i_pass.text().strip()
            i_sg = self.ed_i_sg.text().strip()
            i_down = int(float(self.ed_i_down.text()))
            t60 = float(self.ed_60.text())
            # 记录本次运行参数（用于自动生成报告时写入meta）
            self._last_run_meta = {
                'mode': 'verify_only',
                'temp_tol': temp_tol,
                'chamber_stable_s': ch_stable,
                'sensor_stable_s': sen_stable,
                'use_chamber': bool(use_ch),
                'chamber_ip': ch_ip,
                'chamber_port': int(ch_port),
                'iotdb_enable': bool(i_en),
                'iotdb_host': i_host,
                'iotdb_port': int(i_port),
                'iotdb_sg': i_sg,
                'iotdb_downsample_ms': int(i_down),
                'verify_temp': float(t60),
                'verify_wait_s': int(v60_wait),
                'started_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            }
        except Exception:
            QMessageBox.warning(self, '错误', '参数格式不正确')
            return

        self.btn_start_cal.setEnabled(False)
        self.btn_verify60.setEnabled(False)
        self.btn_stop_cal.setEnabled(True)
        self.btn_rescan.setEnabled(True)
        self.btn_export.setEnabled(False)
        try:
            self.btn_reset_rt.setEnabled(False)
            self.btn_reset_ad.setEnabled(False)
        except Exception:
            pass
        self.lbl_stage.setText('状态：60℃复核启动中...')
        self._last_report = None

        # 打开实时监控弹窗
        try:
            if self.monitor_dialog is None:
                self.monitor_dialog = RealtimeMonitorDialog(self, self.devices, i_sg)
            else:
                self.monitor_dialog.sg = i_sg
                self.monitor_dialog.devices = [d for d in self.devices if d.selected]
                self.monitor_dialog._init_rows()
            self.monitor_dialog.set_stage('60℃复核启动中')
            self.monitor_dialog.show()
            self.monitor_dialog.raise_()
            self.btn_monitor.setEnabled(True)
        except Exception:
            pass

        self.verify_worker = Verify60Worker(
            devices=self.devices,
            baudrate=self.baudrate,
            use_chamber=use_ch,
            chamber_ip=ch_ip,
            chamber_port=ch_port,
            temp_tol=temp_tol,
            chamber_stable_s=ch_stable,
            verify_window_s=v60_wait,
            verify_temp=t60,
            iotdb_enable=i_en,
            iotdb_host=i_host,
            iotdb_port=i_port,
            iotdb_user=i_user,
            iotdb_pass=i_pass,
            iotdb_sg=i_sg,
            iotdb_downsample_ms=i_down,
        )
        self.verify_worker.log_signal.connect(self.log)
        self.verify_worker.stage_signal.connect(lambda s: self.lbl_stage.setText('状态：' + s))
        self.verify_worker.sensor_status_signal.connect(self._set_row_status)
        try:
            if self.monitor_dialog:
                self.verify_worker.sample_signal.connect(self.monitor_dialog.on_sample)
                self.verify_worker.iotdb_status_signal.connect(self.monitor_dialog.on_iotdb_status)
                self.verify_worker.stage_signal.connect(self.monitor_dialog.set_stage)
                self.verify_worker.log_signal.connect(self.monitor_dialog.append_log)
        except Exception:
            pass
        self.verify_worker.finished_signal.connect(self.on_verify_finished)
        self.verify_worker.start()

    def on_verify_finished(self, report: dict):
        self.btn_start_cal.setEnabled(True)
        self.btn_verify60.setEnabled(True)
        self.btn_stop_cal.setEnabled(False)
        self.btn_rescan.setEnabled(False)
        self.btn_export.setEnabled(True)
        self.btn_monitor.setEnabled(True)
        try:
            self.btn_reset_rt.setEnabled(True)
            self.btn_reset_ad.setEnabled(True)
        except Exception:
            pass
        self._last_report = report
        self.lbl_stage.setText('状态：完成')
        try:
            if self.monitor_dialog:
                self.monitor_dialog.set_stage('完成')
        except Exception:
            pass
        self.log('60℃复核完成。')
        # 自动生成报告文件（包含Rt/AD写入点与合格判定）
        try:
            self._auto_save_report(report)
        except Exception as e:
            self.log(f'⚠️ 报告保存失败: {e}')

    def on_rescan(self):
        """接触不良时，允许在不中断流程的情况下重新扫描/重连，然后继续后续步骤。"""
        if self.cal_worker and self.cal_worker.isRunning():
            try:
                self.cal_worker.request_rescan()
                self.log('已请求重新扫描/重连（标定线程会在合适的循环点执行）。')
            except Exception as e:
                self.log(f'⚠️ 请求重新扫描失败: {e}')
        elif self.verify_worker and self.verify_worker.isRunning():
            try:
                self.verify_worker.request_rescan()
                self.log('已请求重新扫描/重连（复核线程会在合适的循环点执行）。')
            except Exception as e:
                self.log(f'⚠️ 请求重新扫描失败: {e}')
        else:
            QMessageBox.information(self, '提示', '当前没有正在运行的标定/复核任务。')

    def on_open_monitor(self):
        self._collect_table_to_devices()
        sg = self.ed_i_sg.text().strip() if hasattr(self, 'ed_i_sg') else 'root.h2'
        if self.monitor_dialog is None:
            self.monitor_dialog = RealtimeMonitorDialog(self, self.devices, sg)
        else:
            self.monitor_dialog.sg = sg
            self.monitor_dialog.devices = [d for d in self.devices if d.selected]
            self.monitor_dialog._init_rows()
        self.monitor_dialog.show()
        self.monitor_dialog.raise_()

    def on_manual_continue(self):
        """在采样中断/接触不良等情况下，允许用户手动补齐三点，然后继续执行：写入(0x41)+回读(0x40)+可选60℃复核。"""
        self._sync_baud()
        self._collect_table_to_devices()
        if self.cal_worker and self.cal_worker.isRunning():
            QMessageBox.information(self, '提示', '当前有标定任务正在运行，请先停止。')
            return

        targets = [d for d in self.devices if d.selected]
        if not targets:
            QMessageBox.information(self, '提示', '没有选中的传感器')
            return
        if any((d.current_sn is None) for d in targets):
            QMessageBox.warning(self, '提示', '有传感器SN为空。建议先“扫描端口”确保SN已读出（你也可以先写入自定义SN）。')
            # 仍允许继续（用户可能手动输入时不依赖SN？但写入/入库会受影响）

        try:
            stage_temps = [float(self.ed_t1.text()), float(self.ed_t2.text()), float(self.ed_t3.text())]
            temp_tol = float(self.ed_tol.text())
            ch_stable = int(float(self.ed_ch_stable.text()))
            sen_stable = int(float(self.ed_sen_stable.text()))
            a_range = float(self.ed_a_range.text())
            min_samples = int(float(self.ed_min_samples.text()))
            use_ch = self.cb_ch.isChecked()
            ch_ip = self.ed_ch_ip.text().strip()
            ch_port = int(float(self.ed_ch_port.text()))
            i_en = self.cb_iotdb.isChecked()
            i_host = self.ed_i_host.text().strip()
            i_port = int(float(self.ed_i_port.text()))
            i_user = self.ed_i_user.text().strip()
            i_pass = self.ed_i_pass.text().strip()
            i_sg = self.ed_i_sg.text().strip()
            i_down = int(float(self.ed_i_down.text()))
            do60 = self.cb_60.isChecked()
            t60 = float(self.ed_60.text())
            v60_wait = int(float(self.ed_60_wait.text()))
        except Exception:
            QMessageBox.warning(self, '错误', '标定参数格式不正确')
            return

        # 打开手动补点对话框
        dlg = ManualPointsDialog(self, self.devices, stage_temps, last_report=self._last_report,
                                 cache_points=self.points_cache)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        manual_points = dlg.get_points()
        if not manual_points:
            QMessageBox.information(self, '提示', '没有获得手动点数据')
            return

        self.btn_start_cal.setEnabled(False)
        self.btn_verify60.setEnabled(False)
        self.btn_stop_cal.setEnabled(True)
        self.btn_rescan.setEnabled(True)
        self.btn_export.setEnabled(False)
        try:
            self.btn_reset_rt.setEnabled(False)
            self.btn_reset_ad.setEnabled(False)
        except Exception:
            pass
        self.lbl_stage.setText('状态：手动点写入中...')
        self._last_report = None

        # 打开实时监控弹窗（60℃复核时会显示F值与入库）
        try:
            if self.monitor_dialog is None:
                self.monitor_dialog = RealtimeMonitorDialog(self, self.devices, i_sg)
            else:
                self.monitor_dialog.sg = i_sg
                self.monitor_dialog.devices = [d for d in self.devices if d.selected]
                self.monitor_dialog._init_rows()
            self.monitor_dialog.set_stage('手动点写入中')
            self.monitor_dialog.show()
            self.monitor_dialog.raise_()
            self.btn_monitor.setEnabled(True)
        except Exception:
            pass

        self.cal_worker = CalibrationWorker(
            devices=self.devices,
            baudrate=self.baudrate,
            use_chamber=use_ch,
            chamber_ip=ch_ip,
            chamber_port=ch_port,
            stage_temps=stage_temps,
            temp_tol=temp_tol,
            chamber_stable_s=ch_stable,
            sensor_stable_s=sen_stable,
            a_range_thresh=a_range,
            min_samples=min_samples,
            iotdb_enable=i_en,
            iotdb_host=i_host,
            iotdb_port=i_port,
            iotdb_user=i_user,
            iotdb_pass=i_pass,
            iotdb_sg=i_sg,
            iotdb_downsample_ms=i_down,
            do_60c_verify=do60,
            verify_temp=t60,
            verify_wait_s=v60_wait,
            manual_points_by_sn=manual_points,
        )
        self.cal_worker.log_signal.connect(self.log)
        self.cal_worker.stage_signal.connect(lambda s: self.lbl_stage.setText('状态：' + s))
        self.cal_worker.sensor_status_signal.connect(self._set_row_status)
        try:
            if self.monitor_dialog:
                self.cal_worker.sample_signal.connect(self.monitor_dialog.on_sample)
                self.cal_worker.iotdb_status_signal.connect(self.monitor_dialog.on_iotdb_status)
                self.cal_worker.stage_signal.connect(self.monitor_dialog.set_stage)
                self.cal_worker.log_signal.connect(self.monitor_dialog.append_log)
        except Exception:
            pass
        self.cal_worker.finished_signal.connect(self.on_cal_finished)
        self.cal_worker.start()

    def on_stop_cal(self):
        # 停止标定或单独60℃复核
        if getattr(self, 'cal_worker', None) and self.cal_worker.isRunning():
            self.cal_worker.stop()
            self.log('已请求停止标定。')
            try:
                if self.monitor_dialog:
                    self.monitor_dialog.set_stage('停止中')
            except Exception:
                pass

        if getattr(self, 'verify_worker', None) and self.verify_worker.isRunning():
            self.verify_worker.stop()
            self.log('已请求停止60℃复核。')
            try:
                if self.monitor_dialog:
                    self.monitor_dialog.set_stage('停止中')
            except Exception:
                pass

        try:
            self.btn_stop_cal.setEnabled(False)
        except Exception:
            pass
        try:
            self.btn_rescan.setEnabled(False)
        except Exception:
            pass

    def on_cal_finished(self, report: dict):
        try:
            self.btn_start_cal.setEnabled(True)
        except Exception:
            pass
        try:
            self.btn_verify60.setEnabled(True)
        except Exception:
            pass
        try:
            self.btn_stop_cal.setEnabled(False)
        except Exception:
            pass
        try:
            self.btn_rescan.setEnabled(False)
        except Exception:
            pass
        try:
            self.btn_export.setEnabled(True)
            self.btn_monitor.setEnabled(True)
        except Exception:
            pass
        try:
            self.btn_reset_rt.setEnabled(True)
            self.btn_reset_ad.setEnabled(True)
        except Exception:
            pass

        self._last_report = report

        # 缓存点位，便于后续“手动补点/继续”预填
        try:
            for sn, info in (report or {}).items():
                if not isinstance(info, dict):
                    continue
                pts = info.get('points') or {}
                if pts:
                    self.points_cache[sn] = {
                        k: v for k, v in pts.items()
                        if k in ('P1', 'P2', 'P3') and isinstance(v, dict)
                    }
        except Exception:
            pass

        try:
            self.lbl_stage.setText('状态：完成')
        except Exception:
            pass
        self.log('标定完成（或已停止）。')
        # 自动生成报告文件（包含Rt/AD写入点与合格判定）
        try:
            self._auto_save_report(report)
        except Exception as e:
            self.log(f'⚠️ 报告保存失败: {e}')
        try:
            if self.monitor_dialog:
                self.monitor_dialog.set_stage('完成')
        except Exception:
            pass

    def _build_report_payload(self, report: dict) -> dict:
        """把本次标定/复核的结果封装成可长期保存的payload。"""
        meta = {}
        try:
            meta = getattr(self, '_last_run_meta', None) or {}
        except Exception:
            meta = {}
        return {
            'generated_at': time.strftime('%Y-%m-%d %H:%M:%S'),
            'run_meta': meta,
            'sensors': report or {},
        }

    def _auto_save_report(self, report: dict):
        """每次标定结束后自动生成一个报告文件，永久保留写入点与合格判定。"""
        try:
            import os
            import json

            payload = self._build_report_payload(report)
            mode = (payload.get('run_meta') or {}).get('mode') or 'calibration'
            ts = time.strftime('%Y%m%d_%H%M%S')

            # 尽量保存到“可写”的目录：
            # 1) 脚本同目录/calibration_reports（便于就近管理）
            # 2) 当前工作目录/calibration_reports（适配打包exe等场景）
            # 3) 用户主目录/calibration_reports（兜底）
            base_dir = os.path.dirname(os.path.abspath(__file__))
            candidates = [
                os.path.join(base_dir, 'calibration_reports'),
                os.path.join(os.getcwd(), 'calibration_reports'),
                os.path.join(os.path.expanduser('~'), 'calibration_reports'),
            ]

            out_dir = None
            out_path = None
            last_err = None
            for d in candidates:
                try:
                    os.makedirs(d, exist_ok=True)
                    out_dir = d
                    out_path = os.path.join(out_dir, f'{mode}_report_{ts}.json')
                    with open(out_path, 'w', encoding='utf-8') as f:
                        json.dump(payload, f, ensure_ascii=False, indent=2)
                    break
                except Exception as e:
                    last_err = e
                    out_dir = None
                    out_path = None

            if not out_path:
                raise RuntimeError(f'无法写入报告文件：{last_err}')

            self.log('✅ 已自动生成报告：' + out_path)
        except Exception as e:
            self.log(f'⚠️ 自动生成报告失败：{e}')

    def on_export(self):
        if not getattr(self, '_last_report', None):
            QMessageBox.information(self, '提示', '没有可导出的报告')
            return
        path, _ = QFileDialog.getSaveFileName(self, '保存报告', 'calibration_report.csv', 'CSV Files (*.csv)')
        if not path:
            return
        try:
            import csv

            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.writer(f)
                w.writerow([
                    'SN', 'Port', 'Final',
                    'P1_target', 'P1_chamber', 'P1_adc', 'P1_e', 'P1_temp',
                    'P2_target', 'P2_chamber', 'P2_adc', 'P2_e', 'P2_temp',
                    'P3_target', 'P3_chamber', 'P3_adc', 'P3_e', 'P3_temp',
                    'RT_num', 'RT_points', 'AD_num', 'AD_points', 'Verify60'
                ])
                for sn, info in (self._last_report or {}).items():
                    if not isinstance(info, dict):
                        continue
                    p1 = (info.get('points', {}) or {}).get('P1', {})
                    p2 = (info.get('points', {}) or {}).get('P2', {})
                    p3 = (info.get('points', {}) or {}).get('P3', {})
                    rt_rb = info.get('rt_readback')
                    rt_num = rt_rb.get('num') if isinstance(rt_rb, dict) else ''
                    rt_pts = rt_rb.get('points') if isinstance(rt_rb, dict) else ''
                    ad_rb = info.get('ad_readback')
                    ad_num = ad_rb.get('num') if isinstance(ad_rb, dict) else ''
                    ad_pts = ad_rb.get('points') if isinstance(ad_rb, dict) else ''
                    v60 = info.get('verify_60c')

                    w.writerow([
                        sn,
                        info.get('port'),
                        info.get('final'),
                        p1.get('target'), p1.get('chamber'), p1.get('adc'), p1.get('e'), p1.get('temp'),
                        p2.get('target'), p2.get('chamber'), p2.get('adc'), p2.get('e'), p2.get('temp'),
                        p3.get('target'), p3.get('chamber'), p3.get('adc'), p3.get('e'), p3.get('temp'),
                        rt_num, rt_pts, ad_num, ad_pts, v60,
                    ])
            self.log('✅ 报告已导出：' + path)
        except Exception as e:
            QMessageBox.warning(self, '错误', f'导出失败：{e}')


def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()