"""
浓度标定软件（UI版，PyQt6） - 终极产线抗插拔版

特性与改进：
- 流程与《浓度标定文档版》完全一致。
- 支持倒计时平均值采集。
- 引入“按SN自动重映射端口”机制：遇到接触不良或断联，即使重插拔后COM口改变，
  点击“接触不良，重新扫描”时也能自动通过SN找回设备，无缝续跑，彻底解决USB枚举漂移痛点。
- 支持复核流程独立运行。
"""

from __future__ import annotations

import sys
import time
import re
import math
import queue
import threading
from dataclasses import dataclass, field
from collections import deque
from typing import Dict, List, Optional, Tuple, Any

import serial
import crcmod.predefined
from serial.tools import list_ports

from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QIntValidator, QDoubleValidator
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
    QInputDialog,
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


# -------------------- 通用工具 --------------------

def now_ms() -> int:
    return int(time.time() * 1000)


def sanitize_sn(sn_bytes: bytes) -> str:
    s = ''.join(chr(b) if 32 <= b <= 126 else '' for b in sn_bytes).replace('\x00', '').strip()
    s = re.sub(r'[^A-Za-z0-9_]', '_', s)
    if not s or not re.match(r'^[A-Za-z_]', s):
        s = 'd_' + s
    return s or 'd_unknown'


def fmt_sn_dual(sn_bytes: Optional[bytes]) -> str:
    if not sn_bytes:
        return "None"
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


def u16_from_int16(v: int) -> int: return v & 0xFFFF


def int16_from_u16(v: int) -> int: return v - 65536 if v >= 32768 else v


def u32_from_int32(v: int) -> int: return v & 0xFFFFFFFF


def int32_from_u32(v: int) -> int: return v - 4294967296 if v >= 2147483648 else v


def u24_from_u32(v: int) -> int: return int(v) & 0xFFFFFF


def bytes_u24_be(v: int) -> Tuple[int, int, int]:
    v = u24_from_u32(v)
    return (v >> 16) & 0xFF, (v >> 8) & 0xFF, v & 0xFF


def bytes_u16_be(v: int) -> Tuple[int, int]:
    v = int(v) & 0xFFFF
    return (v >> 8) & 0xFF, v & 0xFF


def bytes_u32_be(v: int) -> Tuple[int, int, int, int]:
    v = int(v) & 0xFFFFFFFF
    return (v >> 24) & 0xFF, (v >> 16) & 0xFF, (v >> 8) & 0xFF, v & 0xFF


# -------------------- 传感器串口封装 --------------------

class UniversalSensor:
    def __init__(self, port: str, baudrate: int = 115200, timeout: float = 0.6):
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

    def send_command(
            self,
            body: List[int],
            wait_s: float = 1.0,
            min_len: int = 5,
            drain_after_min: bool = True,
            drain_quiet_s: float = 0.04,
    ) -> Optional[bytes]:
        if not self.ser or not self.ser.is_open:
            return None
        payload = bytes(body)
        full = payload + self.modbus_crc(payload)
        with self.lock:
            try:
                self.ser.reset_input_buffer()
                self.ser.write(full)

                deadline = time.time() + float(wait_s)
                buf = bytearray()

                while time.time() < deadline:
                    n = self.ser.in_waiting
                    if n:
                        buf += self.ser.read(n)
                    else:
                        time.sleep(0.02)
                    if len(buf) >= int(min_len):
                        break

                if drain_after_min and buf:
                    quiet_deadline = time.time() + float(drain_quiet_s)
                    while time.time() < quiet_deadline:
                        n = self.ser.in_waiting
                        if n:
                            buf += self.ser.read(n)
                            quiet_deadline = time.time() + float(drain_quiet_s)
                        else:
                            time.sleep(0.01)

                return bytes(buf) if buf else None
            except Exception:
                return None

    def enter_debug(self) -> bool:
        resp = self.send_command([0x01, 0x20, 0x00], wait_s=1.0, min_len=5)
        return bool(resp and len(resp) >= 3 and resp[0] == 0x01 and resp[1] == 0x20 and resp[2] == 0x00)

    def enter_user(self) -> bool:
        resp = self.send_command([0x01, 0x1E, 0x00], wait_s=1.0, min_len=5)
        return bool(resp and len(resp) >= 3 and resp[0] == 0x01 and resp[1] == 0x1E and resp[2] == 0x00)

    def read_sn(self) -> Optional[bytes]:
        resp = self.send_command([0x01, 0x03, 0x00, 0x07, 0x00, 0x06], wait_s=1.0, min_len=3 + 12)
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

    def conc_factory_reset(self) -> bool:
        r = self.send_command([0x01, 0x34, 0x00], wait_s=1.0, min_len=5)
        return bool(r and len(r) >= 3 and r[0] == 0x01 and r[1] == 0x34 and r[2] == 0x00)

    def write_reg_u16(self, addr: int, val_u16: int) -> bool:
        addr = int(addr) & 0xFFFF
        val_u16 = int(val_u16) & 0xFFFF
        aH, aL = bytes_u16_be(addr)
        vH, vL = bytes_u16_be(val_u16)
        body = [0x01, 0x10, aH, aL, 0x00, 0x01, 0x02, vH, vL]
        r = self.send_command(body, wait_s=1.0, min_len=8)
        return bool(
            r and len(r) >= 6 and r[0] == 0x01 and r[1] == 0x10 and r[2] == aH and r[3] == aL and r[4] == 0x00 and r[
                5] == 0x01)

    def conc_set_num_points(self, n: int) -> bool:
        n = int(n) & 0xFF
        r = self.send_command([0x01, 0x2A, 0x00, n], wait_s=1.0, min_len=6)
        return bool(r and len(r) >= 4 and r[0] == 0x01 and r[1] == 0x2A and r[2] == 0x00 and r[3] == n)

    def conc_write_point(self, idx: int, ppm_u24: int, s_i32: int) -> bool:
        idx = int(idx) & 0xFF
        pH, pM, pL = bytes_u24_be(ppm_u24)
        s_u32 = u32_from_int32(int(s_i32))
        sH, sMH, sML, sL = bytes_u32_be(s_u32)
        body = [0x01, 0x2A, idx, pH, pM, pL, sH, sMH, sML, sL]
        r = self.send_command(body, wait_s=1.0, min_len=5)
        return bool(r and len(r) >= 3 and r[0] == 0x01 and r[1] == 0x2A and r[2] == idx)

    def conc_read_num(self) -> Optional[int]:
        r = self.send_command([0x01, 0x2B, 0x00], wait_s=1.0, min_len=7)
        if not r or len(r) < 5:
            return None
        if r[0] != 0x01 or r[1] != 0x2B or r[2] != 0x00:
            return None
        return int(r[3])

    def conc_read_point(self, idx: int) -> Optional[Tuple[int, int]]:
        idx = int(idx) & 0xFF
        r = self.send_command([0x01, 0x2B, idx], wait_s=1.0, min_len=14)
        if not r or len(r) < 10:
            return None
        if r[0] != 0x01 or r[1] != 0x2B or r[2] != idx:
            return None
        ppm = (r[3] << 16) | (r[4] << 8) | r[5]
        s_u32 = (r[6] << 24) | (r[7] << 16) | (r[8] << 8) | r[9]
        s_i32 = int32_from_u32(s_u32)
        return int(ppm), int(s_i32)


@dataclass
class SensorDevice:
    port: str
    sensor: UniversalSensor
    current_sn: Optional[bytes] = None
    expected_sn: Optional[bytes] = None
    selected: bool = True
    new_sn_text: str = ''
    last_vals: Dict[str, float] = field(default_factory=dict)
    history: deque = field(default_factory=lambda: deque(maxlen=8000))
    status: str = 'Idle'


# -------------------- IoTDB 写入线程 --------------------

class IoTDBWriterThread(QThread):
    log_signal = pyqtSignal(str)
    status_signal = pyqtSignal(str, int, bool, str)

    def __init__(self, host: str, port: int, user: str, password: str, storage_group: str, enable: bool = True):
        super().__init__()
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.sg = storage_group
        self.enable = bool(enable)
        self._q: "queue.Queue[Tuple[str,int,Dict[str,float]]]" = queue.Queue(maxsize=20000)
        self._stop = threading.Event()
        self.session = None
        self._known_devices = set()

    def submit(self, device_id: str, ts_ms: int, values: Dict[str, float]):
        if not self.enable:
            return
        try:
            self._q.put_nowait((device_id, ts_ms, values))
        except queue.Full:
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
                            self.log_signal.emit(f"⚠️ IoTDB 写入失败(将继续): {e}")
                            time.sleep(0.2)
                            break
                batch.clear()
                last_flush = time.time()

        try:
            if self.session:
                self.session.close()
        except Exception:
            pass


# -------------------- 传感器实时采集线程 --------------------

class SensorStreamThread(QThread):
    log_signal = pyqtSignal(str)
    sample_signal = pyqtSignal(str, dict)

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

                    self.device.last_vals = vals
                    try:
                        self.device.history.append((ts, vals))
                    except Exception:
                        pass

                    self.sample_signal.emit(self.device.port, vals)

                    if self.iotdb_writer and (ts - self._last_write_ms) >= self.downsample_ms:
                        sn = self.device.current_sn or self.device.expected_sn or b''
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
    finished_signal = pyqtSignal(list)

    def __init__(self, baudrate: int = 115200):
        super().__init__()
        self.baudrate = int(baudrate)

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
                ok = s.enter_user()
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
    progress_signal = pyqtSignal(str, str)
    finished_signal = pyqtSignal()

    def __init__(self, devices: List[SensorDevice], baudrate: int = 115200):
        super().__init__()
        self.devices = [d for d in devices if d.selected]
        self.baudrate = int(baudrate)
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
                _ = s.enter_user()
                old = s.read_sn()
                newb = self._sn_text_to_12b(d.new_sn_text.strip())
                ok = s.write_sn(newb)
                if ok:
                    d.current_sn = newb
                    d.expected_sn = newb
                    self.progress_signal.emit(d.port, '✅ 写入成功')
                    self.log_signal.emit(f'[{d.port}] 写入SN OK：{fmt_sn_dual(newb)} (旧SN={fmt_sn_dual(old)})')
                else:
                    verify = s.read_sn()
                    self.progress_signal.emit(d.port, '❌ 校验失败')
                    self.log_signal.emit(f'[{d.port}] 写入SN失败：期望={fmt_sn_dual(newb)} 实际={fmt_sn_dual(verify)}')
            except Exception as e:
                self.progress_signal.emit(d.port, f'❌ 异常:{e}')
            finally:
                s.close()
        self.finished_signal.emit()


# -------------------- 浓度恢复出厂线程 --------------------

class ConcFactoryResetWorker(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(str, str)
    finished_signal = pyqtSignal()

    def __init__(self, devices: List[SensorDevice], baudrate: int = 115200):
        super().__init__()
        self.devices = [d for d in devices if d.selected]
        self.baudrate = int(baudrate)
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run(self):
        if not self.devices:
            self.log_signal.emit('没有选中的传感器。')
            self.finished_signal.emit()
            return

        self.log_signal.emit('开始执行：浓度恢复出厂(01 34 00)...')
        for d in self.devices:
            if self._stop.is_set():
                break
            self.progress_signal.emit(d.port, '恢复出厂中...')
            s = UniversalSensor(d.port, self.baudrate, timeout=0.8)
            if not s.open():
                self.progress_signal.emit(d.port, '❌ 串口打开失败')
                continue
            try:
                _ = s.enter_user()
                ok = s.conc_factory_reset()
                if ok:
                    self.progress_signal.emit(d.port, '✅ 已恢复出厂')
                else:
                    self.progress_signal.emit(d.port, '❌ 回包异常')
                    self.log_signal.emit(f'[{d.port}] 浓度恢复出厂 回包异常')
            except Exception as e:
                self.progress_signal.emit(d.port, f'❌ 异常:{e}')
            finally:
                s.close()

        self.log_signal.emit('浓度恢复出厂流程完成。')
        self.finished_signal.emit()


# -------------------- 实时监控弹窗 --------------------

class RealtimeMonitorDialog(QDialog):
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
        self.tbl.setWordWrap(True)
        try:
            self.tbl.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        except Exception:
            pass
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
            sn_str = sanitize_sn(d.current_sn or d.expected_sn or b'')
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
                        if k in ('A', 'I', 'J', 'M', 'N'):
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

    # 重要修复补充：监控窗口自身也必须处理端口映射，否则数据刷不上去
    def on_port_remap(self, old_port: str, new_port: str):
        r = self._row_by_port.pop(old_port, None)
        if r is not None:
            self._row_by_port[new_port] = r
            it = self.tbl.item(r, 0)
            if it:
                it.setText(new_port)


# -------------------- 手动补点（浓度） --------------------

class ManualConcPointsDialog(QDialog):
    def __init__(
            self,
            parent: QWidget,
            devices: List[SensorDevice],
            last_report: Optional[dict] = None,
            mode: str = "CAL_PARTIAL",
            min_points_required: int = 0,
    ):
        super().__init__(parent)
        self.setWindowTitle("手动补点 / 修正")
        self.resize(1080, 560)
        self.devices = [d for d in devices if d.selected]
        self.last_report = last_report or {}
        self.mode = (mode or "CAL_PARTIAL").upper()
        self.min_points_required = max(0, int(min_points_required))
        self.points_by_sn: Dict[str, Dict[str, Any]] = {}

        layout = QVBoxLayout()

        if self.mode in ("FULL", "WRITE_EDIT"):
            tip_txt = (
                "说明：当前阶段要求【每个传感器】都填写完整的【零点N】与【三个浓度点(PPM,M)】。\n"
                "填写完成后，软件会使用这些数据继续后续写入/回读流程。"
            )
        else:
            tip_txt = (
                "说明：你可以只补齐缺失的一个或两个浓度点（PPM 与 M 必须成对填写）。\n"
                "但要求：所有传感器最终填写的浓度点数量必须一致（例如都只有点1，或都完成点1+点2）。"
            )
            if self.min_points_required > 0:
                tip_txt += f"\n当前至少需要保证每个传感器已完成 {self.min_points_required} 个浓度点。"

        tip = QLabel(tip_txt)
        tip.setStyleSheet("color: blue; font-weight: bold;")
        tip.setWordWrap(True)
        layout.addWidget(tip)

        cols = ["端口", "SN", "零点N",
                "P1_PPM", "P1_M",
                "P2_PPM", "P2_M",
                "P3_PPM", "P3_M"]
        self.tbl = QTableWidget(0, len(cols))
        self.tbl.setHorizontalHeaderLabels(cols)
        self.tbl.horizontalHeader().setStretchLastSection(True)
        self.tbl.setEditTriggers(QTableWidget.EditTrigger.DoubleClicked | QTableWidget.EditTrigger.EditKeyPressed)
        self.tbl.setAlternatingRowColors(True)
        layout.addWidget(self.tbl)

        self._init_rows()
        self._prefill_from_report()

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
            sn_key = sanitize_sn(d.current_sn or d.expected_sn or b'')
            it_sn = QTableWidgetItem(sn_key)
            it_sn.setFlags(it_sn.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.tbl.setItem(r, 1, it_sn)
            for c in range(2, 9):
                self.tbl.setItem(r, c, QTableWidgetItem(""))

    def _prefill_from_report(self):
        rep = self.last_report if isinstance(self.last_report, dict) else {}
        for r in range(self.tbl.rowCount()):
            sn = (self.tbl.item(r, 1).text() or "").strip()
            info = rep.get(sn) if isinstance(rep, dict) else None
            if not info or not isinstance(info, dict):
                continue

            z = info.get("zero_n")
            if z is None:
                z2 = (info.get("zero") or {}).get("avg")
                z = z2
            if z is not None:
                self.tbl.item(r, 2).setText(str(int(z)))

            pts = info.get("cal_points") or []
            if isinstance(pts, list):
                try:
                    pts2 = sorted(
                        [p for p in pts if isinstance(p, dict) and "ppm" in p and "m" in p],
                        key=lambda x: float(x.get("ppm", 0)),
                    )
                except Exception:
                    pts2 = []
                for i in range(min(3, len(pts2))):
                    ppm = pts2[i].get("ppm")
                    m = pts2[i].get("m")
                    if ppm is not None:
                        self.tbl.item(r, 3 + i * 2).setText(str(int(float(ppm))))
                    if m is not None:
                        self.tbl.item(r, 4 + i * 2).setText(str(int(float(m))))

    @staticmethod
    def _parse_int_field(s: str) -> Optional[int]:
        s = (s or "").strip()
        if s == "":
            return None
        try:
            return int(round(float(s)))
        except Exception:
            return None

    def _on_ok(self):
        points: Dict[str, Dict[str, Any]] = {}
        errors: List[str] = []
        counts: List[int] = []

        for r in range(self.tbl.rowCount()):
            sn = (self.tbl.item(r, 1).text() or "").strip()
            row_map: Dict[str, Any] = {}

            z = self._parse_int_field(self.tbl.item(r, 2).text() if self.tbl.item(r, 2) else "")
            if z is not None:
                row_map["zero_n"] = int(z)

            filled = 0
            for i in range(1, 4):
                ppm = self._parse_int_field(
                    self.tbl.item(r, 3 + (i - 1) * 2).text() if self.tbl.item(r, 3 + (i - 1) * 2) else "")
                m = self._parse_int_field(
                    self.tbl.item(r, 4 + (i - 1) * 2).text() if self.tbl.item(r, 4 + (i - 1) * 2) else "")
                if ppm is None and m is None:
                    continue
                if ppm is None or m is None:
                    errors.append(f"[{sn}] P{i} 点不完整（PPM与M必须同时填）")
                    continue
                row_map[f"P{i}"] = {"ppm": int(ppm), "m": int(m)}
                filled += 1

            counts.append(filled)

            if self.mode in ("FULL", "WRITE_EDIT"):
                if "zero_n" not in row_map:
                    errors.append(f"[{sn}] 必须填写零点N")
                if filled != 3:
                    errors.append(f"[{sn}] 必须填写 3 个浓度点（目前 {filled} 个）")

            points[sn] = row_map

        if self.mode == "CAL_PARTIAL":
            uniq = sorted(set(counts))
            if len(uniq) > 1:
                errors.append(f"所有传感器填写的浓度点数量必须一致（当前分别为：{uniq}）")
            else:
                cnt = uniq[0] if uniq else 0
                if self.min_points_required and cnt < self.min_points_required:
                    errors.append(f"每个传感器至少需要 {self.min_points_required} 个浓度点（当前 {cnt} 个）")
                if cnt == 0 and self.min_points_required == 0:
                    errors.append("未填写任何浓度点；请至少补齐一个点，或取消。")

        if self.mode in ("FULL", "WRITE_EDIT"):
            for sn, m in points.items():
                if not m or ("zero_n" not in m):
                    continue
                for i in range(1, 4):
                    if f"P{i}" not in m:
                        continue

        if errors:
            QMessageBox.warning(self, "输入有误", "请修正以下问题：\n" + "\n".join(errors[:20]))
            return

        out: Dict[str, Dict[str, Any]] = {}
        for sn, row_map in points.items():
            if row_map:
                out[sn] = row_map

        self.points_by_sn = out
        self.accept()

    def get_points(self) -> Dict[str, Dict[str, Any]]:
        return self.points_by_sn


# -------------------- 浓度标定线程（总控 + UI交互） --------------------

class ConcentrationCalibrationWorker(QThread):
    log_signal = pyqtSignal(str)
    stage_signal = pyqtSignal(str)
    sensor_status_signal = pyqtSignal(str, str)
    sample_signal = pyqtSignal(str, dict)
    iotdb_status_signal = pyqtSignal(str, int, bool, str)
    need_user_action = pyqtSignal(str, dict)
    finished_signal = pyqtSignal(dict)

    phase_signal = pyqtSignal(str)
    countdown_signal = pyqtSignal(int, str)
    # 核心：端口映射变更信号，通知全链路UI
    port_remap_signal = pyqtSignal(str, str)

    def __init__(
            self,
            devices: List[SensorDevice],
            baudrate: int,
            # iotdb
            iotdb_enable: bool,
            iotdb_host: str,
            iotdb_port: int,
            iotdb_user: str,
            iotdb_pass: str,
            iotdb_sg: str,
            iotdb_downsample_ms: int,
            # params
            max_ppm: int = 40000,
            min_ppm: int = 1000,
            verify_tol_percent: float = 5.0,
            do_factory_reset_at_start: bool = True,
            hold_seconds: int = 60,
            do_verify: bool = True,
            run_mode: str = 'CALIBRATE',
            force_factory_reset_before_points: bool = False,
            manual_points_by_sn: Optional[Dict[str, Dict[str, dict]]] = None,
    ):
        super().__init__()
        self.devices = [d for d in devices if d.selected]
        self.baudrate = int(baudrate)

        self.iotdb_enable = bool(iotdb_enable)
        self.iotdb_host = iotdb_host
        self.iotdb_port = int(iotdb_port)
        self.iotdb_user = iotdb_user
        self.iotdb_pass = iotdb_pass
        self.iotdb_sg = iotdb_sg
        self.iotdb_downsample_ms = int(iotdb_downsample_ms)

        self.max_ppm = int(max_ppm)
        self.min_ppm = int(min_ppm)
        self.verify_tol_percent = float(verify_tol_percent)
        self.do_factory_reset_at_start = bool(do_factory_reset_at_start)
        self.hold_seconds = float(max(1, int(hold_seconds)))
        self.do_verify = bool(do_verify)
        self.run_mode = str(run_mode or 'CALIBRATE').strip().upper()
        self.force_factory_reset_before_points = bool(force_factory_reset_before_points)
        self.manual_points_by_sn = manual_points_by_sn or None

        self._phase = 'INIT'
        self._stop = threading.Event()
        self._rescan = threading.Event()

        self._streams: Dict[str, SensorStreamThread] = {}
        self._iotdb_writer: Optional[IoTDBWriterThread] = None
        self._current_mode = 'idle'

        self._input_lock = threading.Lock()
        self._input_events: Dict[str, threading.Event] = {}
        self._input_values: Dict[str, Any] = {}

        self.report: Dict[str, dict] = {}
        self.cal_points: Dict[str, List[Dict[str, int]]] = {}
        self.verify_points: Dict[str, List[Dict[str, float]]] = {}
        self.m_to_s_scale = 1000

    def stop(self):
        self._stop.set()

    def request_rescan(self):
        self._rescan.set()

    def provide_input(self, token: str, value: Any):
        with self._input_lock:
            self._input_values[token] = value
            ev = self._input_events.get(token)
            if ev:
                ev.set()

    def _log_port(self, d: SensorDevice, msg: str):
        try:
            self.log_signal.emit(f"[{d.port}] {msg}")
        except Exception:
            pass

    def _ask_ui(self, kind: str, payload: dict, timeout_s: Optional[float] = None) -> Optional[Any]:
        token = f"{kind}_{now_ms()}_{len(self._input_events)}"
        payload2 = dict(payload or {})
        payload2['token'] = token
        with self._input_lock:
            ev = threading.Event()
            self._input_events[token] = ev
        self.need_user_action.emit(kind, payload2)

        t0 = time.time()
        while not self._stop.is_set():
            if self._rescan.is_set():
                self._maybe_rescan()
            if ev.is_set():
                with self._input_lock:
                    val = self._input_values.get(token)
                return val
            if timeout_s is not None and (time.time() - t0) >= float(timeout_s):
                return None
            time.sleep(0.1)
        return None

    def _start_iotdb_and_streams(self):
        if self.iotdb_enable and self._iotdb_writer is None:
            self._iotdb_writer = IoTDBWriterThread(
                self.iotdb_host, self.iotdb_port, self.iotdb_user, self.iotdb_pass, self.iotdb_sg, enable=True
            )
            self._iotdb_writer.log_signal.connect(self.log_signal.emit)
            try:
                self._iotdb_writer.status_signal.connect(self.iotdb_status_signal)
            except Exception:
                pass
            self._iotdb_writer.start()

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

        # 不要在这里 stop iotdb_writer，因为数据还得续写
        # 真正退出 run() 时再去关 IoTDB

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

    def _enter_debug_all(self):
        self._current_mode = 'debug'
        for d in self.devices:
            ok = d.sensor.enter_debug()
            self.sensor_status_signal.emit(d.port, '调试模式' if ok else '⚠️ 调试模式失败')

    def _enter_user_all(self):
        self._current_mode = 'user'
        for d in self.devices:
            ok = d.sensor.enter_user()
            self.sensor_status_signal.emit(d.port, '用户模式' if ok else '⚠️ 用户模式失败')

    def _refresh_sn_all(self):
        for d in self.devices:
            sn = d.sensor.read_sn()
            d.current_sn = sn
            if d.expected_sn is None and sn is not None:
                d.expected_sn = sn

    def _assert_sn(self, d: SensorDevice, retries: int = 3, delay_s: float = 0.12) -> Tuple[bool, str]:
        if d.expected_sn is None and d.current_sn:
            d.expected_sn = d.current_sn
        sn_now: Optional[bytes] = None
        retries_i = max(1, int(retries))
        for i in range(retries_i):
            try:
                sn_now = d.sensor.read_sn()
            except Exception:
                sn_now = None
            if sn_now:
                break
            if i < retries_i - 1:
                time.sleep(float(delay_s))

        if not sn_now:
            return False, 'READ_FAIL'
        d.current_sn = sn_now
        if d.expected_sn is None:
            d.expected_sn = sn_now
            return True, 'EXPECTED_SET'
        if sn_now != d.expected_sn:
            return False, 'MISMATCH'
        return True, 'MATCH'

    def _maybe_rescan(self) -> bool:
        if not self._rescan.is_set() or self._stop.is_set():
            return False
        self._rescan.clear()
        self.log_signal.emit('🔄 接触不良：开始扫描所有串口并按SN重映射端口...')

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

        # 全局扫描所有的串口并读取 SN
        ports = [p.device for p in list_ports.comports()]
        found: Dict[str, Optional[bytes]] = {}
        for dev in ports:
            s2 = UniversalSensor(dev, self.baudrate, timeout=0.6)
            if not s2.open():
                continue
            try:
                if not s2.enter_user():
                    continue
                sn = s2.read_sn()
                found[dev] = sn
            except Exception:
                pass
            finally:
                s2.close()

        all_ok = True
        for d in self.devices:
            exp = d.expected_sn
            matched_port = None

            # 核心：根据 expected_sn 找它现在飘到了哪个 COM 口
            for p, sn in found.items():
                if exp is not None and sn is not None and sn == exp:
                    matched_port = p
                    break

            if matched_port:
                if matched_port != d.port:
                    old_p = d.port
                    d.port = matched_port
                    # 发送重映射信号，UI全面更新
                    self.port_remap_signal.emit(old_p, matched_port)

                    # 更新报告中的 port
                    sn_key = sanitize_sn(exp or d.current_sn or b'')
                    if sn_key in self.report:
                        self.report[sn_key]['port'] = matched_port

                self.sensor_status_signal.emit(d.port, '✅ 重新识别OK')
            else:
                all_ok = False
                self.sensor_status_signal.emit(d.port, '❌ 重新识别失败(未找到匹配的SN)')

        if not all_ok:
            self.log_signal.emit('❌ 重新扫描未能匹配全部设备的SN（请检查线缆插拔），将继续等待。')
            return False

        if not self._open_sensors():
            self.log_signal.emit('❌ 重连后串口打开失败，将继续等待。')
            return False

        if self._current_mode == 'user':
            self._enter_user_all()
        else:
            self._enter_debug_all()
            if was_streaming:
                self._start_iotdb_and_streams()

        self.log_signal.emit('✅ 端口重连完成，继续当前流程。')
        return True

    def _set_phase(self, phase: str):
        phase = str(phase or '').strip().upper() or 'INIT'
        self._phase = phase
        try:
            self.phase_signal.emit(phase)
        except Exception:
            pass

    def _clear_countdown(self):
        try:
            self.countdown_signal.emit(0, '')
        except Exception:
            pass

    def _collect_hold_stats_all(self, key: str, duration_s: float, label: str) -> Dict[str, dict]:
        key = str(key)
        label = str(label or key)
        duration_s = float(max(1.0, duration_s))
        data_by_sn: Dict[str, List[float]] = {}
        for d in self.devices:
            sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
            data_by_sn[sn_key] = []

        self.log_signal.emit(f'⏳ {label}：开始采集 {int(duration_s)}s 平均值（同时记录最大/最小/次数）...')
        t0 = time.time()
        deadline = t0 + duration_s
        last_remain = None

        while (time.time() < deadline) and (not self._stop.is_set()):
            if self._rescan.is_set():
                self.log_signal.emit(f'🔄 {label} 采集中检测到“重新扫描”请求，将重连并重新开始计时。')
                ok = self._maybe_rescan()
                t0 = time.time()
                deadline = t0 + duration_s
                for k in data_by_sn.keys():
                    data_by_sn[k].clear()
                if not ok:
                    time.sleep(0.2)
                    continue

            for d in self.devices:
                sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                v = d.last_vals.get(key)
                if v is None:
                    continue
                try:
                    data_by_sn[sn_key].append(float(v))
                except Exception:
                    pass

            remain = int(math.ceil(max(0.0, deadline - time.time())))
            if remain != last_remain:
                last_remain = remain
                try:
                    self.countdown_signal.emit(remain, label)
                except Exception:
                    pass
            time.sleep(0.2)

        self._clear_countdown()

        stats_by_sn: Dict[str, dict] = {}
        for d in self.devices:
            sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
            arr = data_by_sn.get(sn_key) or []
            if not arr:
                stats_by_sn[sn_key] = {'avg': None, 'min': None, 'max': None, 'count': 0, 'duration_s': duration_s,
                                       'key': key}
                continue
            avg_f = sum(arr) / float(len(arr))
            mn_f = min(arr)
            mx_f = max(arr)
            stats_by_sn[sn_key] = {
                'avg_f': float(avg_f),
                'min_f': float(mn_f),
                'max_f': float(mx_f),
                'avg': int(round(avg_f)),
                'min': int(round(mn_f)),
                'max': int(round(mx_f)),
                'count': int(len(arr)),
                'duration_s': float(duration_s),
                'key': key,
            }
        return stats_by_sn

    def _safe_user_mode(self):
        try:
            if self._streams:
                self._stop_streams()
        except Exception:
            pass
        self._enter_user_all()
        time.sleep(0.05)

    def _safe_debug_mode(self):
        self._enter_debug_all()
        time.sleep(0.05)
        try:
            if not self._streams:
                self._start_iotdb_and_streams()
        except Exception:
            pass

    def _write_zero_and_range(self) -> bool:
        self.stage_signal.emit('写入零点N + 量程(最大/最小)...')
        self._safe_user_mode()

        ok_all = True
        for d in self.devices:
            sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
            ok_sn, sn_reason = self._assert_sn(d)
            if not ok_sn:
                if sn_reason == 'READ_FAIL':
                    self.sensor_status_signal.emit(d.port, '⚠️ SN读取超时(请重新扫描后重试)')
                    ok_all = False
                    continue
                self.sensor_status_signal.emit(d.port, '❌ SN不匹配(写入中止)')
                self.report[sn_key]['final'] = 'FAIL_SN_MISMATCH'
                ok_all = False
                continue

            if self.do_factory_reset_at_start:
                ok_fr = d.sensor.conc_factory_reset()
                self.log_signal.emit(f'[{d.port}] 恢复浓度出厂(0x34): {"OK" if ok_fr else "FAIL"}')
                if not ok_fr:
                    ok_all = False

            z = self.report[sn_key].get('zero_n', None)
            if z is None:
                self.sensor_status_signal.emit(d.port, '❌ 零点N缺失')
                self.report[sn_key]['final'] = 'FAIL_ZERO_MISSING'
                ok_all = False
                continue

            z_u16 = u16_from_int16(int(z))
            ok_z = d.sensor.write_reg_u16(0x001C, z_u16)
            self.log_signal.emit(f'[{d.port}] 写零点N(0x001C)={int(z)} -> {"OK" if ok_z else "FAIL"}')
            if not ok_z:
                ok_all = False
                self.report[sn_key]['final'] = 'FAIL_WRITE_ZERO'
                self.sensor_status_signal.emit(d.port, '❌ 写零点失败')
                continue

            ok_max = d.sensor.write_reg_u16(0x0011, int(self.max_ppm) & 0xFFFF)
            ok_min = d.sensor.write_reg_u16(0x0013, int(self.min_ppm) & 0xFFFF)
            self.log_signal.emit(f'[{d.port}] 写最大(0x0011)={self.max_ppm} -> {"OK" if ok_max else "FAIL"}')
            self.log_signal.emit(f'[{d.port}] 写最小(0x0013)={self.min_ppm} -> {"OK" if ok_min else "FAIL"}')
            if not (ok_max and ok_min):
                ok_all = False
                self.report[sn_key]['final'] = 'FAIL_WRITE_RANGE'
                self.sensor_status_signal.emit(d.port, '⚠️ 量程写入异常')
            else:
                self.sensor_status_signal.emit(d.port, '✅ 零点&量程写入OK')

        return ok_all

    def _write_and_readback_points(self, force_factory_reset: bool = False) -> Tuple[bool, List[str]]:
        self.stage_signal.emit('写入三点浓度标定(0x2A)并回读校验(0x2B)...')
        self._safe_user_mode()

        fail_lines: List[str] = []

        if bool(force_factory_reset):
            for d in self.devices:
                try:
                    ok_fr = d.sensor.conc_factory_reset()
                    self._log_port(d, f'强制恢复浓度出厂(0x34) -> {"OK" if ok_fr else "FAIL"}')
                except Exception:
                    self._log_port(d, '强制恢复浓度出厂(0x34) -> EXCEPTION')
            time.sleep(0.10)

        for d in self.devices:
            sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
            ok_sn, sn_reason = self._assert_sn(d)
            if not ok_sn:
                if sn_reason == 'READ_FAIL':
                    fail_lines.append(f'{d.port} {sn_key}: SN读取超时')
                    self.sensor_status_signal.emit(d.port, '⚠️ SN读取超时')
                else:
                    fail_lines.append(f'{d.port} {sn_key}: SN不匹配')
                    self.sensor_status_signal.emit(d.port, '❌ SN不匹配')
                continue

            ok_num = d.sensor.conc_set_num_points(3)
            self._log_port(d, f'设置点数=3 -> {"OK" if ok_num else "FAIL"}')
            if not ok_num:
                fail_lines.append(f'{d.port} {sn_key}: 设置点数失败(0x2A 00)')
        time.sleep(0.05)

        for d in self.devices:
            sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
            if sn_key not in self.cal_points or len(self.cal_points[sn_key]) < 3:
                fail_lines.append(f'{d.port} {sn_key}: 标定点不足3个')
                continue

            ok_sn, sn_reason = self._assert_sn(d)
            if not ok_sn:
                continue

            pts = sorted(self.cal_points[sn_key], key=lambda x: int(x['ppm']))
            ok_w = True
            fail_idx = 0
            for idx, p in enumerate(pts, start=1):
                s_write = int(p.get('s', int(p['m']) * self.m_to_s_scale))
                ok = d.sensor.conc_write_point(idx, int(p['ppm']), s_write)
                if not ok:
                    ok_w = False
                    fail_idx = idx
                    break
                time.sleep(0.05)

            if ok_w:
                self.sensor_status_signal.emit(d.port, '✅ 0x2A写入OK')
                try:
                    pts_txt = ",".join([f"({int(p['ppm'])},{int(p['m'])})" for p in pts[:3]])
                except Exception:
                    pts_txt = ""
                self._log_port(d, f'0x2A写入OK：{pts_txt}')
            else:
                self.sensor_status_signal.emit(d.port, '❌ 0x2A写入失败')
                fail_lines.append(f'{d.port} {sn_key}: 写入点失败(0x2A idx={fail_idx})')
                self._log_port(d, f'0x2A写入失败：idx={fail_idx}')

        for d in self.devices:
            sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
            if sn_key not in self.cal_points or len(self.cal_points[sn_key]) < 3:
                continue

            ok_sn, sn_reason = self._assert_sn(d)
            if not ok_sn:
                continue

            num = d.sensor.conc_read_num()
            if num != 3:
                fail_lines.append(f'{d.port} {sn_key}: 回读点数num={num} != 3')
                self.sensor_status_signal.emit(d.port, '⚠️ 0x2B点数异常')
                self._log_port(d, f'0x2B点数异常：num={num}')
                continue

            rb: List[Tuple[int, int]] = []
            for idx in range(1, 4):
                it = d.sensor.conc_read_point(idx)
                if not it:
                    fail_lines.append(f'{d.port} {sn_key}: 回读点{idx}失败(0x2B)')
                    continue
                rb.append(it)

            pts = sorted(self.cal_points[sn_key], key=lambda x: int(x['ppm']))
            ok_match = True
            if len(rb) < 3:
                ok_match = False
            else:
                for (ppm_r, s_r), p_w in zip(rb[:3], pts[:3]):
                    exp_s = int(p_w.get('s', int(p_w['m']) * self.m_to_s_scale))
                    if int(ppm_r) != int(p_w['ppm']) or int(s_r) != exp_s:
                        ok_match = False
                        break

            self.report[sn_key]['conc_readback'] = {
                'num': num,
                'points': [{'ppm': int(ppm), 's': int(s), 'm': float(s) / float(self.m_to_s_scale)} for (ppm, s) in rb],
            }

            try:
                rb_txt = ",".join(
                    [f"({int(ppm)},{int(round(float(s) / float(self.m_to_s_scale)))})" for (ppm, s) in rb[:3]])
            except Exception:
                rb_txt = ""

            if ok_match:
                self.report[sn_key]['conc_write_ok'] = True
                self.sensor_status_signal.emit(d.port, '✅ 0x2B回读一致')
                self._log_port(d, f'0x2B回读一致：{rb_txt}')
            else:
                self.report[sn_key]['conc_write_ok'] = False
                self.sensor_status_signal.emit(d.port, '❌ 0x2B不一致')
                fail_lines.append(f'{d.port} {sn_key}: 回读与写入不一致')
                self._log_port(d, f'0x2B回读不一致：{rb_txt}')

        try:
            if fail_lines:
                self.log_signal.emit(f"❌ 写入/回读阶段存在失败：{len(fail_lines)} 项（见日志；可点击“接触不良，重新扫描”自动重试）")
            else:
                self.log_signal.emit("✅ 写入/回读阶段全部成功。")
        except Exception:
            pass

        return (len(fail_lines) == 0), fail_lines

    def _recover_write_stage_after_bad_contact(self) -> bool:
        ok_scan = self._maybe_rescan()
        if not ok_scan:
            return False
        ok_zero = self._write_zero_and_range()
        ok_pts, fails = self._write_and_readback_points(force_factory_reset=True)
        if ok_zero and ok_pts:
            self.log_signal.emit('✅ 接触不良恢复：重写并回读全部成功。')
            return True
        self.log_signal.emit('⚠️ 接触不良恢复后仍存在失败：')
        for ln in fails[:30]:
            self.log_signal.emit('  - ' + ln)
        if len(fails) > 30:
            self.log_signal.emit(f'  ... 共 {len(fails)} 条')
        return False

    def run(self):
        if not self.devices:
            self.log_signal.emit('没有选中的传感器，无法标定。')
            self.finished_signal.emit({})
            return

        try:
            self._set_phase('INIT')

            self.stage_signal.emit('打开串口...')
            if not self._open_sensors():
                self.finished_signal.emit({})
                return

            self.stage_signal.emit('读取SN...')
            self._enter_user_all()
            self._refresh_sn_all()
            for d in self.devices:
                if not d.current_sn:
                    self.sensor_status_signal.emit(d.port, '⚠️ SN读取失败')
                else:
                    self.sensor_status_signal.emit(d.port, 'SN=' + sanitize_sn(d.current_sn))

            for d in self.devices:
                sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                self.report[sn_key] = {
                    'port': d.port,
                    'sn_hex_ascii': fmt_sn_dual(d.expected_sn or d.current_sn),
                    'zero_n': None,
                    'zero': None,
                    'range': {'max_ppm': int(self.max_ppm), 'min_ppm': int(self.min_ppm)},
                    'cal_points': [],
                    'conc_write_ok': False,
                    'conc_readback': None,
                    'verify_points': [],
                    'verify_tol_percent': float(self.verify_tol_percent),
                    'hold_seconds': float(self.hold_seconds),
                    'run_mode': str(self.run_mode),
                    'final': 'UNKNOWN',
                }
                self.cal_points[sn_key] = []
                self.verify_points[sn_key] = []

            self.stage_signal.emit('进入调试模式 & 开始采集...')
            self._enter_debug_all()
            self._start_iotdb_and_streams()
            time.sleep(0.3)

            if self.manual_points_by_sn:
                self.stage_signal.emit('载入手动补点数据...')
                for d in self.devices:
                    sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                    mp = self.manual_points_by_sn.get(sn_key)
                    if not mp:
                        continue

                    if 'zero_n' in mp:
                        try:
                            z = int(round(float(mp['zero_n'])))
                            self.report[sn_key]['zero_n'] = int(z)
                            self.report[sn_key]['zero'] = {'avg': int(z), 'min': int(z), 'max': int(z), 'count': 1,
                                                           'duration_s': 0.0, 'key': 'N'}
                        except Exception:
                            pass

                    pts: List[Dict[str, int]] = []
                    for i in range(1, 4):
                        p = mp.get(f'P{i}')
                        if not p:
                            continue
                        try:
                            ppm = int(round(float(p['ppm'])))
                            m = int(round(float(p['m'])))
                            pts.append({'ppm': ppm, 'm': m, 'm_min': m, 'm_max': m, 'm_count': 1, 'hold_s': 0.0,
                                        's': m * self.m_to_s_scale, 'source': 'MANUAL'})
                        except Exception:
                            pass
                    if pts:
                        pts = sorted(pts, key=lambda x: int(x.get('ppm', 0)))
                        self.cal_points[sn_key] = pts[:3]
                        self.report[sn_key]['cal_points'] = [
                            {
                                'ppm': int(x['ppm']),
                                'm': int(x['m']),
                                'm_min': int(x.get('m_min', x['m'])),
                                'm_max': int(x.get('m_max', x['m'])),
                                'm_count': int(x.get('m_count', 1)),
                                'hold_s': float(x.get('hold_s', 0.0)),
                                's': int(x.get('s', int(x['m']) * self.m_to_s_scale)),
                                'source': x.get('source', 'MANUAL'),
                            } for x in pts[:3]
                        ]
                        self.sensor_status_signal.emit(d.port, f'✅ 已载入手动点 {len(pts[:3])}/3')

            # -------------------- VERIFY ONLY --------------------
            if self.run_mode == 'VERIFY_ONLY':
                self._set_phase('VERIFY')
                self._safe_debug_mode()
                if not self._streams:
                    self._start_iotdb_and_streams()

                default_ppms = [10000, 20000, 30000]
                for vi in range(1, 4):
                    if self._stop.is_set():
                        break
                    self.stage_signal.emit(f'浓度复核点{vi}/3：到达后点击“到达并记录”(将采集 {int(self.hold_seconds)}s 平均J)')
                    ppm_v = self._ask_ui('HOLD_VERIFY_POINT', {
                        'idx': vi,
                        'default_ppm': default_ppms[vi - 1],
                        'hold_s': int(self.hold_seconds),
                        'text': f'复核点{vi}：请稳定后点击“到达并记录”，随后输入当前ppm。'
                    })
                    if ppm_v is None or self._stop.is_set():
                        self._stop.set()
                        break
                    try:
                        ppm_i = int(ppm_v)
                    except Exception:
                        ppm_i = default_ppms[vi - 1]
                    ppm_i = max(0, min(40000, ppm_i))

                    stats_map = self._collect_hold_stats_all('J', self.hold_seconds, f'复核点{vi} J')

                    for d in self.devices:
                        sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                        st = stats_map.get(sn_key) or {}
                        if not st.get('count'):
                            self.sensor_status_signal.emit(d.port, f'❌ 复核点{vi}无J数据')
                            self.report[sn_key]['final'] = 'FAIL_NO_J'
                            continue

                        j_avg = int(st.get('avg'))
                        err_percent = (float(j_avg) - float(ppm_i)) / 40000.0 * 100.0
                        ok = abs(err_percent) <= float(self.verify_tol_percent)

                        rec = {
                            'ppm': float(ppm_i),
                            'j': float(j_avg),
                            'j_min': int(st.get('min')) if st.get('min') is not None else None,
                            'j_max': int(st.get('max')) if st.get('max') is not None else None,
                            'j_count': int(st.get('count', 0)),
                            'hold_s': float(st.get('duration_s', self.hold_seconds)),
                            'err_percent': float(err_percent),
                            'ok': bool(ok),
                        }
                        self.verify_points[sn_key].append(rec)
                        self.report[sn_key]['verify_points'] = self.verify_points[sn_key]
                        self.sensor_status_signal.emit(d.port,
                                                       f'复核{vi}: err={err_percent:+.2f}% {"OK" if ok else "FAIL"}')
                        self._log_port(d,
                                       f'复核点{vi}：输入ppm={ppm_i}, J(avg)={j_avg}, min={st.get("min")}, max={st.get("max")}, n={st.get("count")} err={err_percent:+.2f}% ({"OK" if ok else "FAIL"})')

                for d in self.devices:
                    sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                    info = self.report.get(sn_key, {}) or {}
                    if info.get('final') not in (None, 'UNKNOWN'):
                        continue
                    vps = info.get('verify_points') or []
                    if not vps:
                        info['final'] = 'FAIL_NO_VERIFY'
                    elif all(bool(p.get('ok')) for p in vps[:3]):
                        info['final'] = 'PASS_VERIFY_ONLY'
                    else:
                        info['final'] = 'FAIL_VERIFY'

                self._set_phase('DONE')
                self.stage_signal.emit('完成')
                self.finished_signal.emit(self.report)
                return

            # -------------------- CALIBRATION --------------------
            self._set_phase('ZERO')
            need_zero_wait = any(
                (self.report[sanitize_sn(d.expected_sn or d.current_sn or b'')].get('zero_n') is None) for d in
                self.devices
            )
            if need_zero_wait:
                self.stage_signal.emit(f'零点：请调整到0ppm并点击“到达并记录”(将采集 {int(self.hold_seconds)}s 平均N)')
                _ = self._ask_ui('HOLD_ZERO',
                                 {'hold_s': int(self.hold_seconds), 'text': '请确认当前环境为 0ppm（氢气浓度=0），然后点击“到达并记录”。'})
                if self._stop.is_set():
                    raise RuntimeError('Stopped')

                stats_map = self._collect_hold_stats_all('N', self.hold_seconds, '零点N')

                for d in self.devices:
                    sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                    if self.report[sn_key].get('zero_n') is not None:
                        continue
                    st = stats_map.get(sn_key) or {}
                    if not st.get('count'):
                        self.sensor_status_signal.emit(d.port, '❌ 零点阶段无N数据')
                        self.report[sn_key]['final'] = 'FAIL_NO_N'
                        continue
                    n_i16 = int(st.get('avg'))
                    self.report[sn_key]['zero_n'] = int(n_i16)
                    self.report[sn_key]['zero'] = {
                        'avg': int(n_i16),
                        'min': int(st.get('min')) if st.get('min') is not None else None,
                        'max': int(st.get('max')) if st.get('max') is not None else None,
                        'count': int(st.get('count', 0)),
                        'duration_s': float(st.get('duration_s', self.hold_seconds)),
                        'key': 'N',
                    }
                    self.sensor_status_signal.emit(d.port, f'零点N(avg)={n_i16}')
                    self._log_port(d,
                                   f'零点记录：N(avg)={n_i16}, min={st.get("min")}, max={st.get("max")}, n={st.get("count")}')

            self._set_phase('WRITE')
            ok_zero = self._write_zero_and_range()

            if not ok_zero and not self._stop.is_set():
                self.log_signal.emit('❗ 检测到零点/量程写入未完成，已暂停后续流程。请处理后点击“接触不良，重新扫描”（将按SN重连并重写零点/量程）。')
                self.stage_signal.emit('零点/量程写入失败：等待“接触不良，重新扫描”或“停止”')
                while not self._stop.is_set():
                    if self._rescan.is_set():
                        self.stage_signal.emit('零点/量程写入失败：执行接触不良恢复（重连+重写）...')
                        if self._maybe_rescan():
                            ok_zero = self._write_zero_and_range()
                            if ok_zero:
                                break
                    time.sleep(0.2)

            self._set_phase('CAL')
            self._safe_debug_mode()
            if not self._streams:
                self._start_iotdb_and_streams()

            default_ppms = [10000, 20000, 30000]
            for pi in range(1, 4):
                if self._stop.is_set():
                    break
                need_cal_wait = False
                for d in self.devices:
                    sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                    if len(self.cal_points.get(sn_key, [])) < pi:
                        need_cal_wait = True
                        break

                if not need_cal_wait:
                    continue

                self.stage_signal.emit(f'浓度标定点{pi}/3：到达后点击“到达并记录”(将采集 {int(self.hold_seconds)}s 平均M)')
                ppm_val = self._ask_ui('HOLD_CAL_POINT', {
                    'idx': pi,
                    'default_ppm': default_ppms[pi - 1],
                    'hold_s': int(self.hold_seconds),
                    'text': f'标定点{pi}：请稳定后点击“到达并记录”，随后输入当前ppm。'
                })
                if ppm_val is None or self._stop.is_set():
                    self._stop.set()
                    break
                try:
                    ppm_i = int(ppm_val)
                except Exception:
                    ppm_i = default_ppms[pi - 1]
                ppm_i = max(0, min(40000, ppm_i))

                stats_map = self._collect_hold_stats_all('M', self.hold_seconds, f'标定点{pi} M')

                for d in self.devices:
                    sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                    already = self.cal_points.get(sn_key, [])
                    if len(already) >= pi:
                        continue

                    st = stats_map.get(sn_key) or {}
                    if not st.get('count'):
                        self.sensor_status_signal.emit(d.port, f'⚠️ 点{pi}无M数据')
                        self._log_port(d, f'标定点{pi}无M数据')
                        continue

                    m_i32 = int(st.get('avg'))
                    self.cal_points[sn_key].append({
                        'ppm': int(ppm_i),
                        'm': int(m_i32),
                        'm_min': int(st.get('min')) if st.get('min') is not None else None,
                        'm_max': int(st.get('max')) if st.get('max') is not None else None,
                        'm_count': int(st.get('count', 0)),
                        'hold_s': float(st.get('duration_s', self.hold_seconds)),
                        's': int(m_i32) * self.m_to_s_scale,
                        'source': 'AUTO_HOLD',
                    })
                    self.report[sn_key]['cal_points'] = [
                        {
                            'ppm': int(x.get('ppm', 0)),
                            'm': int(x.get('m', 0)),
                            'm_min': x.get('m_min'),
                            'm_max': x.get('m_max'),
                            'm_count': x.get('m_count'),
                            'hold_s': x.get('hold_s'),
                            's': int(x.get('s', int(x.get('m', 0)) * self.m_to_s_scale)),
                            'source': x.get('source', 'AUTO_HOLD'),
                        } for x in self.cal_points[sn_key]
                    ]
                    self.sensor_status_signal.emit(d.port, f'点{pi}: PPM={ppm_i} M(avg)={m_i32}')
                    self._log_port(d,
                                   f'标定点{pi}记录：ppm={ppm_i}, M(avg)={m_i32}, min={st.get("min")}, max={st.get("max")}, n={st.get("count")}')

            self._set_phase('WRITE')
            ok_pts, fail_lines = self._write_and_readback_points(
                force_factory_reset=bool(self.force_factory_reset_before_points))

            if not ok_pts and not self._stop.is_set():
                self.log_signal.emit('❗ 检测到写入/回读未完成，已暂停进入复核。请处理后点击“接触不良，重新扫描”（将重写并回读）。')
                for ln in fail_lines[:30]:
                    self.log_signal.emit('  - ' + ln)
                self.stage_signal.emit('写入失败：等待“接触不良，重新扫描”或“停止”')
                while not self._stop.is_set():
                    if self._rescan.is_set():
                        self.stage_signal.emit('写入失败：执行接触不良恢复（重连+强制恢复出厂+重写并回读）...')
                        if self._recover_write_stage_after_bad_contact():
                            ok_pts = True
                            break
                    time.sleep(0.2)

            if self.do_verify and (not self._stop.is_set()):
                self._set_phase('VERIFY')
                self.stage_signal.emit(f'进入复核：调试模式输出J并到点采集 {int(self.hold_seconds)}s 平均值...')
                self._safe_debug_mode()
                if not self._streams:
                    self._start_iotdb_and_streams()

                default_ppms = [10000, 20000, 30000]
                for vi in range(1, 4):
                    if self._stop.is_set():
                        break
                    self.stage_signal.emit(f'浓度复核点{vi}/3：到达后点击“到达并记录”(将采集 {int(self.hold_seconds)}s 平均J)')
                    ppm_v = self._ask_ui('HOLD_VERIFY_POINT', {
                        'idx': vi,
                        'default_ppm': default_ppms[vi - 1],
                        'hold_s': int(self.hold_seconds),
                        'text': f'复核点{vi}：请稳定后点击“到达并记录”，随后输入当前ppm。'
                    })
                    if ppm_v is None:
                        self._stop.set()
                        break
                    try:
                        ppm_i = int(ppm_v)
                    except Exception:
                        ppm_i = default_ppms[vi - 1]
                    ppm_i = max(0, min(40000, ppm_i))

                    stats_map = self._collect_hold_stats_all('J', self.hold_seconds, f'复核点{vi} J')

                    for d in self.devices:
                        sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                        st = stats_map.get(sn_key) or {}
                        if not st.get('count'):
                            self.sensor_status_signal.emit(d.port, f'❌ 复核点{vi}无J数据')
                            self.report[sn_key]['final'] = 'FAIL_NO_J'
                            continue

                        j_avg = int(st.get('avg'))
                        err_percent = (float(j_avg) - float(ppm_i)) / 40000.0 * 100.0
                        ok = abs(err_percent) <= float(self.verify_tol_percent)

                        self.verify_points[sn_key].append({
                            'ppm': float(ppm_i),
                            'j': float(j_avg),
                            'j_min': int(st.get('min')) if st.get('min') is not None else None,
                            'j_max': int(st.get('max')) if st.get('max') is not None else None,
                            'j_count': int(st.get('count', 0)),
                            'hold_s': float(st.get('duration_s', self.hold_seconds)),
                            'err_percent': float(err_percent),
                            'ok': bool(ok),
                        })
                        self.report[sn_key]['verify_points'] = self.verify_points[sn_key]
                        self.sensor_status_signal.emit(d.port,
                                                       f'复核{vi}: err={err_percent:+.2f}% {"OK" if ok else "FAIL"}')
                        self._log_port(d,
                                       f'复核点{vi}：输入ppm={ppm_i}, J(avg)={j_avg}, min={st.get("min")}, max={st.get("max")}, n={st.get("count")} err={err_percent:+.2f}% ({"OK" if ok else "FAIL"})')
            else:
                if not self.do_verify:
                    self.stage_signal.emit('已跳过复核（主界面未勾选）')

            for d in self.devices:
                sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                info = self.report.get(sn_key, {}) or {}
                if info.get('final') not in (None, 'UNKNOWN'):
                    continue

                if (info.get('zero_n') is None) and (self.run_mode != 'VERIFY_ONLY'):
                    info['final'] = 'FAIL_ZERO_MISSING'
                    continue

                if (self.run_mode != 'VERIFY_ONLY') and (not info.get('conc_write_ok')):
                    info['final'] = 'FAIL_WRITE'
                    continue

                vps = info.get('verify_points') or []
                if not vps:
                    info['final'] = 'PASS_NO_VERIFY' if (self.run_mode != 'VERIFY_ONLY') else 'FAIL_NO_VERIFY'
                    continue

                if all(bool(p.get('ok')) for p in vps[:3]):
                    info['final'] = 'PASS'
                else:
                    info['final'] = 'FAIL_VERIFY'

            try:
                self.log_signal.emit("—— 浓度流程汇总（按端口） ——")
                for d in self.devices:
                    sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                    info = self.report.get(sn_key, {}) or {}
                    z = info.get("zero") or {}
                    z_txt = f"{info.get('zero_n')}(min={z.get('min')},max={z.get('max')},n={z.get('count')})" if info.get(
                        'zero_n') is not None else "无"
                    cps = info.get("cal_points") or []
                    cps_txt = ",".join([
                                           f"P{i + 1}({int(p.get('ppm', 0))},{int(p.get('m', 0))}|min={p.get('m_min')},max={p.get('m_max')},n={p.get('m_count')})"
                                           for i, p in enumerate(cps[:3])]) or "无"
                    w_ok = bool(info.get("conc_write_ok"))
                    vps = info.get("verify_points") or []
                    v_txt = ",".join(
                        [f"V{i + 1}{float(p.get('err_percent', 0.0)):+.2f}%({'OK' if p.get('ok') else 'FAIL'})" for i, p
                         in enumerate(vps[:3])]) or "无"
                    final = info.get("final", "UNKNOWN")
                    self.log_signal.emit(
                        f"[{d.port}] zeroN={z_txt}; cal={cps_txt}; write={'OK' if w_ok else 'FAIL'}; verify={v_txt}; final={final}")
            except Exception:
                pass

            self._set_phase('DONE')
            self.stage_signal.emit('完成')
            self.finished_signal.emit(self.report)

        except Exception as e:
            self.log_signal.emit(f'❌ 浓度标定线程异常: {e}')
            self.finished_signal.emit(self.report)
        finally:
            try:
                self._clear_countdown()
            except Exception:
                pass
            try:
                self._stop_streams()
            except Exception:
                pass
            try:
                if self._iotdb_writer:
                    self._iotdb_writer.stop()
                    self._iotdb_writer.wait(2000)
            except Exception:
                pass
            try:
                self._close_sensors()
            except Exception:
                pass


# -------------------- 浓度标定弹窗 --------------------

class ConcentrationWizardDialog(QDialog):
    def __init__(self, parent: QWidget, devices: List[SensorDevice], make_worker_fn, title: str = "浓度标定流程"):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(980, 660)

        self.devices = devices
        self.make_worker_fn = make_worker_fn
        self.worker: Optional[ConcentrationCalibrationWorker] = None
        self._last_token: Optional[str] = None
        self._last_kind: Optional[str] = None
        self._last_payload: dict = {}
        self.current_phase: str = "INIT"

        layout = QVBoxLayout()

        self.lbl_stage = QLabel("状态：准备")
        self.lbl_stage.setStyleSheet("font-size:16px;")
        layout.addWidget(self.lbl_stage)

        self.lbl_countdown = QLabel("")
        self.lbl_countdown.setStyleSheet("color: blue; font-weight: bold; font-size: 18px;")
        layout.addWidget(self.lbl_countdown)

        self.tbl = QTableWidget(0, 4)
        self.tbl.setHorizontalHeaderLabels(["端口", "SN", "当前步骤状态", "备注"])
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.tbl.setAlternatingRowColors(True)
        self.tbl.setWordWrap(True)
        try:
            self.tbl.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        except Exception:
            pass
        layout.addWidget(self.tbl)
        self._init_rows()

        bar = QHBoxLayout()
        self.btn_action = QPushButton("到达并记录")
        self.btn_action.setEnabled(False)
        self.btn_action.clicked.connect(self.on_action_click)

        self.btn_rescan = QPushButton("接触不良，重新扫描")
        self.btn_rescan.clicked.connect(self.on_rescan)

        self.btn_manual = QPushButton("手动补点/继续")
        self.btn_manual.clicked.connect(self.on_manual)

        self.btn_summary = QPushButton("查看汇总")
        self.btn_summary.setEnabled(False)
        self.btn_summary.clicked.connect(self.on_summary)

        self.btn_stop = QPushButton("停止")
        self.btn_stop.clicked.connect(self.on_stop)

        bar.addWidget(self.btn_action)
        bar.addWidget(self.btn_rescan)
        bar.addWidget(self.btn_manual)
        bar.addWidget(self.btn_summary)
        bar.addStretch(1)
        bar.addWidget(self.btn_stop)
        layout.addLayout(bar)

        self.log = QTextEdit()
        self.log.setReadOnly(True)
        self.log.setStyleSheet('background-color:#f0f0f0; font-family:Consolas;')
        layout.addWidget(QLabel("日志："))
        layout.addWidget(self.log)

        self.setLayout(layout)
        self._start_worker()

    def _init_rows(self):
        self.tbl.setRowCount(0)
        for d in [x for x in self.devices if x.selected]:
            r = self.tbl.rowCount()
            self.tbl.insertRow(r)
            self.tbl.setItem(r, 0, QTableWidgetItem(d.port))
            self.tbl.setItem(r, 1, QTableWidgetItem(sanitize_sn(d.current_sn or d.expected_sn or b'')))
            self.tbl.setItem(r, 2, QTableWidgetItem("—"))
            self.tbl.setItem(r, 3, QTableWidgetItem(""))

    def append_log(self, msg: str):
        self.log.append(msg)
        sb = self.log.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _append_row_remark(self, port: str, msg: str):
        try:
            for r in range(self.tbl.rowCount()):
                if (self.tbl.item(r, 0).text() or "") == port:
                    it = self.tbl.item(r, 3)
                    old = (it.text() or "") if it else ""
                    new = (old + "\n" + msg).strip() if old else msg
                    if it is None:
                        self.tbl.setItem(r, 3, QTableWidgetItem(new))
                    else:
                        it.setText(new)
                    try:
                        self.tbl.resizeRowsToContents()
                    except Exception:
                        pass
                    return
        except Exception:
            pass

    def _on_worker_log(self, msg: str):
        self.append_log(msg)
        try:
            mm = re.match(r'^\[(.+?)\]\s*(.*)$', (msg or '').strip())
            if mm:
                port = mm.group(1).strip()
                detail = mm.group(2).strip()
                if port and detail:
                    self._append_row_remark(port, detail)
        except Exception:
            pass

    def _set_row_status(self, port: str, status: str):
        for r in range(self.tbl.rowCount()):
            if (self.tbl.item(r, 0).text() or "") == port:
                self.tbl.item(r, 2).setText(status)
                return

    def _start_worker(self):
        self.worker = self.make_worker_fn()
        self.worker.log_signal.connect(self._on_worker_log)
        self.worker.stage_signal.connect(lambda s: self.lbl_stage.setText("状态：" + s))
        self.worker.sensor_status_signal.connect(self._set_row_status)
        self.worker.need_user_action.connect(self.on_need_user_action)
        self.worker.finished_signal.connect(self.on_finished)

        try:
            self.worker.phase_signal.connect(self.on_phase_changed)
        except Exception:
            pass
        try:
            self.worker.countdown_signal.connect(self.on_countdown)
        except Exception:
            pass
        try:
            self.worker.port_remap_signal.connect(self.on_port_remap)
        except Exception:
            pass

        self.worker.start()

    def on_port_remap(self, old_port: str, new_port: str):
        for r in range(self.tbl.rowCount()):
            if (self.tbl.item(r, 0).text() or "") == old_port:
                self.tbl.item(r, 0).setText(new_port)
        try:
            if isinstance(self.parent(), MainWindow):
                for d in self.parent().devices:
                    if d.port == old_port:
                        d.port = new_port
                self.parent()._rebuild_table()
        except Exception:
            pass
        self.append_log(f"🔄 端口重映射：[{old_port}] -> [{new_port}]，自动恢复连接。")

    def on_phase_changed(self, phase: str):
        self.current_phase = str(phase or "INIT")
        if self.current_phase.upper() == "VERIFY":
            self.btn_manual.setEnabled(False)
        else:
            self.btn_manual.setEnabled(True)

    def on_countdown(self, remain_s: int, label: str):
        try:
            remain_s = int(remain_s)
        except Exception:
            remain_s = 0
        label = (label or "").strip()
        if remain_s > 0 and label:
            self.lbl_countdown.setText(f"{label} 倒计时：{remain_s}s")
        else:
            self.lbl_countdown.setText("")

    def on_action_click(self):
        if not self.worker or not self._last_token or not self._last_kind:
            return

        kind = self._last_kind
        token = self._last_token

        if kind == 'HOLD_ZERO':
            self.worker.provide_input(token, True)
            self.btn_action.setEnabled(False)
            return

        if kind in ('HOLD_CAL_POINT', 'HOLD_VERIFY_POINT'):
            idx = int((self._last_payload or {}).get('idx', 1))
            default_ppm = int((self._last_payload or {}).get('default_ppm', idx * 10000))
            ppm, ok = QInputDialog.getInt(self, "输入ppm", f"请输入当前ppm（点{idx}）", default_ppm, 0, 40000, 1)
            if not ok:
                self.worker.provide_input(token, None)
                self.btn_action.setEnabled(False)
                return
            self.worker.provide_input(token, int(ppm))
            self.btn_action.setEnabled(False)
            return

    def on_need_user_action(self, kind: str, payload: dict):
        self._last_kind = kind
        self._last_payload = payload or {}
        self._last_token = (payload or {}).get('token')

        text = (payload or {}).get('text') or ''
        if kind in ('HOLD_ZERO', 'HOLD_CAL_POINT', 'HOLD_VERIFY_POINT'):
            self.btn_action.setText("到达并记录")
            self.btn_action.setEnabled(True)
            if text:
                self.append_log("👉 " + text)
            return

        self.btn_action.setEnabled(False)
        if text:
            self.append_log("👉 " + text)

    def on_rescan(self):
        if self.worker and self.worker.isRunning():
            self.worker.request_rescan()
            self.append_log("已请求重新扫描/重连（线程会在合适时机执行）。")

    def on_manual(self):
        if str(self.current_phase).upper() == "VERIFY":
            QMessageBox.information(self, "提示", "复核阶段无需手动补点。")
            return

        if not self.worker or not self.worker.isRunning():
            QMessageBox.information(self, "提示", "当前没有正在运行的任务。")
            return

        phase = str(self.current_phase or "INIT").upper()
        mode = "CAL_PARTIAL"
        min_required = 0
        if phase in ("ZERO", "WRITE"):
            mode = "FULL"
            min_required = 3
        elif phase == "CAL":
            mode = "CAL_PARTIAL"
            try:
                rep = getattr(self.worker, "report", None) or {}
                counts = []
                for d in [x for x in self.devices if x.selected]:
                    sn_key = sanitize_sn(d.expected_sn or d.current_sn or b'')
                    cps = (rep.get(sn_key) or {}).get("cal_points") or []
                    counts.append(len(cps))
                min_required = min(counts) if counts else 0
            except Exception:
                min_required = 0
        else:
            mode = "CAL_PARTIAL"

        dlg = ManualConcPointsDialog(self, self.devices, last_report=getattr(self.worker, 'report', None), mode=mode,
                                     min_points_required=min_required)
        if dlg.exec() != QDialog.DialogCode.Accepted:
            return
        manual_pts = dlg.get_points()
        if not manual_pts:
            QMessageBox.information(self, "提示", "没有获得手动点数据。")
            return

        try:
            self.worker.stop()
        except Exception:
            pass
        try:
            self.worker.wait(1500)
        except Exception:
            pass

        old_make = self.make_worker_fn
        force_reset_points = phase in ("ZERO", "WRITE")

        def make2():
            w = old_make()
            w.manual_points_by_sn = manual_pts
            try:
                w.force_factory_reset_before_points = bool(force_reset_points)
            except Exception:
                pass
            return w

        self.make_worker_fn = make2
        self.append_log("🔁 已载入手动补点数据，重新开始流程（会自动跳到对应阶段继续）。")
        self._start_worker()

    def on_summary(self):
        if not getattr(self, "_last_report", None):
            QMessageBox.information(self, "提示", "暂无汇总数据。")
            return
        dlg = ConcSummaryDialog(self, self._last_report)
        dlg.show()
        dlg.raise_()
        dlg.activateWindow()

    def on_finished(self, report: dict):
        self._last_report = report or {}
        self.btn_action.setEnabled(False)
        self.btn_stop.setEnabled(False)
        try:
            self.btn_summary.setEnabled(True)
        except Exception:
            pass
        self.lbl_countdown.setText("")
        self.append_log("✅ 流程结束。你可以关闭窗口，或回到主界面导出报告。")
        try:
            self.append_log("—— 结果汇总 ——")
            for sn_key, info in (self._last_report or {}).items():
                port = (info or {}).get("port", "")
                final = (info or {}).get("final", "UNKNOWN")
                vps = (info or {}).get("verify_points") or []
                parts = []
                for i, p in enumerate(vps[:3], start=1):
                    parts.append(
                        f"V{i}: err={float(p.get('err_percent', 0.0)):+.2f}% ({'OK' if p.get('ok') else 'FAIL'})")
                self.append_log(f"[{port}] final={final}; " + ("; ".join(parts) if parts else "无复核数据/未复核"))
        except Exception:
            pass

        try:
            if isinstance(self.parent(), MainWindow):
                self.parent()._last_report = report
                self.parent().btn_export.setEnabled(True)
        except Exception:
            pass

    def on_stop(self):
        if self.worker and self.worker.isRunning():
            self.worker.stop()
            self.append_log("已请求停止。")
        self.btn_action.setEnabled(False)

    def closeEvent(self, event):
        try:
            self.on_stop()
        except Exception:
            pass
        super().closeEvent(event)


class ConcSummaryDialog(QDialog):
    def __init__(self, parent: QWidget, report: dict):
        super().__init__(parent)
        self.setWindowTitle("浓度标定/复核结果汇总")
        self.resize(1220, 650)
        self.report = report or {}

        layout = QVBoxLayout()

        tbl = QTableWidget(0, 9)
        tbl.setHorizontalHeaderLabels([
            "端口", "SN(ASCII)", "零点N(avg|min|max)",
            "标定点(PPM,Mavg|min|max)", "写入&回读", "回读点",
            "复核点误差(%)", "判定", "备注"
        ])
        tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        tbl.setWordWrap(True)
        try:
            tbl.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        except Exception:
            pass

        for sn_key, info in (self.report or {}).items():
            info = info or {}
            r = tbl.rowCount()
            tbl.insertRow(r)

            port = str(info.get("port", ""))
            sn_dual = str(info.get("sn_hex_ascii", ""))
            sn_ascii = ""
            try:
                if "ASCII=" in sn_dual:
                    sn_ascii = sn_dual.split("ASCII=", 1)[1].strip()
            except Exception:
                sn_ascii = sn_dual

            zero_n = info.get("zero_n", None)
            z = info.get("zero") or {}
            if zero_n is None:
                z_txt = ""
            else:
                z_txt = f"{int(zero_n)} | {z.get('min')}~{z.get('max')} (n={z.get('count')})"

            cal_pts = info.get("cal_points") or []
            cal_lines = []
            for i, p in enumerate(cal_pts[:3], start=1):
                try:
                    cal_lines.append(
                        f"P{i}: {int(p.get('ppm', 0))}, {int(p.get('m', 0))} | {p.get('m_min')}~{p.get('m_max')} (n={p.get('m_count')})")
                except Exception:
                    pass
            cal_txt = "\n".join(cal_lines)

            write_ok = bool(info.get("conc_write_ok"))
            rb = (info.get("conc_readback") or {}).get("points") or []
            rb_lines = []
            for i, p in enumerate(rb[:3], start=1):
                try:
                    rb_lines.append(f"R{i}: {int(p.get('ppm', 0))}, {int(p.get('m', 0))}")
                except Exception:
                    pass
            rb_txt = "\n".join(rb_lines)

            vps = info.get("verify_points") or []
            v_lines = []
            for i, p in enumerate(vps[:3], start=1):
                try:
                    v_lines.append(
                        f"V{i}: {float(p.get('err_percent', 0.0)):+.2f}% (J={int(p.get('j', 0))}, {p.get('j_min')}~{p.get('j_max')}, n={p.get('j_count')})")
                except Exception:
                    pass
            v_txt = "\n".join(v_lines)

            final = str(info.get("final", "UNKNOWN"))
            note = ""
            if (not write_ok) and rb:
                note = "回读与写入不一致/写入失败"
            if (not vps) and final.startswith("PASS"):
                note = (note + "; " if note else "") + "未进行复核"

            tbl.setItem(r, 0, QTableWidgetItem(port))
            tbl.setItem(r, 1, QTableWidgetItem(sn_ascii))
            tbl.setItem(r, 2, QTableWidgetItem(z_txt))
            tbl.setItem(r, 3, QTableWidgetItem(cal_txt))
            tbl.setItem(r, 4, QTableWidgetItem("OK" if write_ok else "FAIL"))
            tbl.setItem(r, 5, QTableWidgetItem(rb_txt))
            tbl.setItem(r, 6, QTableWidgetItem(v_txt))
            tbl.setItem(r, 7, QTableWidgetItem(final))
            tbl.setItem(r, 8, QTableWidgetItem(note))

        layout.addWidget(tbl)

        btn_close = QPushButton("关闭")
        btn_close.clicked.connect(self.close)
        layout.addWidget(btn_close, alignment=Qt.AlignmentFlag.AlignRight)

        self.setLayout(layout)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle('浓度标定软件（UI版 v2）')
        self.resize(1120, 780)

        self.devices: List[SensorDevice] = []
        self.baudrate = 115200

        self.scan_worker: Optional[PortScanWorker] = None
        self.sn_worker: Optional[WriteSNWorker] = None
        self.reset_worker: Optional[ConcFactoryResetWorker] = None

        self._last_report: Optional[dict] = None
        self.monitor_dialog: Optional[RealtimeMonitorDialog] = None
        self.wizard_dialog = None

        self._build_ui()

    def log(self, msg: str):
        self.log_text.append(msg)
        sb = self.log_text.verticalScrollBar()
        sb.setValue(sb.maximum())

    def _sync_baud(self):
        try:
            self.baudrate = int(self.ed_baud.text().strip())
        except Exception:
            self.baudrate = 115200
            self.ed_baud.setText(str(self.baudrate))

    def _build_ui(self):
        root = QWidget()
        self.setCentralWidget(root)
        layout = QVBoxLayout()

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
        tabs.addTab(self._build_tab_conc(), '浓度标定')
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

        self.tbl = QTableWidget(0, 6)
        self.tbl.setHorizontalHeaderLabels(['选中', '端口', '当前SN(ASCII)', '当前SN(HEX)', '新SN(可编辑)', '状态'])
        self.tbl.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.tbl.setAlternatingRowColors(True)
        self.tbl.setWordWrap(True)
        try:
            self.tbl.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        except Exception:
            pass
        v.addWidget(self.tbl)

        gen = QGroupBox('批量生成/写入序列号（12字节ASCII，超长截断，不足\\x00补齐）')
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

    def _build_tab_conc(self) -> QWidget:
        w = QWidget()
        v = QVBoxLayout()

        gb_i = QGroupBox('IoTDB（需要保证IoTDB服务已启动；软件负责连接和入库，可不勾选）')
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

        gb_p = QGroupBox('浓度标定参数')
        pl = QGridLayout()
        self.ed_max_ppm = QLineEdit('40000')
        self.ed_min_ppm = QLineEdit('1000')
        self.ed_verify_tol = QLineEdit('5.0')
        self.ed_verify_tol.setValidator(QDoubleValidator(0.0, 100.0, 2, self))
        self.ed_hold_s = QLineEdit('60')
        self.ed_hold_s.setValidator(QIntValidator(1, 3600, self))
        self.cb_do_verify = QCheckBox('标定后执行三点复核(J误差判断)')
        self.cb_do_verify.setChecked(True)
        self.cb_factory_reset = QCheckBox('开始写入前先恢复浓度出厂(0x34)')
        self.cb_factory_reset.setChecked(True)

        self.ed_max_ppm.setValidator(QIntValidator(0, 65535, self))
        self.ed_min_ppm.setValidator(QIntValidator(0, 65535, self))

        pl.addWidget(QLabel('最大浓度(ppm)'), 0, 0)
        pl.addWidget(self.ed_max_ppm, 0, 1)
        pl.addWidget(QLabel('最小浓度(ppm)'), 0, 2)
        pl.addWidget(self.ed_min_ppm, 0, 3)
        pl.addWidget(QLabel('复核误差阈值(±%, 默认5%)'), 0, 4)
        pl.addWidget(self.ed_verify_tol, 0, 5)
        pl.addWidget(QLabel('平均采集时长(s)'), 1, 0)
        pl.addWidget(self.ed_hold_s, 1, 1)
        pl.addWidget(self.cb_do_verify, 1, 2, 1, 4)
        pl.addWidget(self.cb_factory_reset, 2, 0, 1, 3)
        gb_p.setLayout(pl)
        v.addWidget(gb_p)

        btns = QHBoxLayout()
        self.btn_start = QPushButton('开始浓度标定（弹窗）')
        self.btn_verify_only = QPushButton('仅三点复核（弹窗）')
        self.btn_reset = QPushButton('恢复浓度出厂(0x34)')
        self.btn_monitor = QPushButton('打开实时监控')
        self.btn_export = QPushButton('导出报告(CSV)')
        self.btn_export.setEnabled(False)

        self.btn_start.clicked.connect(self.on_start_conc)
        self.btn_verify_only.clicked.connect(self.on_start_verify_only)
        self.btn_reset.clicked.connect(self.on_reset_conc)
        self.btn_monitor.clicked.connect(self.on_open_monitor)
        self.btn_export.clicked.connect(self.on_export)

        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_verify_only)
        btns.addWidget(self.btn_reset)
        btns.addWidget(self.btn_monitor)
        btns.addWidget(self.btn_export)
        btns.addStretch(1)
        v.addLayout(btns)

        self.lbl_stage = QLabel('状态：待机')
        self.lbl_stage.setStyleSheet('font-size:16px;')
        v.addWidget(self.lbl_stage)

        w.setLayout(v)
        return w

    def _rebuild_table(self):
        self.tbl.setRowCount(0)
        for d in self.devices:
            self._add_row(d)
        self.lbl_found.setText(f'已识别: {len(self.devices)}')

    def _add_row(self, d: SensorDevice):
        r = self.tbl.rowCount()
        self.tbl.insertRow(r)
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
            dev = SensorDevice(port=port, sensor=UniversalSensor(port, self.baudrate, timeout=0.6), current_sn=sn,
                               expected_sn=sn)
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
        self.log('已按规则填充新SN。')

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
        self._rebuild_table()
        self.log('SN写入流程完成。')

    def on_reset_conc(self):
        self._sync_baud()
        self._collect_table_to_devices()
        if self.reset_worker and self.reset_worker.isRunning():
            return
        targets = [d for d in self.devices if d.selected]
        if not targets:
            QMessageBox.information(self, '提示', '没有选中的传感器')
            return
        ret = QMessageBox.question(self, '恢复出厂确认', f'将对选中 {len(targets)} 个传感器执行：浓度恢复出厂(01 34 00)\n\n是否继续？')
        if ret != QMessageBox.StandardButton.Yes:
            return
        self.btn_reset.setEnabled(False)
        self.reset_worker = ConcFactoryResetWorker(self.devices, baudrate=self.baudrate)
        self.reset_worker.log_signal.connect(self.log)
        self.reset_worker.progress_signal.connect(self._set_row_status)
        self.reset_worker.finished_signal.connect(lambda: self.btn_reset.setEnabled(True))
        self.reset_worker.start()

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

    def on_start_conc(self):
        self._sync_baud()
        self._collect_table_to_devices()
        targets = [d for d in self.devices if d.selected]
        if not targets:
            QMessageBox.information(self, '提示', '没有选中的传感器')
            return
        if any((d.expected_sn is None) for d in targets):
            self.log('⚠️ 有传感器SN为空：建议先“扫描端口”确保SN已读出。')

        try:
            max_ppm = int(float(self.ed_max_ppm.text()))
            min_ppm = int(float(self.ed_min_ppm.text()))
            tol = float(self.ed_verify_tol.text())
            do_fr = self.cb_factory_reset.isChecked()
            hold_s = int(float(self.ed_hold_s.text()))
            do_verify = self.cb_do_verify.isChecked()
            i_en = self.cb_iotdb.isChecked()
            i_host = self.ed_i_host.text().strip()
            i_port = int(float(self.ed_i_port.text()))
            i_user = self.ed_i_user.text().strip()
            i_pass = self.ed_i_pass.text().strip()
            i_sg = self.ed_i_sg.text().strip()
            i_down = int(float(self.ed_i_down.text()))
        except Exception:
            QMessageBox.warning(self, '错误', '参数格式不正确')
            return

        self._last_run_meta = {
            'mode': 'concentration_calibration',
            'max_ppm': max_ppm,
            'min_ppm': min_ppm,
            'verify_tol_percent': tol,
            'hold_seconds': int(hold_s),
            'do_verify': bool(do_verify),
            'run_mode': 'CALIBRATE',
            'do_factory_reset_at_start': bool(do_fr),
            'iotdb_enable': bool(i_en),
            'iotdb_host': i_host,
            'iotdb_port': int(i_port),
            'iotdb_sg': i_sg,
            'iotdb_downsample_ms': int(i_down),
            'started_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        }

        def make_worker():
            w = ConcentrationCalibrationWorker(
                devices=self.devices,
                baudrate=self.baudrate,
                iotdb_enable=i_en,
                iotdb_host=i_host,
                iotdb_port=i_port,
                iotdb_user=i_user,
                iotdb_pass=i_pass,
                iotdb_sg=i_sg,
                iotdb_downsample_ms=i_down,
                max_ppm=max_ppm,
                min_ppm=min_ppm,
                verify_tol_percent=tol,
                do_factory_reset_at_start=do_fr,
                hold_seconds=hold_s,
                do_verify=do_verify,
                run_mode='CALIBRATE',
            )
            try:
                if self.monitor_dialog is None:
                    self.monitor_dialog = RealtimeMonitorDialog(self, self.devices, i_sg)
                else:
                    self.monitor_dialog.sg = i_sg
                    self.monitor_dialog.devices = [d for d in self.devices if d.selected]
                    self.monitor_dialog._init_rows()
                self.monitor_dialog.show()
                self.monitor_dialog.raise_()
                w.sample_signal.connect(self.monitor_dialog.on_sample)
                w.iotdb_status_signal.connect(self.monitor_dialog.on_iotdb_status)
                w.stage_signal.connect(self.monitor_dialog.set_stage)
                w.log_signal.connect(self.monitor_dialog.append_log)
                # 重要：也把重映射信号连接到监控弹窗
                w.port_remap_signal.connect(self.monitor_dialog.on_port_remap)
            except Exception:
                pass

            w.stage_signal.connect(lambda s: self.lbl_stage.setText('状态：' + s))
            w.log_signal.connect(self.log)
            w.sensor_status_signal.connect(self._set_row_status)
            w.finished_signal.connect(self.on_conc_finished)
            return w

        try:
            if self.wizard_dialog is not None and self.wizard_dialog.isVisible():
                self.wizard_dialog.raise_()
                self.wizard_dialog.activateWindow()
                return
        except Exception:
            pass

        dlg = ConcentrationWizardDialog(self, self.devices, make_worker, title='浓度标定流程')
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        self.wizard_dialog = dlg
        try:
            self.btn_start.setEnabled(False)
            self.btn_verify_only.setEnabled(False)
        except Exception:
            pass

        def _clear_wiz(*_args):
            try:
                self.wizard_dialog = None
                self.btn_start.setEnabled(True)
                self.btn_verify_only.setEnabled(True)
            except Exception:
                pass

        dlg.destroyed.connect(_clear_wiz)
        dlg.show()
        dlg.raise_()
        dlg.activateWindow()

    def on_start_verify_only(self):
        self._sync_baud()
        self._collect_table_to_devices()
        targets = [d for d in self.devices if d.selected]
        if not targets:
            QMessageBox.information(self, '提示', '没有选中的传感器')
            return

        try:
            tol = float(self.ed_verify_tol.text())
            hold_s = int(float(self.ed_hold_s.text()))
            i_en = self.cb_iotdb.isChecked()
            i_host = self.ed_i_host.text().strip()
            i_port = int(float(self.ed_i_port.text()))
            i_user = self.ed_i_user.text().strip()
            i_pass = self.ed_i_pass.text().strip()
            i_sg = self.ed_i_sg.text().strip()
            i_down = int(float(self.ed_i_down.text()))
        except Exception:
            QMessageBox.warning(self, '错误', '参数格式不正确')
            return

        self._last_run_meta = {
            'mode': 'concentration_verify_only',
            'verify_tol_percent': tol,
            'hold_seconds': int(hold_s),
            'run_mode': 'VERIFY_ONLY',
            'iotdb_enable': bool(i_en),
            'iotdb_host': i_host,
            'iotdb_port': int(i_port),
            'iotdb_sg': i_sg,
            'iotdb_downsample_ms': int(i_down),
            'started_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        }

        def make_worker():
            w = ConcentrationCalibrationWorker(
                devices=self.devices,
                baudrate=self.baudrate,
                iotdb_enable=i_en,
                iotdb_host=i_host,
                iotdb_port=i_port,
                iotdb_user=i_user,
                iotdb_pass=i_pass,
                iotdb_sg=i_sg,
                iotdb_downsample_ms=i_down,
                max_ppm=int(float(self.ed_max_ppm.text() or 40000)),
                min_ppm=int(float(self.ed_min_ppm.text() or 1000)),
                verify_tol_percent=tol,
                do_factory_reset_at_start=False,
                hold_seconds=hold_s,
                do_verify=True,
                run_mode='VERIFY_ONLY',
            )
            try:
                if self.monitor_dialog is None:
                    self.monitor_dialog = RealtimeMonitorDialog(self, self.devices, i_sg)
                else:
                    self.monitor_dialog.sg = i_sg
                    self.monitor_dialog.devices = [d for d in self.devices if d.selected]
                    self.monitor_dialog._init_rows()
                self.monitor_dialog.show()
                self.monitor_dialog.raise_()
                w.sample_signal.connect(self.monitor_dialog.on_sample)
                w.iotdb_status_signal.connect(self.monitor_dialog.on_iotdb_status)
                w.stage_signal.connect(self.monitor_dialog.set_stage)
                w.log_signal.connect(self.monitor_dialog.append_log)
                w.port_remap_signal.connect(self.monitor_dialog.on_port_remap)
            except Exception:
                pass

            w.stage_signal.connect(lambda s: self.lbl_stage.setText('状态：' + s))
            w.log_signal.connect(self.log)
            w.sensor_status_signal.connect(self._set_row_status)
            w.finished_signal.connect(self.on_conc_finished)
            return w

        try:
            if self.wizard_dialog is not None and self.wizard_dialog.isVisible():
                self.wizard_dialog.raise_()
                self.wizard_dialog.activateWindow()
                return
        except Exception:
            pass

        dlg = ConcentrationWizardDialog(self, self.devices, make_worker, title='浓度复核流程（仅复核）')
        dlg.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        self.wizard_dialog = dlg
        try:
            self.btn_start.setEnabled(False)
            self.btn_verify_only.setEnabled(False)
        except Exception:
            pass

        def _clear_wiz(*_args):
            try:
                self.wizard_dialog = None
                self.btn_start.setEnabled(True)
                self.btn_verify_only.setEnabled(True)
            except Exception:
                pass

        dlg.destroyed.connect(_clear_wiz)
        dlg.show()
        dlg.raise_()
        dlg.activateWindow()

    def on_conc_finished(self, report: dict):
        self._last_report = report
        self.btn_export.setEnabled(True)
        self.log('流程结束（或已停止）。')
        try:
            self._auto_save_report(report)
        except Exception as e:
            self.log(f'⚠️ 报告保存失败: {e}')

    def _build_report_payload(self, report: dict) -> dict:
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
        import os
        import json
        payload = self._build_report_payload(report)
        mode = (payload.get('run_meta') or {}).get('mode') or 'concentration'
        ts = time.strftime('%Y%m%d_%H%M%S')
        base_dir = os.path.dirname(os.path.abspath(__file__))
        candidates = [
            os.path.join(base_dir, 'calibration_reports'),
            os.path.join(os.getcwd(), 'calibration_reports'),
            os.path.join(os.path.expanduser('~'), 'calibration_reports'),
        ]
        out_path = None
        last_err = None
        for d in candidates:
            try:
                os.makedirs(d, exist_ok=True)
                out_path = os.path.join(d, f'{mode}_report_{ts}.json')
                with open(out_path, 'w', encoding='utf-8') as f:
                    json.dump(payload, f, ensure_ascii=False, indent=2)
                break
            except Exception as e:
                last_err = e
                out_path = None
        if not out_path:
            raise RuntimeError(f'无法写入报告文件：{last_err}')
        self.log('✅ 已自动生成JSON报告：' + out_path)

    def on_export(self):
        if not getattr(self, '_last_report', None):
            QMessageBox.information(self, '提示', '没有可导出的报告')
            return
        path, _ = QFileDialog.getSaveFileName(self, '保存报告', 'concentration_report.csv', 'CSV Files (*.csv)')
        if not path:
            return
        try:
            import csv
            with open(path, 'w', newline='', encoding='utf-8-sig') as f:
                w = csv.writer(f)
                w.writerow([
                    'SN', 'Port', 'Final',
                    'ZeroN', 'MaxPPM', 'MinPPM',
                    'CalPoints(ppm:m)', 'WriteOK', 'Readback',
                    'VerifyPoints(ppm:j:err%)'
                ])
                for sn, info in (self._last_report or {}).items():
                    if not isinstance(info, dict):
                        continue
                    cal_pts = info.get('cal_points') or []
                    cal_str = '; '.join(
                        [f"{p.get('ppm')}:{p.get('m')}|{p.get('m_min')}~{p.get('m_max')}" for p in cal_pts if
                         isinstance(p, dict)])
                    vps = info.get('verify_points') or []
                    ver_str = '; '.join([
                                            f"{p.get('ppm')}:{p.get('j'):.0f}|{p.get('j_min')}~{p.get('j_max')}:{p.get('err_percent'):+.2f}%"
                                            for p in vps if isinstance(p, dict) and p.get('j') is not None])
                    w.writerow([
                        sn,
                        info.get('port'),
                        info.get('final'),
                        info.get('zero_n'),
                        (info.get('range') or {}).get('max_ppm'),
                        (info.get('range') or {}).get('min_ppm'),
                        cal_str,
                        info.get('conc_write_ok'),
                        info.get('conc_readback'),
                        ver_str
                    ])
            self.log('✅ 报告已导出 CSV：' + path)
        except Exception as e:
            QMessageBox.warning(self, '错误', f'导出失败：{e}')


def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()