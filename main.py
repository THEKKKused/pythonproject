import serial
import time
import re
import threading
import crcmod.predefined
from serial.tools import list_ports  # 自动扫描可用串口

# ======== IoTDB 配置（如使用 5 功能请确认服务端已启动） ========
IOTDB_HOST = "127.0.0.1"
IOTDB_PORT = 6667
IOTDB_USER = "root"
IOTDB_PASS = "root"
IOTDB_SG   = "root.h2"
IOTDB_AUTO_CREATE_SCHEMA = True  # 服务器未开启自动建模时可改为 False

# IoTDB Python 客户端（pip install apache-iotdb==1.2.0）
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
    pass


def sanitize_sn(sn_bytes: bytes) -> str:
    """
    将12字节SN转为可用IoTDB设备名节点：
    - 去不可见字符/0x00
    - 非 [A-Za-z0-9_] 替换为 '_'
    - 若首字符不是字母/下划线，加前缀 'd_'
    """
    s = ''.join(chr(b) if 32 <= b <= 126 else '' for b in sn_bytes).replace('\x00', '').strip()
    s = re.sub(r'[^A-Za-z0-9_]', '_', s)
    if not s or not re.match(r'^[A-Za-z_]', s):
        s = 'd_' + s
    return s or 'd_unknown'


def fmt_sn_dual(sn_bytes: bytes) -> str:
    """把 SN 同时以 HEX 和 ASCII 显示"""
    hex_part = ' '.join(f'{b:02X}' for b in sn_bytes)
    ascii_part = ''.join(chr(b) if 32 <= b <= 126 else '.' for b in sn_bytes)
    return f"{hex_part}    (ASCII: {ascii_part})"


def parse_line_values(line: bytes) -> dict:
    """
    解析一行调试输出中的 A~N 数值（ASCII），返回 {'A': float, ...}
    典型：A=  14831B=  14832...N=     13;
    """
    text = line.decode('ascii', errors='ignore')
    pairs = re.findall(r'([A-N])=\s*([+-]?\d+(?:\.\d+)?)', text)
    vals = {}
    for key, val in pairs:
        try:
            vals[key] = float(val)
        except ValueError:
            pass
    return vals


class IoTDBWriter:
    """
    Session 一般不保证线程安全，所以每个端口/线程用自有实例更稳。
    """
    def __init__(self, host, port, user, password, storage_group):
        if Session is None:
            raise RuntimeError("未找到 IoTDB Python 客户端。请先 pip install apache-iotdb==1.2.0")
        self.session = Session(host, port, user, password)
        try:
            try:
                self.session.open(False)
            except TypeError:
                self.session.open()
        except Exception as e:
            raise RuntimeError(f"连接 IoTDB 失败: {e}")
        try:
            self.session.set_storage_group(storage_group)
        except Exception:
            pass
        self.sg = storage_group

    def ensure_timeseries(self, device_id: str, measurements):
        if IOTDB_AUTO_CREATE_SCHEMA:
            return
        for m in measurements:
            path = f"{device_id}.{m}"
            try:
                self.session.create_timeseries(
                    path, TSDataType.DOUBLE, TSEncoding.GORILLA, Compressor.SNAPPY
                )
            except Exception:
                pass

    def insert_record(self, device_id: str, values_map: dict, ts_ms: int = None):
        if not values_map:
            return
        measurements = sorted(values_map.keys())
        values = [float(values_map[m]) for m in measurements]
        data_types = [TSDataType.DOUBLE] * len(measurements)
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)
        # 重要：timestamp 在第二位
        self.session.insert_record(device_id, ts_ms, measurements, data_types, values)

    def close(self):
        try:
            self.session.close()
        except Exception:
            pass


class UniversalSensorTester:
    """
    单个端口的传感器对象。
    """
    def __init__(self, port, baudrate, timeout=1.0):
        self.port = port
        self.baudrate = baudrate
        self.timeout = timeout
        self.ser = None
        self.is_connected = False

    def connect(self):
        try:
            self.ser = serial.Serial(self.port, self.baudrate, timeout=self.timeout)
            self.is_connected = True
            print(f"[{self.port}] 已连接 (baud={self.baudrate})")
            return True
        except serial.SerialException as e:
            print(f"[{self.port}] 串口打开失败: {e}")
            return False

    def disconnect(self):
        if self.ser and self.ser.is_open:
            self.ser.close()
        self.is_connected = False
        print(f"[{self.port}] 已断开")

    @staticmethod
    def modbus_crc(data: bytes) -> bytes:
        crc16 = crcmod.predefined.mkPredefinedCrcFun('modbus')
        return crc16(data).to_bytes(2, 'little')

    def send_command(self, command_body_hex_list, wait_s: float = 1.0):
        if not self.is_connected:
            print(f"[{self.port}] 未连接")
            return None
        body = bytes(command_body_hex_list)
        full = body + self.modbus_crc(body)
        try:
            self.ser.reset_input_buffer()
            self.ser.write(full)
            print(f"[{self.port}] >> {' '.join(f'{b:02X}' for b in full)}")
            deadline = time.time() + wait_s
            buf = bytearray()
            while time.time() < deadline:
                n = self.ser.in_waiting
                if n:
                    buf += self.ser.read(n)
                else:
                    time.sleep(0.05)
                if len(buf) >= 5:  # 常见应答长度至少 5（含CRC）
                    break
            if buf:
                print(f"[{self.port}] << {' '.join(f'{b:02X}' for b in buf)}")
                return bytes(buf)
            else:
                print(f"[{self.port}] << 超时无回包")
                return None
        except serial.SerialException as e:
            print(f"[{self.port}] 读写错误: {e}")
            return None

    # === SN 读写 ===
    def read_sn(self):
        resp = self.send_command([0x01, 0x03, 0x00, 0x07, 0x00, 0x06])
        if not resp or len(resp) < 3 + 12:
            return None
        if resp[0] != 0x01 or resp[1] != 0x03 or resp[2] != 0x0C:
            return None
        return resp[3:3+12]

    def write_sn(self, sn_bytes: bytes):
        if len(sn_bytes) != 12:
            raise ValueError("SN 必须 12 字节")
        body = [0x01, 0x10, 0x00, 0x07, 0x00, 0x06, 0x0C] + list(sn_bytes)
        _ = self.send_command(body, wait_s=1.0)
        time.sleep(0.3)
        verify = self.read_sn()
        if verify is None:
            print(f"[{self.port}] 写入后读取失败")
            return False
        ok = (verify == sn_bytes)
        print(f"[{self.port}] 写入后 SN：{fmt_sn_dual(verify)}")
        print(f"[{self.port}] {'✅ 成功' if ok else '❌ 失败'}")
        return ok

    # === 调试采集 + IoTDB 入库（按行 CRLF 重组，边打印HEX边入库） ===
    def collect_and_write_iotdb(self, duration_s: float = 10.0):
        if Session is None:
            print(f"[{self.port}] 未安装 apache-iotdb 客户端")
            return
        # 进入用户模式->读SN
        self.send_command([0x01, 0x1E, 0x00])
        sn_bytes = self.read_sn()
        if not sn_bytes:
            print(f"[{self.port}] 读取 SN 失败，放弃入库")
            return
        sn_str = sanitize_sn(sn_bytes)
        device_id = f"{IOTDB_SG}.{sn_str}"
        print(f"[{self.port}] IoTDB 设备路径: {device_id}")
        print(f"[{self.port}] 当前SN：{fmt_sn_dual(sn_bytes)}")

        # 进入调试模式
        self.send_command([0x01, 0x20, 0x00])

        writer = IoTDBWriter(IOTDB_HOST, IOTDB_PORT, IOTDB_USER, IOTDB_PASS, IOTDB_SG)
        writer.ensure_timeseries(device_id, list("ABCDEFGHIJKLMN"))

        print(f"[{self.port}] 开始采集并入库 {duration_s:.1f}s")
        buf = bytearray()
        inserted = 0
        start = time.time()
        try:
            while time.time() - start < duration_s:
                n = self.ser.in_waiting
                chunk = self.ser.read(n or 1)
                if not chunk:
                    continue
                buf.extend(chunk)
                while True:
                    idx = buf.find(b'\r\n')
                    if idx == -1:
                        break
                    line = bytes(buf[:idx])
                    buf = buf[idx+2:]
                    print(f"[{self.port}] <<< FRAME HEX:", ' '.join(f"{b:02X}" for b in line), "0D 0A")
                    vals = parse_line_values(line)
                    if not vals:
                        continue
                    ts_ms = int(time.time() * 1000)
                    try:
                        writer.insert_record(device_id, vals, ts_ms)
                        inserted += 1
                    except Exception as e:
                        print(f"[{self.port}] 入库失败：{e}")
        except serial.SerialException as e:
            print(f"[{self.port}] 采集错误: {e}")
        finally:
            writer.close()
        print(f"[{self.port}] 入库完成：{inserted} 行\n")


# ========= 多设备管理 =========
class MultiSensorManager:
    def __init__(self, baudrate=115200):
        self.baudrate = baudrate
        self.testers = {}  # port -> UniversalSensorTester

    def add_ports(self, ports):
        added = []
        for p in ports:
            p = p.strip()
            if not p:
                continue
            if p in self.testers:
                print(f"[{p}] 已存在")
                continue
            t = UniversalSensorTester(p, self.baudrate, timeout=1.0)
            if t.connect():
                self.testers[p] = t
                added.append(p)
        if not added:
            print("未添加任何端口。")
        else:
            print("已添加端口：", ", ".join(added))

    def list_ports(self):
        if not self.testers:
            print("当前无已连接的端口。")
        else:
            print("当前端口：", ", ".join(self.testers.keys()))

    def disconnect_all(self):
        for p, t in list(self.testers.items()):
            try:
                t.disconnect()
            except Exception:
                pass
        self.testers.clear()

    def choose_targets(self):
        """
        选择目标端口：
        - 输入 ALL 表示全部
        - 输入单个端口如 COM4
        - 输入多个以逗号分隔
        """
        if not self.testers:
            print("没有端口，请先添加或自动扫描。")
            return []
        self.list_ports()
        s = input("选择目标端口（ALL / 单个端口 / 多个逗号分隔）: ").strip()
        if not s:
            return []
        if s.upper() == "ALL":
            return list(self.testers.keys())
        ports = [x.strip() for x in s.split(",")]
        return [p for p in ports if p in self.testers]

    # === 自动扫描端口（按文档：回复 01 1E 00 CRC 即认定为传感器） ===
    def auto_scan(self):
        candidates = [p.device for p in list_ports.comports()]
        if not candidates:
            print("未发现任何串口。")
            return

        print("发现串口：", ", ".join(candidates))
        found = []
        for dev in candidates:
            if dev in self.testers:
                continue
            t = UniversalSensorTester(dev, self.baudrate, timeout=0.5)
            if not t.connect():
                continue
            resp = t.send_command([0x01, 0x1E, 0x00], wait_s=1.0)
            ok = False
            if resp and len(resp) >= 5 and resp[0] == 0x01 and resp[1] == 0x1E and resp[2] == 0x00:
                ok = True
            if ok:
                self.testers[dev] = t
                found.append(dev)
                print(f"[{dev}] ✅ 识别为传感器端口")
            else:
                print(f"[{dev}] 非传感器或无有效应答，跳过")
                t.disconnect()

        if found:
            print("自动扫描完成，已加入端口：", ", ".join(found))
        else:
            print("自动扫描完成，未识别到传感器端口。")

    # === 执行动作：可并发 ===
    def run_on(self, ports, func, *args, concurrent=True):
        if not ports:
            print("未选择目标端口。")
            return
        if concurrent and len(ports) > 1:
            threads = []
            for p in ports:
                t = threading.Thread(target=self._safe_call, args=(p, func, args), daemon=True)
                threads.append(t)
                t.start()
            for th in threads:
                th.join()
        else:
            for p in ports:
                self._safe_call(p, func, args)

    def _safe_call(self, port, func, args):
        tester = self.testers.get(port)
        if not tester:
            print(f"[{port}] 不存在")
            return
        try:
            func(tester, *args)
        except Exception as e:
            print(f"[{port}] 执行出错：{e}")


# ========= 交互菜单 =========
def print_menu():
    print("\n=== 多传感器调试工具 ===")
    print("0. 自动扫描端口")
    print("1. 手动添加端口（示例：COM4,COM10）")
    print("2. 列出当前端口")
    print("3. 进入调试模式")
    print("4. 进入用户模式")
    print("5. 调试采集并写入 IoTDB（可设定秒数，默认10s）")
    print("6. 输入自定义指令(HEX)")
    print("7. 序列号烧写(读→写→读校验)")
    print("8. 退出")
    print("================================")


def get_sn_input():
    mode = input("输入 SN 的方式：[a] ASCII（默认） / [h] HEX: ").strip().lower()
    if mode == 'h':
        hx = input("请输入 12 个字节的十六进制（空格分隔）: ").strip()
        vals = [int(x, 16) for x in hx.split()]
        if len(vals) != 12:
            raise ValueError("HEX 输入需要正好 12 个字节。")
        return bytes(vals)
    else:
        s = input("请输入 SN（ASCII，最多 12 字符；不足以\\x00 右补）: ")
        b = s.encode('ascii', errors='ignore')[:12]
        if len(b) < 12:
            b = b + b'\x00' * (12 - len(b))
        return b


def main():
    mgr = MultiSensorManager(baudrate=115200)
    try:
        while True:
            print_menu()
            choice = input("选择(0-8): ").strip()
            if choice == '8':
                break
            elif choice == '0':
                mgr.auto_scan()
            elif choice == '1':
                s = input("输入端口（示例：COM4,COM10）: ")
                ports = [x.strip() for x in s.split(",") if x.strip()]
                mgr.add_ports(ports)
            elif choice == '2':
                mgr.list_ports()
            elif choice == '3':
                targets = mgr.choose_targets()
                mgr.run_on(targets, UniversalSensorTester.send_command, [0x01, 0x20, 0x00])
            elif choice == '4':
                targets = mgr.choose_targets()
                mgr.run_on(targets, UniversalSensorTester.send_command, [0x01, 0x1E, 0x00])
            elif choice == '5':
                targets = mgr.choose_targets()
                dur_str = input("输入采集时长（秒，默认10）: ").strip()
                try:
                    duration = float(dur_str) if dur_str else 10.0
                    if duration <= 0:
                        duration = 10.0
                except Exception:
                    duration = 10.0
                mgr.run_on(targets, UniversalSensorTester.collect_and_write_iotdb, duration)
            elif choice == '6':
                targets = mgr.choose_targets()
                hex_str = input("输入指令主体(HEX，空格分隔)，如 '01 41 00 03': ")
                try:
                    cmd = [int(x, 16) for x in hex_str.split()]
                except Exception:
                    print("格式错误")
                    continue
                mgr.run_on(targets, UniversalSensorTester.send_command, cmd)
            elif choice == '7':
                targets = mgr.choose_targets()
                # 进入用户模式
                mgr.run_on(targets, UniversalSensorTester.send_command, [0x01, 0x1E, 0x00])
                # 读现有 SN（HEX+ASCII 同时显示）
                def _print_sn(t):
                    sn = t.read_sn()
                    if sn:
                        print(f"[{t.port}] 当前SN：{fmt_sn_dual(sn)}")
                    else:
                        print(f"[{t.port}] 读取SN失败")
                mgr.run_on(targets, _print_sn)
                # 写新 SN
                try:
                    new_sn = get_sn_input()
                except Exception as e:
                    print(f"输入错误：{e}")
                    continue
                mgr.run_on(targets, UniversalSensorTester.write_sn, new_sn)
            else:
                print("无效选择。")
    finally:
        mgr.disconnect_all()


if __name__ == "__main__":
    main()
