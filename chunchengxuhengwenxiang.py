import time
import pymodbus
from pymodbus.client import ModbusTcpClient

# --- 配置参数 ---
DEVICE_IP = '192.168.50.2'  # 恒温箱IP
DEVICE_PORT = 8000
SLAVE_ID = 1

# --- 寄存器地址 ---
REG_SET_TEMP = 8100
REG_CUR_TEMP = 7991
COIL_START = 8000
COIL_STOP = 8001
REG_MODE = 8108

# --- 兼容性处理 ---
ver = pymodbus.__version__
if ver.startswith('3.1') or ver.startswith('4.'):
    SLAVE_ARG = 'device_id'
elif ver.startswith('3.'):
    SLAVE_ARG = 'slave'
else:
    SLAVE_ARG = 'unit'


def send_command(client, func_name, address, value=None):
    """通用发送函数"""
    func = getattr(client, func_name)
    kwargs = {SLAVE_ARG: SLAVE_ID}
    if 'write' in func_name:
        return func(address, value, **kwargs)
    else:
        return func(address, count=1, **kwargs)


def get_current_temp(client):
    """读取当前温度"""
    res = send_command(client, 'read_holding_registers', REG_CUR_TEMP)
    if not res.isError():
        raw = res.registers[0]
        if raw > 32767: raw -= 65536  # 处理负数读数
        return raw / 10.0
    return None


def set_target_temp(client, temp_val):
    """设定目标温度 (需包含负数写入转换)"""
    # 转换为整数，例如 -35.0 -> -350
    target_int = int(temp_val * 10)

    # Modbus 协议写入负数需要转换为无符号整数
    # 示例 : -23度(即-230) -> 65536 - 230 = 65306
    if target_int < 0:
        write_val = 65536 + target_int
    else:
        write_val = target_int

    res = send_command(client, 'write_register', REG_SET_TEMP, write_val)
    if not res.isError():
        print(f"✅ 目标温度已设为: {temp_val}℃ (写入值: {write_val})")
        return True
    else:
        print(f"❌ 设定温度失败: {res}")
        return False


def wait_for_temp(client, target, mode='heat'):
    """
    等待温度到达
    mode='heat': 等待温度 >= target
    mode='cool': 等待温度 <= target
    """
    print(f"正在{'升温' if mode == 'heat' else '降温'}至 {target}℃...")
    while True:
        curr = get_current_temp(client)
        if curr is not None:
            print(f" 当前: {curr}℃ | 目标: {target}℃")

            if mode == 'cool' and curr <= target:
                print(f"已降温至 {target}℃！")
                break
            elif mode == 'heat' and curr >= target:
                print(f"已升温至 {target}℃！")
                break
        else:
            print("❌ 读取错误，重试中...")

        time.sleep(2)


def main():
    client = ModbusTcpClient(DEVICE_IP, port=DEVICE_PORT)
    if not client.connect():
        print("无法连接设备")
        return

    try:
        print(" 设备已连接")

        # 1. 确保定值模式
        send_command(client, 'write_register', REG_MODE, 0)
        time.sleep(0.5)

        # 2. 启动设备
        send_command(client, 'write_coil', COIL_START, True)
        print("设备已启动")
        print("-" * 30)

        # === 第一阶段：降温至 -35度 ===
        print("正在降温至-35℃")
        if set_target_temp(client, -35.0):
            wait_for_temp(client, -35.0, mode='cool')

        time.sleep(2)  # 阶段缓冲
        print("-" * 30)

        # === 第二阶段：升温至 35度 ===
        print("正在升温至35℃")
        if set_target_temp(client, 35.0):
            wait_for_temp(client, 35.0, mode='heat')

        time.sleep(2)
        print("-" * 30)

        # === 第三阶段：升温至 70度 ===
        print("正在升温至70℃")
        if set_target_temp(client, 70.0):
            wait_for_temp(client, 70.0, mode='heat')

        print("-" * 30)
        print("所有阶段完成！")

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断")
    except Exception as e:
        print(f"❌ 发生错误: {e}")
    finally:
        # 任务结束，停止设备
        send_command(client, 'write_coil', COIL_STOP, True)
        print("设备已停止")
        client.close()


if __name__ == "__main__":
    main()