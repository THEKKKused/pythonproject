"""
IoTDB 数据可视化管理软件 (防Excel自动格式化版)
修改点：
1. 导出 CSV 时，在时间前添加制表符，强制 Excel 显示完整日期时间。
2. 保持正序排列。
3. 保持宽列显示。
"""

import sys
import os
import datetime
import pandas as pd

from PyQt6.QtCore import Qt, QThread, pyqtSignal, QDateTime, QAbstractTableModel, QModelIndex
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QLineEdit, QPushButton, QGroupBox, QSplitter,
    QListWidget, QListWidgetItem, QTableView, QHeaderView,
    QCheckBox, QDateTimeEdit, QMessageBox, QFileDialog,
    QProgressBar, QSpinBox
)

# ---- IoTDB 导入 ----
try:
    from iotdb.Session import Session
except ImportError:
    Session = None
    print("请先安装 apache-iotdb==1.2.0")

# ---------------- 配置常量 ----------------
DEFAULT_HOST = '127.0.0.1'
DEFAULT_PORT = 6667
DEFAULT_USER = 'root'
DEFAULT_PASS = 'root'
DEFAULT_SG = 'root.h2'


# ---------------- 高性能表格模型 ----------------
class PandasModel(QAbstractTableModel):
    def __init__(self, data: pd.DataFrame):
        super().__init__()
        self._data = data

    def rowCount(self, parent=QModelIndex()):
        return self._data.shape[0]

    def columnCount(self, parent=QModelIndex()):
        return self._data.shape[1]

    def data(self, index, role=Qt.ItemDataRole.DisplayRole):
        if index.isValid():
            if role == Qt.ItemDataRole.DisplayRole:
                val = self._data.iloc[index.row(), index.column()]
                return str(val)
        return None

    def headerData(self, col, orientation, role):
        if orientation == Qt.Orientation.Horizontal and role == Qt.ItemDataRole.DisplayRole:
            return self._data.columns[col]
        return None


# ---------------- 工具函数 ----------------
def get_beijing_time_str(ts_ms):
    """
    将毫秒时间戳转换为北京时间字符串
    格式：YYYY-MM-DD HH:MM:SS.mmm
    """
    try:
        # 构造 UTC+8 时区
        tz_bj = datetime.timezone(datetime.timedelta(hours=8))
        # 转换为 datetime 对象
        dt = datetime.datetime.fromtimestamp(ts_ms / 1000.0, tz_bj)
        # 格式化字符串 (截取毫秒前3位)
        return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    except Exception as e:
        return str(ts_ms)


# ---------------- 后台线程：查询设备列表 ----------------
class DeviceScanWorker(QThread):
    finished_signal = pyqtSignal(list, str)

    def __init__(self, host, port, user, password, sg):
        super().__init__()
        self.cfg = (host, port, user, password)
        self.sg = sg

    def run(self):
        if Session is None:
            self.finished_signal.emit([], "未安装 apache-iotdb")
            return
        try:
            session = Session(*self.cfg)
            session.open(False)
            sql = f"show devices {self.sg}.**"
            devices = []
            with session.execute_query_statement(sql) as dataset:
                while dataset.has_next():
                    row = dataset.next()
                    fields = row.get_fields()
                    if fields:
                        devices.append(str(fields[0]))
            session.close()
            # 排序：尝试按数字后缀排序
            try:
                devices.sort(key=lambda x: int(x.split('_')[-1]) if '_' in x and x.split('_')[-1].isdigit() else x)
            except:
                devices.sort()
            self.finished_signal.emit(devices, "")
        except Exception as e:
            self.finished_signal.emit([], str(e))


# ---------------- 后台线程：查询数据 (UI预览) ----------------
class DataQueryWorker(QThread):
    data_signal = pyqtSignal(object)  # pandas DataFrame
    error_signal = pyqtSignal(str)

    def __init__(self, host, port, user, password, device_id, start_ms, end_ms, limit_rows=0):
        super().__init__()
        self.cfg = (host, port, user, password)
        self.device_id = device_id
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.limit = limit_rows

    def run(self):
        try:
            session = Session(*self.cfg)
            session.open(False)

            # 构造 SQL
            time_clause = []
            if self.start_ms is not None:
                time_clause.append(f"time >= {self.start_ms}")
            if self.end_ms is not None:
                time_clause.append(f"time <= {self.end_ms}")

            where_sql = (" WHERE " + " AND ".join(time_clause)) if time_clause else ""
            limit_sql = f" LIMIT {self.limit}" if self.limit > 0 else ""

            # 按时间正序 ASC
            sql = f"SELECT * FROM {self.device_id}{where_sql} ORDER BY time ASC{limit_sql}"

            data_list = []
            headers = ['Time']

            with session.execute_query_statement(sql) as dataset:
                raw_cols = dataset.get_column_names()
                for c in raw_cols:
                    if c.lower() == 'time': continue
                    short = c.split(self.device_id + '.')[-1] if self.device_id in c else c
                    headers.append(short)

                while dataset.has_next():
                    r = dataset.next()
                    ts = r.get_timestamp()
                    fields = r.get_fields()

                    # 转换为北京时间
                    time_str = get_beijing_time_str(ts)

                    row = [time_str] + [str(f) if f is not None else "" for f in fields]
                    data_list.append(row)

            session.close()

            df = pd.DataFrame(data_list, columns=headers)
            self.data_signal.emit(df)

        except Exception as e:
            self.error_signal.emit(f"查询异常: {e}")


# ---------------- 后台线程：导出 (流式写入CSV - 修复版) ----------------
class ExportWorker(QThread):
    progress_signal = pyqtSignal(int, int, str)
    finished_signal = pyqtSignal(str)

    def __init__(self, host, port, user, password, devices, start_ms, end_ms, save_dir, export_suffix=""):
        super().__init__()
        self.cfg = (host, port, user, password)
        self.devices = devices
        self.start_ms = start_ms
        self.end_ms = end_ms
        self.save_dir = save_dir
        self.export_suffix = export_suffix.strip()
        self._running = True

    def run(self):
        try:
            session = Session(*self.cfg)
            session.open(False)
        except Exception as e:
            self.finished_signal.emit(f"连接失败: {e}")
            return

        total = len(self.devices)
        count = 0

        for idx, dev in enumerate(self.devices):
            if not self._running:
                break
            name = dev.split('.')[-1]
            export_name = f"{name} {self.export_suffix}" if self.export_suffix else name
            self.progress_signal.emit(idx, total, f"正在导出 {export_name}...")

            try:
                # 导出无需 LIMIT，查询全部指定时间段
                time_clause = []
                if self.start_ms:
                    time_clause.append(f"time >= {self.start_ms}")
                if self.end_ms:
                    time_clause.append(f"time <= {self.end_ms}")
                where_s = (" WHERE " + " AND ".join(time_clause)) if time_clause else ""

                # 导出也按时间正序
                sql = f"SELECT * FROM {dev}{where_s} ORDER BY time ASC"

                file_path = os.path.join(self.save_dir, f"{export_name}.csv")

                with open(file_path, 'w', encoding='utf-8-sig') as f:
                    with session.execute_query_statement(sql) as ds:
                        # 1. 写入表头
                        cols = ds.get_column_names()
                        clean_cols = ['Time'] + [c.split(dev + '.')[-1] for c in cols if c.lower() != 'time']
                        f.write(",".join(clean_cols) + "\n")

                        # 2. 写入数据
                        while ds.has_next():
                            r = ds.next()
                            ts = r.get_timestamp()
                            fields = r.get_fields()

                            t_str = get_beijing_time_str(ts)

                            # 【核心修复】在时间前加 \t，强制 Excel 将其视为文本，不进行自动格式化
                            # 这样显示的就是完整的 "2023-10-27 10:00:00.123"
                            vals = ["\t" + t_str] + [str(x) if x is not None else "" for x in fields]

                            f.write(",".join(vals) + "\n")
                count += 1
            except Exception as e:
                print(f"导出 {export_name} 失败: {e}")

        session.close()
        self.progress_signal.emit(total, total, "完成")
        self.finished_signal.emit(f"成功导出 {count}/{total} 个文件")


# ---------------- 主界面 ----------------
class IoTDBViewer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("温度标定数据库管理 (Final)")
        self.resize(1200, 760)
        self._setup_ui()

    def _setup_ui(self):
        main = QWidget()
        self.setCentralWidget(main)
        layout = QVBoxLayout(main)

        # 1. 连接栏
        gb_conn = QGroupBox("连接设置")
        hl_conn = QHBoxLayout()
        self.ed_host = QLineEdit(DEFAULT_HOST)
        self.ed_port = QLineEdit(str(DEFAULT_PORT))
        self.ed_sg = QLineEdit(DEFAULT_SG)
        self.btn_conn = QPushButton("连接并扫描")
        self.btn_conn.clicked.connect(self.on_scan)

        hl_conn.addWidget(QLabel("Host:"))
        hl_conn.addWidget(self.ed_host)
        hl_conn.addWidget(QLabel("Port:"))
        hl_conn.addWidget(self.ed_port)
        hl_conn.addWidget(QLabel("Group:"))
        hl_conn.addWidget(self.ed_sg)
        hl_conn.addWidget(self.btn_conn)
        gb_conn.setLayout(hl_conn)
        layout.addWidget(gb_conn)

        # 2. 主体
        splitter = QSplitter(Qt.Orientation.Horizontal)

        # 左侧列表
        left = QWidget()
        vl_left = QVBoxLayout(left)
        vl_left.addWidget(QLabel("传感器列表 (支持多选)"))
        self.lst_dev = QListWidget()
        self.lst_dev.setSelectionMode(QListWidget.SelectionMode.ExtendedSelection)
        self.lst_dev.itemClicked.connect(self.on_dev_click)
        vl_left.addWidget(self.lst_dev)
        self.btn_all = QPushButton("全选")
        self.btn_all.clicked.connect(self.select_all)
        vl_left.addWidget(self.btn_all)
        splitter.addWidget(left)

        # 右侧数据
        right = QWidget()
        vl_right = QVBoxLayout(right)

        # 筛选栏
        gb_filter = QGroupBox("查询与导出控制")
        hl_filter = QHBoxLayout()

        self.cb_time = QCheckBox("全部时间")
        self.cb_time.setChecked(True)
        self.cb_time.toggled.connect(lambda c: (self.dt_s.setEnabled(not c), self.dt_e.setEnabled(not c)))

        now = QDateTime.currentDateTime()
        self.dt_s = QDateTimeEdit(now.addDays(-1))
        self.dt_s.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.dt_s.setEnabled(False)
        self.dt_e = QDateTimeEdit(now)
        self.dt_e.setDisplayFormat("yyyy-MM-dd HH:mm:ss")
        self.dt_e.setEnabled(False)

        # 显示限制
        hl_filter.addWidget(self.cb_time)
        hl_filter.addWidget(QLabel("起:"))
        hl_filter.addWidget(self.dt_s)
        hl_filter.addWidget(QLabel("止:"))
        hl_filter.addWidget(self.dt_e)
        hl_filter.addWidget(QLabel("  |  预览行数(0=全部):"))
        self.sp_limit = QSpinBox()
        self.sp_limit.setRange(0, 10000000)
        self.sp_limit.setValue(1000)
        self.sp_limit.setToolTip("设为0则显示所有数据")
        hl_filter.addWidget(self.sp_limit)
        hl_filter.addWidget(QLabel("  |  导出文件后缀:"))
        self.ed_export_suffix = QLineEdit()
        self.ed_export_suffix.setPlaceholderText("如：标定 / 复核（留空则不追加）")
        self.ed_export_suffix.setMaximumWidth(220)
        hl_filter.addWidget(self.ed_export_suffix)
        hl_filter.addStretch()
        gb_filter.setLayout(hl_filter)
        vl_right.addWidget(gb_filter)

        # 按钮栏
        hl_btns = QHBoxLayout()
        self.btn_query = QPushButton("刷新显示")
        self.btn_query.clicked.connect(self.on_query)
        self.btn_export = QPushButton("导出选中数据 (CSV)")
        self.btn_export.setStyleSheet("background-color:#e1f5fe; font-weight:bold;")
        self.btn_export.clicked.connect(self.on_export)
        hl_btns.addWidget(self.btn_query)
        hl_btns.addWidget(self.btn_export)
        vl_right.addLayout(hl_btns)

        # 表格 (QTableView)
        self.table = QTableView()
        self.table.setAlternatingRowColors(True)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True)
        vl_right.addWidget(self.table)

        splitter.addWidget(right)
        splitter.setStretchFactor(1, 3)
        layout.addWidget(splitter)

        # 状态栏
        self.prog = QProgressBar()
        self.lbl_stat = QLabel("就绪")
        hl_bot = QHBoxLayout()
        hl_bot.addWidget(self.lbl_stat)
        hl_bot.addWidget(self.prog)
        layout.addLayout(hl_bot)

    # ---------------- 逻辑 ----------------
    def select_all(self):
        for i in range(self.lst_dev.count()):
            self.lst_dev.item(i).setSelected(True)

    def on_scan(self):
        self.lst_dev.clear()
        self.btn_conn.setEnabled(False)
        self.lbl_stat.setText("扫描中...")
        self.worker_scan = DeviceScanWorker(
            self.ed_host.text(), int(self.ed_port.text()),
            DEFAULT_USER, DEFAULT_PASS, self.ed_sg.text()
        )
        self.worker_scan.finished_signal.connect(self.on_scan_done)
        self.worker_scan.start()

    def on_scan_done(self, devs, err):
        self.btn_conn.setEnabled(True)
        if err:
            QMessageBox.warning(self, "错误", err)
            self.lbl_stat.setText("扫描失败")
            return
        for d in devs:
            item = QListWidgetItem(d.split('.')[-1])
            item.setData(Qt.ItemDataRole.UserRole, d)
            self.lst_dev.addItem(item)
        self.lbl_stat.setText(f"发现 {len(devs)} 个设备")

    def on_dev_click(self):
        pass

    def get_times(self):
        if self.cb_time.isChecked(): return None, None
        return (self.dt_s.dateTime().toMSecsSinceEpoch(),
                self.dt_e.dateTime().toMSecsSinceEpoch())

    def on_query(self):
        items = self.lst_dev.selectedItems()
        if not items: return
        dev = items[0].data(Qt.ItemDataRole.UserRole)

        s, e = self.get_times()
        lim = self.sp_limit.value()

        if lim == 0 and s is None:
            ret = QMessageBox.question(self, "确认",
                                       "您选择了【无上限】且【全部时间】。\n数据量过大可能导致查询缓慢。\n是否继续？",
                                       QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if ret == QMessageBox.StandardButton.No:
                return

        self.lbl_stat.setText("正在查询数据...")
        self.btn_query.setEnabled(False)

        self.worker_query = DataQueryWorker(
            self.ed_host.text(), int(self.ed_port.text()),
            DEFAULT_USER, DEFAULT_PASS, dev, s, e, lim
        )
        self.worker_query.data_signal.connect(self.on_data_ready)
        self.worker_query.error_signal.connect(
            lambda e: (self.lbl_stat.setText("错误"), QMessageBox.warning(self, "错", e)))
        self.worker_query.finished.connect(lambda: self.btn_query.setEnabled(True))
        self.worker_query.start()

    def on_data_ready(self, df: pd.DataFrame):
        model = PandasModel(df)
        self.table.setModel(model)
        # 强制加宽第一列
        self.table.setColumnWidth(0, 220)
        self.lbl_stat.setText(f"显示 {len(df)} 行数据")

    def on_export(self):
        items = self.lst_dev.selectedItems()
        if not items: return QMessageBox.warning(self, "提示", "未选择设备")

        path = QFileDialog.getExistingDirectory(self, "选择保存目录")
        if not path: return

        devs = [i.data(Qt.ItemDataRole.UserRole) for i in items]
        s, e = self.get_times()

        self.btn_export.setEnabled(False)
        self.worker_exp = ExportWorker(
            self.ed_host.text(), int(self.ed_port.text()),
            DEFAULT_USER, DEFAULT_PASS, devs, s, e, path, self.ed_export_suffix.text()
        )
        self.worker_exp.progress_signal.connect(
            lambda c, t, m: (self.prog.setValue(c), self.prog.setMaximum(t), self.lbl_stat.setText(m)))
        self.worker_exp.finished_signal.connect(
            lambda m: (self.btn_export.setEnabled(True), QMessageBox.information(self, "完成", m)))
        self.worker_exp.start()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = IoTDBViewer()
    w.show()
    sys.exit(app.exec())



