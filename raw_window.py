from PyQt4.QtCore import *
from PyQt4.QtGui import *
from threading import Thread
import sys
import os
import zipfile
from functools import wraps
from extractor import PerformanceExtractor
from adapter import GuoJinAdapter
from connection import MongoConnection
import logging
import bson

_logger_formatter_str = "\t".join([
    "%(levelname)s",
    "%(asctime)s",
    "%(filename)s",
    "%(lineno)d",
    "%(message)s"
])

_table_columns = ["级别", "日期", "时间", "文件名", "行号", "信息"]
_max_rows = 1000
RESULT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "record")


class FileUploader:
    def __init__(self):
        self._files = []
        self._count = 0
        self._error = 0
        self._running = False
        self._thread = None
        self._connection = MongoConnection()

    def set_files(self, files):
        self._files = files

    def _handle_error(self, error):
        self._error += 1
        logging.exception(error)

    def _run(self):
        self._running = True
        self._count = 0
        self._error = 0
        logging.info("上传文件开始")
        for path in self._files:
            if not self._running:
                break
            if path.endswith(".xls") or path.endswith(".xlsx"):
                try:
                    e = PerformanceExtractor()
                    e.open_with_name(path)
                    self._upload(e.strategy)
                except Exception as e:
                    self._handle_error(e)
                    continue
            elif path.endswith(".zip"):
                try:
                    with zipfile.ZipFile(path) as z:
                        for name in z.filelist:
                            try:
                                if not self._running:
                                    break
                                file = z.read(name)
                                e = PerformanceExtractor()
                                e.open_with_content(file)
                                self._upload(e.strategy)
                            except Exception as e:
                                self._handle_error(e)
                                continue
                except Exception as e:
                    self._handle_error(e)
                    continue
        logging.info("上传文件结束，成功上传文件总数:%s,错误数:%s" % (self._count, self._error))

    def _upload(self, strategy):
        try:
            self._connection.collection.update_one({"info.策略名称": strategy["info"]["策略名称"]},
                                                   {"$set": strategy},
                                                   upsert=True)
        except Exception as e:
            logging.warning("策略%s上传失败" % strategy["info"]["策略名称"])
            self._handle_error(e)
            return None
        self._count += 1
        size = sys.getsizeof(bson.BSON.encode(strategy))
        logging.info("策略[%s]上传成功,大小:%s bytes，此次已成功上传文件数:%s，错误数:%s" %
                     (strategy["info"]["策略名称"],
                      size, self._count, self._error)
                     )

    def start(self):
        if self._running:
            return
        self._thread = Thread(target=self._run)
        self._thread.start()

    def stop(self):
        if not self._running:
            return
        self._running = False
        if self._thread is not None:
            self._thread.join()


def name_setter(func):
    @wraps(func)
    def wrapper(*args, file=None, **kwargs):
        symbol, *name = os.path.basename(file).replace("  ", " ").split(" ")[:-1]  # get symbol + strategy name
        name = " ".join(name)
        return func(*args, name=name, symbol=symbol, file=file, **kwargs)

    return wrapper


def ema_filter(func):  # filter ema future symbol.
    @wraps(func)
    def wrapper(*args, name=None, symbol=None, **kwargs):
        if "000000" in symbol:
            return
        else:
            return func(*args, name=name, symbol=symbol, **kwargs)

    return wrapper


def dk_filter(func):  # filter strategy base on daily k bar.
    @wraps(func)
    def wrapper(*args, name=None, symbol=None, **kwargs):
        if "id" in name:
            return
        else:
            return func(*args, name=name, symbol=symbol, **kwargs)

    return wrapper


class FileTransformer(object):
    def __init__(self):
        self._files = []
        self._running = False
        self._thread = None
        self._error = 0
        self._count = 0

    def set_files(self, files):
        self._files = files

    def _handle_error(self, error):
        self._error += 1
        logging.exception(error)

    def _run(self):
        self._running = True
        self._count = 0
        self._error = 0
        logging.info("转换文件开始")
        for path in self._files:
            if not self._running:
                break
            if path.endswith(".xls") or path.endswith(".xlsx"):
                try:
                    self.to_csv(file=path)
                except Exception as e:
                    self._handle_error(e)
                    continue
            elif path.endswith(".zip"):
                try:
                    with zipfile.ZipFile(path) as z:
                        for name in z.filelist:
                            try:
                                if not self._running:
                                    break
                                content = z.read(name)
                                self.to_csv(file=name.filename, content=content)
                            except Exception as e:
                                self._handle_error(e)
                                continue
                except Exception as e:
                    self._handle_error(e)
                    continue
        logging.info("转换文件结束，成功转换文件总数:%s,错误数:%s" % (self._count, self._error))

    @name_setter
    # @ema_filter
    # @dk_filter
    def to_csv(self, name=None, symbol=None, file=None, content=None):
        e = PerformanceExtractor()
        if content:
            e.open_with_content(content)
        elif file:
            e.open_with_name(file)
        else:
            return
        adapter = GuoJinAdapter(e)
        try:
            df1 = adapter.orders
            df2 = adapter.positions
            df1.to_csv(os.path.join(RESULT_DIR, "order", name + "_orders.csv"), index=False)
            df2.to_csv(os.path.join(RESULT_DIR, "position", name + "_positions.csv"), index=False)
        except Exception as e:
            logging.warning("策略[%s]转换失败" % name if name else "未知")
            self._handle_error(e)
            return None
        self._count += 1
        logging.info("策略[%s]转换成功,此次已成功转换文件数:%s，错误数:%s" %
                     (name if name else "未知", self._count, self._error))

    def start(self):
        if self._running:
            return
        self._thread = Thread(target=self._run)
        self._thread.start()

    def stop(self):
        if not self._running:
            return
        self._running = False
        if self._thread is not None:
            self._thread.join()


class LoggingHandleMixin:
    def get_handle(self):
        raise NotImplementedError


class MainWidget(QMainWindow, LoggingHandleMixin):
    def __init__(self):
        super().__init__()
        self.setFixedSize(QSize(1280, 720))
        self.setWindowTitle("上传绩效")
        self._files = []
        self._uploader = FileUploader()
        self._transformer = FileTransformer()
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.file_list = QListWidget()
        left_layout = QVBoxLayout()
        left_layout.addWidget(QLabel("上传文件列表:"))
        left_layout.addWidget(self.file_list)
        right_layout = QVBoxLayout()
        file_select = QPushButton()
        file_select.setText("选择文件")
        file_select.setDefault(True)
        file_select.clicked.connect(self._on_file_select)
        dir_select = QPushButton()
        dir_select.setText("选择文件夹")
        dir_select.clicked.connect(self._on_dir_select)
        file_upload = QPushButton()
        file_upload.setText("上传文件")
        file_upload.clicked.connect(self._on_file_upload)
        upload_stop = QPushButton()
        upload_stop.setText("停止上传")
        upload_stop.clicked.connect(self._uploader.stop)
        transform_start = QPushButton()
        transform_start.setText("开始转换")
        transform_start.clicked.connect(self._on_transform_start)
        transform_stop = QPushButton()
        transform_stop.setText("停止转换")
        transform_stop.clicked.connect(self._transformer.stop)
        right_layout.addWidget(file_select)
        right_layout.addWidget(dir_select)
        right_layout.addWidget(file_upload)
        right_layout.addWidget(upload_stop)
        right_layout.addWidget(transform_start)
        right_layout.addWidget(transform_stop)
        top_layout = QHBoxLayout()
        top_layout.addLayout(left_layout)
        top_layout.addLayout(right_layout)
        self.log_widget = QTableWidget()
        self.log_widget.setColumnCount(6)
        self.log_widget.setHorizontalHeaderLabels(_table_columns)
        self.log_widget.horizontalHeader().setClickable(False)
        self.log_widget.verticalHeader().setVisible(False)
        self.log_widget.horizontalHeader().setStretchLastSection(True)
        self.log_widget.setEditTriggers(QAbstractItemView.NoEditTriggers)
        bottom_layout = QVBoxLayout()
        bottom_layout.addWidget(QLabel("日志信息:"))
        bottom_layout.addWidget(self.log_widget)
        layout = QVBoxLayout()
        layout.addLayout(top_layout)
        layout.addLayout(bottom_layout)
        central_widget.setLayout(layout)

    def _on_file_select(self):
        file_dialog = QFileDialog(self)
        file_dialog.setWindowTitle("选择策略文件(.zip|.xls|.xlsx)")
        file_dialog.setDirectory(".")
        file_dialog.setFilter("files (*.xls *.xlsx *.zip)")
        file_dialog.setFileMode(QFileDialog.ExistingFiles)
        if file_dialog.exec() == QDialog.Accepted:
            self._files = file_dialog.selectedFiles()
            self._refresh_file_list()

    def _on_dir_select(self):
        dir_dialog = QFileDialog(self)
        dir_dialog.setWindowTitle("选择文件夹(该文件夹下所有[.zip|.xls|.xlsx]文件都会被上传)")
        dir_dialog.setDirectory(".")
        dir_dialog.setFileMode(QFileDialog.DirectoryOnly)
        if dir_dialog.exec() == QDialog.Accepted:
            pass

    def _refresh_file_list(self):
        self.file_list.clear()
        self.file_list.addItems(self._files)

    def _on_file_upload(self):
        self._uploader.stop()
        self._uploader.set_files(self._files)
        self._uploader.start()

    def _on_transform_start(self):
        self._transformer.stop()
        self._transformer.set_files(self._files)
        self._transformer.start()

    @property
    def get_handle(self):
        """

        Returns:
            logging.Handler
        """

        class TableHandler(logging.Handler):
            def __init__(self, table: QTableWidget):
                super().__init__()
                self.setFormatter(logging.Formatter(_logger_formatter_str))
                self._table = table

            def emit(self, record: logging.LogRecord):
                fields = self.format(record).split('\t')
                fields[1:2] = fields[1].split(' ')
                if self._table.rowCount() >= _max_rows:
                    self._table.removeRow(0)
                row_count = self._table.rowCount()
                self._table.insertRow(row_count)
                for i in range(self._table.columnCount()):
                    self._table.setItem(row_count, i, QTableWidgetItem(fields[i]))
                self._table.scrollToBottom()

        return TableHandler(self.log_widget)


if __name__ == '__main__':
    def _get_handler():
        handler = logging.StreamHandler()
        formatter = logging.Formatter(_logger_formatter_str)
        handler.setFormatter(formatter)
        return handler


    logging.root.addHandler(_get_handler())
    logging.root.setLevel(logging.INFO)
    app = QApplication(sys.argv)
    window = MainWidget()
    logging.root.addHandler(window.get_handle)
    window.show()
    sys.exit(app.exec_())
