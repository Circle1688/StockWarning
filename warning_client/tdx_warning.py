import csv
import os
import sqlite3
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from .MyTT import *
import akshare as ak
from datetime import datetime, timedelta
import requests
from apscheduler.triggers.interval import IntervalTrigger
from .log import setup_default_logger
from tqdm import tqdm


class StockSignal(object):
    def __init__(self):
        self.diepo_red = False
        self.shouhui_red = False
        self.diepo_green = False
        self.shouhui_green = False
        self.upper = False
        self.shouhui_green = False
        self.magenta_m = False
        self.yellow_m = False
        self.new_high = False
        self.reverse = False
        self.beizhan = False


class WarningClient(object):
    def __init__(self, stock_db: str, tdx_folder: str):
        self.stock_push = {}
        self.logger = setup_default_logger('WarningClient')
        self.webhook = None
        self.keyword = None
        self.stock_db = stock_db
        self.zxg_file = os.path.join(tdx_folder, r"T0002\blocknew\zxg.blk")

    def create_stock_db_connection(self):
        return sqlite3.connect(self.stock_db)

    def init_stock_database(self):
        conn = self.create_stock_db_connection()
        cursor = conn.cursor()

        # 创建数据表（如果不存在）
        table_info = ['stock_daily', 'stock_weekly', 'stock_monthly']
        for table in table_info:
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol TEXT,
                    date DATE,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    PRIMARY KEY (symbol, date)
                )
                """
            cursor.execute(create_table_query)
            conn.commit()
        conn.close()

    def load_stocks_tdx(self):
        # 读取通达信自选股文件

        stock_list = []
        with open(self.zxg_file) as f:
            f_reader = csv.reader(f)
            # 循环文件中的每一行
            for row in f_reader:
                code = ''.join(row)
                stock_list.append(code[1:])
        return stock_list

    def stock_tech(self, symbol, period):
        """period='daily'; choice of {'daily', 'weekly', 'monthly'}"""
        signal = StockSignal()

        df = self.get_stock_data(symbol=symbol, period=period)

        # 收盘价
        CLOSE = df['close'].values

        # 最低价
        LOW = df['low'].values

        # 最高价
        HIGH = df['high'].values

        # 成交量
        VOL = df['volume'].values

        # 红线
        RED = EMA(CLOSE, 8)

        # 绿线
        GREEN = EMA(CLOSE, 21)

        # 蓝上轨
        BLUE = MA(CLOSE, 60) + 2 * STD(CLOSE, 60)

        # 粉上轨
        PINK = MA(CLOSE, 90) + 2 * STD(CLOSE, 90)

        # 跌破红线
        DPHX = CLOSE < RED
        signal.diepo_red = DPHX[-1]

        # 收回红线
        SHHX = (LOW < RED) & (CLOSE > RED)
        signal.shouhui_red = SHHX[-1]
        # print(RED)

        # 跌破绿线
        DPLX = CLOSE < GREEN
        signal.diepo_green = DPLX[-1]

        # 收回绿线
        SHLX = (LOW < GREEN) & (CLOSE > GREEN)
        signal.shouhui_green = SHLX[-1]

        # 接近蓝上轨或粉上轨任意一条
        n = 2
        SG = ((ABS(CLOSE - BLUE) / BLUE * 100) < n) | ((ABS(CLOSE - PINK) / PINK * 100) < n)
        signal.upper = SG[-1]

        if period == "daily":
            # 信号，只支持日级别
            VAR3 = LLV(LOW, 18)
            VAR4 = HHV(HIGH, 18)
            A1 = EMA((CLOSE - VAR3) / (VAR4 - VAR3) * 100, 13)
            A2 = EMA(0.618 * REF(A1, 1) + 0.382 * A1, 3)
            AA = BARSLAST(REF(CROSS(A1, A2), 1))

            # 紫色M
            FM = CROSS(A1, A2) & (COUNT(A1 >= 80, 30) >= 1)

            signal.magenta_m = FM[-1]

            # 金色M
            CF = CROSS(A1, A2) & (EMA(CLOSE, 8) >= (MA(CLOSE, 90) + 2 * STD(CLOSE, 90)))

            signal.yellow_m = CF[-1]

            # 新高
            TW = (LOW < (MA(CLOSE, 60) + 2 * STD(CLOSE, 60))) & (CLOSE >= (MA(CLOSE, 60) + 2 * STD(CLOSE, 60))) & (
                        CLOSE >= HHV(CLOSE, 30))

            signal.new_high = TW[-1]

            # 转
            TZ = CROSS(A1, A2) & (A2 > LREF(A2, AA + 1)) & AA.astype(bool) & (A2 >= 40)

            signal.reverse = TZ[-1]

            # 备战
            TJ = (VOL >= REF(VOL, 1) * 1.5) & (LOW < (MA(CLOSE, 60) + 2 * STD(CLOSE, 60))) & (
                        CLOSE >= (MA(CLOSE, 60) + 2 * STD(CLOSE, 60))) & (CLOSE >= HHV(CLOSE, 19))
            signal.beizhan = TJ[-1]

        return signal

    def get_stock_data(self, symbol, period):
        conn = self.create_stock_db_connection()
        cursor = conn.cursor()

        # 检查数据库中是否已有该股票数据
        cursor.execute(f"SELECT MAX(date) FROM stock_{period} WHERE symbol = ?", (symbol,))
        latest_date = cursor.fetchone()[0]

        # 今天为最新日期
        end_date = datetime.now().strftime("%Y%m%d")

        # 如果数据库中有一部分数据，就进行增量更新
        if latest_date:
            # 增量更新：记录数据库最新日期，然后移除最新一条数据，再从这个日期开始更新
            start_date = datetime.strptime(latest_date, "%Y-%m-%d").strftime("%Y%m%d")

            # tqdm.write(f"移除股票 {symbol} 的{period}最新一条数据")
            cursor.execute(f"""
                                DELETE FROM stock_{period}
                                WHERE symbol = ?
                                AND date = (
                                    SELECT MAX(date)
                                    FROM stock_{period}
                                    WHERE symbol = ?
                                )
                            """, (symbol, symbol))
            conn.commit()

        else:
            # 数据库没有数据，那么全量下载
            # 获取当前日期
            current_date = datetime.now().date()

            # 计算指定天前的日期
            # 小周期使用前复权 大周期使用不复权
            if period == "daily":
                days = 180
            elif period == "weekly":
                days = 800
            elif period == "monthly":
                days = 3600

            date_days_ago = current_date - timedelta(days=days)

            start_date = date_days_ago.strftime("%Y%m%d")

        if period == "daily":
            adjust = 'qfq'
        elif period == "weekly":
            adjust = ''
        elif period == "monthly":
            adjust = ''

        # tqdm.write(f"更新股票 {symbol} 的最新{period}数据")
        stock_df = ak.stock_zh_a_hist(symbol=symbol, period=period, start_date=start_date, end_date=end_date, adjust=adjust)

        # 重命名列以匹配数据库
        stock_df = stock_df.rename(columns={
            "日期": "date",
            "股票代码": "symbol",
            "最高": "high",
            "最低": "low",
            "收盘": "close",
            "成交量": "volume"
        })

        # 选择需要的列
        stock_df = stock_df[['symbol', 'date', 'high', 'low', 'close', 'volume']]

        # 写入数据库
        stock_df.to_sql(f"stock_{period}", conn, if_exists='append', index=False)

        # 构建查询语句
        query = f"SELECT * FROM stock_{period} WHERE symbol = '{symbol}' ORDER BY date"

        # 读取数据到DataFrame
        df = pd.read_sql_query(query, conn)

        # 转换日期列为datetime类型
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        # self.logger.info(f"成功获取股票 {symbol} 的{period}数据: 共 {len(df)} 条记录")
        # tqdm.write(f"从数据库获取股票 {symbol} 的{period}数据: 共 {len(df)} 条记录")

        conn.close()
        return df

    def get_stock_name(self, symbol):
        stock_individual_info_em_df = ak.stock_individual_info_em(symbol=symbol)
        return stock_individual_info_em_df.iloc[2, 1]

    def stock_analysis(self, symbol: str, at_close: bool = False):
        name = self.get_stock_name(symbol)

        signal = self.stock_tech(symbol, 'daily')
        week_signal = self.stock_tech(symbol, 'weekly')
        month_signal = self.stock_tech(symbol, 'monthly')

        push_content = []

        if signal.upper:
            push_content.append('到达日上轨附近')

        if week_signal.upper:
            push_content.append('到达周上轨附近')

        if month_signal.upper:
            push_content.append('到达月上轨附近')

        if month_signal.diepo_red:
            push_content.append('跌破月红')
        else:
            if week_signal.diepo_green:
                push_content.append('跌破周绿')
            else:
                if week_signal.diepo_red:
                    push_content.append('跌破周红')
                else:
                    if signal.diepo_green:
                        push_content.append('跌破日绿')
                    else:
                        if signal.diepo_red:
                            push_content.append('跌破日红')

        if signal.shouhui_red:
            push_content.append('收回日红')

        if signal.shouhui_green:
            push_content.append('收回日绿')

        if week_signal.shouhui_red:
            push_content.append('收回周红')

        if month_signal.shouhui_red:
            push_content.append('收回月红')

        # 日信号
        signal_content = []
        if signal.magenta_m:
            signal_content.append('紫色M')
        if signal.yellow_m:
            signal_content.append('金色M')
        if signal.new_high:
            signal_content.append('新高')
        if signal.reverse:
            signal_content.append('转')
        if signal.beizhan:
            signal_content.append('备战')

        if not signal_content:
            if at_close:
                push_content.append('没有出现日信号')
        else:
            push_content.append(f'出现日{'+'.join(signal_content)}信号')

        return f'{symbol} {name}', "，".join(push_content)

    def push(self, title, message):
        try:
            url = self.webhook
            if url:
                body = {
                    "at": {
                        "isAtAll": True
                    },
                    "text": {
                        "content": f'{self.keyword} {title}\n{message}'
                    },
                    "msgtype": "text"
                }
                headers = {'Content-Type': 'application/json'}
                resp = requests.post(url, json=body, headers=headers)

        except Exception as e:
            self.logger.error(e)

    def stock_warning(self, at_close=False):
        stocks = self.load_stocks_tdx()

        self.logger.info('分析通达信自选股')
        # 多线程容易被封ip
        for stock in tqdm(stocks):
            title, push_content = self.stock_analysis(stock, at_close)

            tqdm.write(f'{title} {push_content}')

            need_push = False

            if not at_close:
                if stock in self.stock_push:
                    if push_content != self.stock_push[stock]:
                        need_push = True
                else:
                    self.stock_push[stock] = push_content
                    need_push = True
            else:
                need_push = True

            if need_push:
                if push_content != "":
                    if at_close:
                        self.push(title, f"尾盘{push_content}")
                    else:
                        self.push(title, push_content)

        if at_close:
            # 尾盘清空记录
            self.stock_push = {}


    def pre_start(self):
        self.logger.info('连接股票数据库')
        self.init_stock_database()

    def start_forever(self):
        self.pre_start()

        scheduler = BlockingScheduler()

        # 盘中检查
        scheduler.add_job(self.stock_warning, args=[False], trigger='cron', day_of_week='mon-fri', hour=9, minute='30-55/5')
        scheduler.add_job(self.stock_warning, args=[False], trigger='cron', day_of_week='mon-fri', hour=10, minute='0-55/5')
        scheduler.add_job(self.stock_warning, args=[False], trigger='cron', day_of_week='mon-fri', hour=11, minute='0-30/5')
        scheduler.add_job(self.stock_warning, args=[False], trigger='cron', day_of_week='mon-fri', hour=13, minute='0-55/5')
        scheduler.add_job(self.stock_warning, args=[False], trigger='cron', day_of_week='mon-fri', hour=14, minute='0-45/5')

        # 尾盘检查
        scheduler.add_job(self.stock_warning, args=[True], trigger='cron', day_of_week='mon-fri', hour=14, minute='50')

        scheduler.add_job(self.stock_warning, args=[False], trigger=IntervalTrigger(minutes=1), next_run_time=datetime.now())

        try:
            # 启动调度器
            scheduler.start()
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()

    def set_logger(self, logger: logging.Logger = None):
        if logger:
            self.logger = logger

    def set_dingtalk_client(self, webhook: str = None, keyword: str = None):
        if webhook:
            self.webhook = webhook
        if keyword:
            self.keyword = keyword
