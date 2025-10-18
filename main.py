# 这是一个示例 Python 脚本。
from warning_client import WarningClient
from warning_client.helper import load_config


def main():
    webhook, keyword = load_config('config.json')

    client = WarningClient(stock_db='stock_data.db', tdx_folder='C:/new_tdx')
    client.set_dingtalk_client(webhook, keyword)
    client.start_forever()


if __name__ == '__main__':
    main()
