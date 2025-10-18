import json
import os


def load_config(filename):
    if not os.path.exists(filename):
        return '', ''

    with open(filename, 'r', encoding='utf-8') as f:
        config = json.load(f)

    return config['webhook'], config['keyword']
