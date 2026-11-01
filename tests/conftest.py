import os
from pathlib import Path

import pytest


@pytest.fixture(scope='session')
def real_dataset():
    '''直接读取当前格式的真实数据，打包产物由各测试写入临时目录。'''
    return Path(os.environ['MKTPACK_REAL_DATA']).resolve()
