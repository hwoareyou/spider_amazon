# -*- coding: utf-8 -*-
# @Time    : 2019/3/5 10:12
# @Author  : Liu
# @File    : OCR.py
# @Software: PyCharm


# 能够识别标准的文字和字母，对模糊的图片识别率较低

import base64
import hashlib
import time
import random
import string
import re
from urllib.parse import quote
import requests


class Ocr():

    def __init__(self):
        self.url = "https://api.ai.qq.com/fcgi-bin/ocr/ocr_generalocr"

    def curlmd5(self,src):
        m = hashlib.md5(src.encode('UTF-8'))
        return m.hexdigest().upper()


    # 请求时间戳（秒级），用于防止请求重放（保证签名5分钟有效）
    def get_params(self,base64_data):
        t = time.time()
        time_stamp = str(int(t))
        # 请求随机字符串，用于保证签名不可预测
        nonce_str = ''.join(random.sample(string.ascii_letters + string.digits, 10))
        # 应用标志，这里修改成自己的id和key
        app_id = '2112519211'
        app_key = 'lb4BvIU2v5bM4XLF'
        params = {'app_id': app_id,
                  'image': base64_data,
                  'time_stamp': time_stamp,
                  'nonce_str': nonce_str,
                  }
        sign_before = ''
        # 要对key排序再拼接
        for key in sorted(params):
            # 键值拼接过程value部分需要URL编码，URL编码算法用大写字母，例如%E8。quote默认大写。
            sign_before += '{}={}&'.format(key, quote(params[key], safe=''))
        # 将应用密钥以app_key为键名，拼接到字符串sign_before末尾
        sign_before += 'app_key={}'.format(app_key)
        # 对字符串sign_before进行MD5运算，得到接口请求签名
        sign = self.curlmd5(sign_before)
        params['sign'] = sign
        return params



    def recognition_character(self, img_dir):
        with open(img_dir, 'rb') as f:
            image_data = f.read()
        base64_data = base64.b64encode(image_data)
        params = self.get_params(base64_data)
        r = requests.post(self.url, data=params)
        item_list = r.json()['data']['item_list']
        return item_list[0]['itemstring']


if  __name__ == '__main__':
    img_dir = 'yan.png'
    get_character = Ocr()
    character = get_character.recognition_character(img_dir)
    print(character)