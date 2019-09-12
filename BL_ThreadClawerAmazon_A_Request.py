# -*- coding: utf-8 -*-
# @Author   : liu
# 加入日志
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium import webdriver
import time
import json, re, os, urllib.request, datetime, random, requests, sys, socket
from lxml import etree
from mysql_utils.mysql_db import MysqlDb
from baidu_OCR import recognition_character
import threading
from threading import Thread
from queue import Queue
from tengxun_OCR import Ocr
# from fake_useragent import UserAgent
from log_utils.mylog import Mylog
from PIL import Image
import traceback
import warnings

warnings.filterwarnings('ignore')


class ThreadClawerAmazon(Thread):

    def __init__(self, i, product_link_queue, product_info_queue, user_id):
        '''
        :param i: 线程编号
        :param product_link_queue:商品链接队列
        :param product_info_queue: 商品信息队列
        :param user_id: 用户id
        '''

        Thread.__init__(self)
        self.user_id = user_id
        self.mysql = MysqlDb()
        self.threadName = '采集线程' + str(i)
        self.product_link_queue = product_link_queue
        self.product_info_queue = product_info_queue

        self.s = requests.session()

        # 代理服务器
        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        proxyUser = "H19SC127905HL13D"
        proxyPass = "7942D1D850E8D5C5"

        cap = DesiredCapabilities.PHANTOMJS.copy()  # 使用copy()防止修改原代码定义dict
        service_args = [
            '--ssl-protocol=any',  # 任何协议
            '--cookies-file=False',  # cookies文件
            '--disk-cache=no',  # 不设置缓存
            # '--ignore-ssl-errors=true'  # 忽略https错误

            "--proxy-type=http",
            "--proxy=%(host)s:%(port)s" % {
                "host": proxyHost,
                "port": proxyPort,
            },
            "--proxy-auth=%(user)s:%(pass)s" % {
                "user": proxyUser,
                "pass": proxyPass,
            },
        ]

        headers = {
            "Host": "www.amazon.com",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            # "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
            "User-Agent": get_useragent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "Referer": "www.amazon.com",
            # "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-CN,zh;q=0.9",
        }
        for key, value in headers.items():
            cap['phantomjs.page.customHeaders.{}'.format(key)] = value

        self.driver = webdriver.PhantomJS(executable_path="/home/phantomjs-2.1.1-linux-x86_64/bin/phantomjs",desired_capabilities=cap, service_args=service_args)
        # self.driver = webdriver.PhantomJS(desired_capabilities=cap, service_args=service_args)

        # 设置页面最大加载时间
        self.driver.set_page_load_timeout(60)

        pass

    # 采集商品的详情页
    def __clawlerProtect__(self, product_link):
        '''
        :param product_link: 商品链接
        :return:
        '''
        try:
            try:
                self.driver.get(product_link)
            except:
                count = 1
                while count <= 5:
                    try:
                        self.driver.get(product_link)
                        break
                    except:
                        err_info = '__clawlerProtect__driver.get() reloading for %d time' % count if count == 1 else '__clawlerProtect__driver.get() reloading for %d times' % count
                        print(err_info)
                        count += 1
                if count > 5:
                    print("__clawlerProtect__driver.get() job failed!")
                    cookie = ''
            product_html = self.driver.page_source
            title = self.driver.title
            cookie = ''
            for cookie_data in self.driver.get_cookies():
                cookie += str(cookie_data['name']) + '=' + str(cookie_data['value']) + ';'
            return product_html, cookie, title
        except Exception as err:
            print(err)

    # 解析提取商品数据
    def __parseProduct__(self, html, product_link, asin, product_attr_value, type):
        '''
        :param html: 商品详情页html
        :param product_link: 商品链接
        :param asin: 商品变体asin
        :param product_attr_value: 商品变体属性值
        :param type: 解析类型（type=1，解析主体商品；type=2，解析变体商品）
        :return: 返回解析后的商品数据
        '''
        try:
            product_html = etree.HTML(html)
            product_info = {}
            # 名称
            productName = str(product_html.xpath('string(//*[@id="productTitle"])')).strip()
            product_info['product_name'] = productName
            # 品牌
            productBrand = str(product_html.xpath('string(//*[@id="bylineInfo"])')).strip()
            product_info['brand_name'] = productBrand

            # ASIN 编码
            productASIN = str(
                product_html.xpath('string(//span[contains(text(), "ASIN")]/following-sibling::span[1])')).strip()
            product_info['productASIN'] = productASIN
            # 商品变体asin编码
            if asin:
                product_info['product_attr_asin'] = asin
            else:
                product_info['product_attr_asin'] = ''
            # 上架时间
            productOnsaleDate = str(product_html.xpath(
                'string(//span[contains(text(), "Date first listed on Amazon")]/following-sibling::span[1])')).strip()
            if productOnsaleDate:
                product_info['upload_date'] = datetime.datetime.strptime(productOnsaleDate, '%B %d, %Y')
            else:
                product_info['upload_date'] = '1900-01-01'
            # 商店
            productStore = productBrand
            product_info['productStore'] = productStore
            # 价格
            productPrice = str(product_html.xpath('string(//*[@id="priceblock_ourprice"])')).strip()
            product_info['price'] = productPrice
            # 商品默认图片链接
            product_info['img_url'] = str(product_html.xpath('string(//*[@id="landingImage"]/@data-old-hires)'))
            # 商品链接
            product_info['product_link'] = product_link
            # 卖家数量
            productSellerNumbers = str(product_html.xpath('string(//*[@id="olp-upd-new"]//span/a/text())')).strip()
            if productSellerNumbers:
                productSellerNumbers = re.search(r'[.*]?(\d+)[.*]?', productSellerNumbers).group(1)
                # productSellerNumbers = re.search(r'\((\d+)\) from', productSellerNumbers).group(1)
                product_info['productSellerNumbers'] = productSellerNumbers
            else:
                product_info['productSellerNumbers'] = 0

            # 星级
            productStarLevel = str(product_html.xpath('string(//*[@id="acrPopover"]/@title)')).strip()
            if productStarLevel:
                productStarLevel = re.search(r'(.+?) [.*]?', productStarLevel).group(1)
                product_info['grade_star'] = productStarLevel
            else:
                product_info['grade_star'] = ''

            # 评论数
            productReviewNumber = str(product_html.xpath('string(//*[@id="acrCustomerReviewText"])'))
            if productReviewNumber:
                productReviewNumber = re.search(r'(\d+) customer review', productReviewNumber).group(1)
                product_info['comment_volume'] = productReviewNumber
            else:
                product_info['comment_volume'] = ''

            # 卖点
            selling_point = str(product_html.xpath('string(//*[@id="feature-bullets"])')).strip().replace('\n', '')
            if selling_point:
                product_info['selling_point'] = selling_point
            else:
                product_info['selling_point'] = ''

            # 商品描述
            product_description = product_html.xpath('//*[@id="productDescription"]')

            if product_description:
                product_info['product_description'] = etree.tostring(product_description[0], encoding="utf-8",pretty_print=True, method="html").decode('utf-8')
            else:
                product_info['product_description'] = ''

            # 排名
            # 总排名
            salesRank = product_html.xpath('//*[@id="SalesRank"]/text()[2]')
            if salesRank:
                salesRank = str(product_html.xpath('//*[@id="SalesRank"]/text()[2]')[0]).replace('(', '')
                # 规范排名格式（元组类型：（排名，类别））
                salesRank = (re.search(r'#([,\d]+) in (.+)', salesRank).group(1).strip().replace(',', ''),
                             re.search(r'#([,\d]+) in (.+)', salesRank).group(2).strip())
                # 分类排名
                # categorySalesRank = product_html.xpath('//*[@id="SalesRank"]//li')
                categorySalesRank = str(product_html.xpath('string(//*[@id="SalesRank"]/ul)'))
                if categorySalesRank:
                    categorySalesRank = re.findall(r'#([,\d]+)    in (.+?)    ', categorySalesRank)
                    categorySalesRank.insert(0, salesRank)
                else:
                    categorySalesRank = [salesRank]
                product_info['categorySalesRank'] = categorySalesRank
            else:
                product_info['categorySalesRank'] = []

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            # err: time data '' does not match format '%B %d, %Y'
            traceback.print_exc()

        try:
            if type == 1:
                print('商品信息：{ASIN:%s}' % (product_info['productASIN']))
                # 变体信息（每个变体对应不同的属性值和asin）
                dimensionValuesDisplayData = re.search(r'dimensionValuesDisplayData" : (.+),', html)
                if dimensionValuesDisplayData:
                    dimensionValuesDisplayData = re.search(r'dimensionValuesDisplayData" : (.+),', html).group(1)
                    product_info['dimensionValuesDisplayData'] = dimensionValuesDisplayData

                    # 从html中提取属性信息
                    # 属性名称（如color、size）
                    dimensionsDisplay = json.loads(re.search(r'dimensionsDisplay" : (.+),', html).group(1))
                    product_info['dimensionsDisplay'] = dimensionsDisplay

                    # 属性名称对应的标签（如color对应color_name,size对应size_name）
                    variationDisplayLabels = json.loads(re.search(r'variationDisplayLabels" : (.+),', html).group(1))
                    product_info['variationDisplayLabels'] = variationDisplayLabels

                    # 根据属性标签提取属性对应的值（例如：属性为color的标签为color_name，其对应的值有black、white... ）
                    # size_data = json.loads(re.search(r'variationValues" : (.+),', html).group(1))['size_name']
                    variationValues = json.loads(re.search(r'variationValues" : (.+),', html).group(1))
                    product_info['variationValues'] = variationValues

                else:
                    product_info['dimensionValuesDisplayData'] = ''
                    product_info['dimensionsDisplay'] = ''
                    product_info['variationDisplayLabels'] = ''
                    product_info['variationValues'] = ''

            elif type == 2:

                # 是否有购物车
                product_cart = product_html.xpath('//*[@id="add-to-cart-button"]')
                if product_cart:
                    product_info['product_cart'] = 1
                else:
                    product_info['product_cart'] = 0

                # 是否有发仓（FBA）
                product_FBA = product_html.xpath('//*[@id="SSOFpopoverLink"]')
                if product_FBA:
                    product_info['product_FBA'] = 1
                else:
                    product_info['product_FBA'] = 0

                # 变体商品的属性
                product_info['attr_value'] = product_attr_value

                print('商品信息：{ASIN:%s,asin:%s,attr_value:%s}' % (
                product_info['productASIN'], product_info['product_attr_asin'], product_info['attr_value']))

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            # err: 'NoneType' object has no attribute 'group'
            traceback.print_exc()

        return product_info

    # 识别验证码(baidu_ocr)
    def get_character_by_ocr(self, product_link):
        '''
        :param product_link: 商品链接
        :return:
        '''
        self.driver.get(product_link)
        time.sleep(random.randint(1, 3))
        try:
            print('出现验证码，正在验证！')
            # 最多进行20次验证
            for i in range(20):
                try:

                    element = self.driver.find_element_by_xpath('/html/body/div/div[1]/div[3]/div/div/form/div[1]/div/div/div[1]')
                except:
                    # 验证元素不存在，说明验证通过（或者已跳转至非验证页面）
                    print('验证通过！')
                    cookie = {}
                    for cookie_data in self.driver.get_cookies():
                        cookie[cookie_data['name']] = cookie_data['value']

                    html = self.driver.page_source
                    # 把cookie值转换为cookiejar类型，然后传给Session
                    self.s.cookies = requests.utils.cookiejar_from_dict(cookie, cookiejar=None, overwrite=True)

                    return html

                # 下载验证码图片
                img_root = os.getcwd() + '/verification/'
                if not os.path.exists(img_root):
                    os.makedirs(img_root)
                img_path = img_root + 'verification_code_' + str(random.randint(1, 1000)) + '.png'
                # 获取验证码的url
                Html = etree.HTML(self.driver.page_source)
                verification_url = \
                    Html.xpath("/html/body/div/div[1]/div[3]/div/div/form/div[1]/div/div/div[1]/img")[0].attrib['src']
                # 下载验证码图片
                r = requests.get(verification_url, timeout=30, verify=False)
                with open(img_path, 'wb') as f:
                    f.write(r.content)
                # 识别验证码
                character = recognition_character(img_path)
                print('识别验证码：', character)

                # 验证码输入
                self.driver.find_element_by_xpath('//*[@id="captchacharacters"]').send_keys(character)
                # 必须等3秒才能验证
                time.sleep(3)
                # 验证
                self.driver.find_element_by_xpath('/html/body/div/div[1]/div[3]/div/div/form/div[2]/div/span/span/button').click()
            else:
                print('验证失败！')
                self.product_link_queue.put(product_link)
                return

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 通过requests请求数据
    def __request__(self, product_link):
        '''
        :param product_link: 商品链接
        :param product_attr_asin: 变体asin
        :return:
        '''
        try:
            headers = {
                "Host": "www.amazon.com",
                "Connection": "keep-alive",
                # "Connection": "close",
                "Upgrade-Insecure-Requests": "1",
                # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36" ,
                "User-Agent": get_useragent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "Referer": "www.amazon.com",
                "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                # "Cookie": cookie,
            }
            proxies = get_proxy()
            try:
                res = self.s.get(product_link, headers=headers, verify=False, timeout=30,proxies=proxies)
            except:
                count = 1
                while count <= 5:
                    try:
                        res = self.s.get(product_link, headers=headers,  verify=False, timeout=30,proxies=proxies)
                        break
                    except:
                        err_info = '__request__ reloading for %d time' % count if count == 1 else '__request__ reloading for %d times' % count
                        print(err_info)
                        count += 1
                if count > 5:
                    print("__request__ job failed!")
                    self.product_link_queue.put(product_link)
                    return

            html = res.text
            title = re.search(r'<title[ dir="ltr"]*>(.+)?</title>', res.text).group(1)

            return html, title, product_link

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 查询商品是否存在
    def __query_product__(self, productASIN):
        sql = 'select id from amazonshop_goods  WHERE ASIN = \'%s\'  ' % productASIN
        res1 = self.mysql.select(sql)
        sql = 'select id from amazonshop_deletedgoodasin  WHERE good_asin = \'%s\' ' % productASIN
        res2 = self.mysql.select(sql)
        if (res1 or res2):
            return True
        else:
            return False

    # 商品数据采集
    def clawer(self, product_link):
        try:
            print('正在爬取商品：', product_link)
            # 主体商品
            product_html, title, product_link = self.__request__(product_link)
            if title == 'Robot Check':
                print('Robot Check--1')
                product_html = self.get_character_by_ocr(product_link)
            elif title == 'Sorry! Something went wrong!':
                print('Sorry! Something went wrong!--1')
                self.product_link_queue.put(product_link)
                return

            product_info = self.__parseProduct__(product_html, product_link, asin='', product_attr_value='', type=1)
            if not product_info['productASIN']:
                self.product_link_queue.put(product_link)
                return
            # 查询库中是否有该商品的数据
            flag = self.__query_product__(product_info['productASIN'])
            if not flag:
                product_info = self.__save_img__(product_info)
                product_data = {1: product_info}
                if product_info['dimensionValuesDisplayData']:
                    dimensionValuesDisplayData = json.loads(product_info['dimensionValuesDisplayData'])
                    product_attr_data = []
                    # 遍历变体的asin和属性
                    for product_attr_asin, product_attr_value in dimensionValuesDisplayData.items():
                        # 变体商品
                        # time.sleep(random.randint(1,3))
                        # 拼接url
                        replace_str = 'dp/' + product_attr_asin + '/ref'
                        attr_product_link = re.sub(r'dp/(.+)/ref', replace_str, product_link) + '&th=1&psc=1'
                        product_html, title, attr_product_link = self.__request__(attr_product_link)
                        if title == 'Robot Check':
                            print('Robot Check--2')
                            product_html = self.get_character_by_ocr(attr_product_link)
                        elif title == 'Sorry! Something went wrong!':
                            print('Sorry! Something went wrong!--2')
                            self.product_link_queue.put(attr_product_link)
                            return

                        product_info = self.__parseProduct__(product_html, attr_product_link, product_attr_asin,product_attr_value, type=2)
                        if not product_info['productASIN']:
                            self.product_link_queue.put(product_link)
                            return
                        product_info = self.__save_img__(product_info)
                        product_attr_data.append(product_info)

                    product_data[2] = product_attr_data

                # 1代表主题商品，2代表变体商品
                self.product_info_queue.put(product_data)
            else:
                print('商品已存在：{ASIN:%s}' % product_info['productASIN'])

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 保存图片
    def __save_img__(self, product_info):
        '''
        :param product_info:
        :return:
        '''
        productASIN = product_info['productASIN']
        img_url = product_info['img_url']
        try:
            if img_url:
                dir = os.getcwd().replace('utils', '') + '/amazon1/amazon/amazon/static/media/img/' + productASIN + '/'

                if not os.path.exists(dir):
                    os.makedirs(dir)

                if product_info['product_attr_asin']:
                    img_dir = dir + product_info['product_attr_asin'] + '.jpg'
                else:
                    img_dir = dir + productASIN + '.jpg'

                try:
                    r = requests.get(img_url, timeout=30, verify=False)
                    with open(img_dir, 'wb') as f:
                        f.write(r.content)
                except:
                    count = 1
                    while count <= 5:
                        try:
                            r = requests.get(img_url, timeout=30, verify=False)
                            with open(img_dir, 'wb') as f:
                                f.write(r.content)
                            break
                        except:
                            err_info = '__save_img__ reloading for %d time' % count if count == 1 else '__save_img__ reloading for %d times' % count
                            print(err_info)
                            count += 1
                    if count > 5:
                        print("__save_img__ job failed!")
                        print(img_url)

                product_info['img_dir'] = '/static' + img_dir.split('static')[1]
            else:
                product_info['img_dir'] = ''

            return product_info

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    def run(self):
        try:
            print('启动：', self.threadName)
            while not flag_clawer:
                try:
                    product_link = self.product_link_queue.get(timeout=3)
                except:
                    time.sleep(3)
                    continue
                self.clawer(product_link)
            print('退出：', self.threadName)
            self.driver.quit()
            self.mysql.close()
        except Exception as err:
            print(err)


class ThreadParse(Thread):

    def __init__(self, i, user_id, product_info_queue, product_total, url, source):
        Thread.__init__(self)
        self.user_id = user_id
        self.source = source
        self.url = url
        self.product_total = product_total
        self.mysql = MysqlDb()
        self.threadName = '解析线程' + str(i)
        self.product_info_queue = product_info_queue

    # 将商品的排名信息写入排名表
    def __save_categorySalesRank__(self, productId, categorySalesRank, type):
        '''
        :param productId: 商品ID(goods表中id)
        :param categorySalesRank: 商品排名信息，list类型（[(排名1，类别1),(排名2，类别2)...]）
        :param type: type=1，保存主体商品排名；type=2，保存变体商品排名
        :return:
        '''
        try:
            if type == 1:
                sql = 'insert ignore into amazonshop_categoryrank (good_id, ranking, sort) values (%s, %s, %s)'
            elif type == 2:
                sql = 'insert ignore into amazonshop_attrcategoryrank (good_attr_id, ranking, sort) values (%s, %s, %s)'

            value = []
            for data in categorySalesRank:
                value.append((productId,) + data)
            self.mysql.insert(sql, value)

            # 把商品类别保存到类别表
            # 商品类别排序(从大到小)
            categorySalesRank.sort(key=lambda x: int(x[0]), reverse=True)
            i = 0
            for item in categorySalesRank:
                i += 1
                if i == 1:
                    # 商品的最大类别
                    sql = 'insert into amazonshop_mpttgood (title,lft,rght,tree_id,level,parent_id) SELECT  \"%s\",%s,%s,%s,%s,%s from dual WHERE  NOT EXISTS (SELECT id FROM amazonshop_mpttgood WHERE  title = \"%s\" ) ' \
                          % (item[1], 0, 0, 1, 1, 1, item[1])
                else:
                    # 商品的其它类别
                    sql = 'select id from amazonshop_mpttgood WHERE  title = \"%s\" ' % categorySalesRank[0][1]
                    id = self.mysql.select(sql)[0]['id']

                    sql = 'insert into amazonshop_mpttgood (title,lft,rght,tree_id,level,parent_id) SELECT  \"%s\",%s,%s,%s,%s,%s from dual WHERE  NOT EXISTS (SELECT id FROM amazonshop_mpttgood WHERE  title = \"%s\" ) ' \
                          % (item[1], 0, 0, 1, 2, id, item[1])

                cur = self.mysql.mysql.cursor()
                cur.execute(sql)
                cur.execute('commit')
                cur.close()

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 将属性及属性值信息写入属性表（属性分类表、属性分类值表）
    def __save_dimensions__(self, dimension, dimensionValues):
        '''
        :param dimension: 商品的属性名称（如color、size）,str类型
        :param dimensionValues: 商品的属性值（如color的属性值有red、black、white）,list类型([])
        :return: 返回属性值的id（属性分类值表的id），list类型
        '''
        try:
            if 'Size' in dimension:
                export_name = 'size'
            elif 'Color' in dimension:
                export_name = 'color'
            elif 'Length' in dimension:
                export_name = 'size'
            elif 'Width' in dimension:
                export_name = 'size'
            elif 'Height' in dimension:
                export_name = 'size'
            else:
                export_name = ''

            # 写入属性信息
            sql = 'insert into amazonshop_attrcategory (attr_name,export_name) select \"%s\",\"%s\" from dual WHERE NOT  EXISTS  (SELECT id from amazonshop_attrcategory WHERE attr_name = \"%s\" ) ' % (
                dimension, export_name, dimension)
            cur = self.mysql.mysql.cursor()
            cur.execute(sql)
            cur.execute('commit')

            # 写入属性值信息
            sql = 'SELECT id FROM amazonshop_attrcategory WHERE attr_name = \"%s\" ' % dimension
            attr_id = self.mysql.select(sql)[0]['id']
            value = [(attr_id, attr_value) for attr_value in dimensionValues]
            sql = 'insert ignore into amazonshop_attrcategoryvalue (attrcategory_id, attr_value) values (%s,%s)'
            self.mysql.insert(sql, value)

            # 关闭游标
            cur.close()
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)
            traceback.print_exc()

    # 将属性值组合信息写入商品属性表
    def __save_dimensionValues__(self, productId, asin, product_info):
        '''
        :param productId: 商品ID（goods表中的id）
        :param product_info: 商品变体的信息，dict类型
        :param asin: 变体的asin编码
        :return:
        '''
        try:
            attr_tuple = ()
            for attr_value in product_info['attr_value']:
                sql = 'select id from amazonshop_attrcategoryvalue where attr_value = \"%s\" ' % attr_value
                id = self.mysql.select(sql)[0]['id']
                attr_tuple += (id,)

            # 保存商品变体信息
            sql = 'insert ignore into amazonshop_goodsattr (good_attr,good_id,ASIN,brand_name,seller_volume,comment_volume,grade_star,product_name,price,upload_date,selling_point,product_description,img_url,img_dir,good_url,source_id,IsCar,FBA) values ' \
                  '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            value = [
                (str(attr_tuple), productId, asin, product_info['brand_name'], product_info['productSellerNumbers'],
                 product_info['comment_volume'], product_info['grade_star'], product_info['product_name'],
                 product_info['price'], product_info['upload_date'], product_info['selling_point'],
                 product_info['product_description'], product_info['img_url'], product_info['img_dir'],
                 product_info['product_link'], self.source, product_info['product_cart'], product_info['product_FBA'])]

            self.mysql.insert(sql, value)
            # 获取变体商品ID
            sql = 'select id from amazonshop_goodsattr WHERE ASIN = \'%s\' ' % asin
            good_attr_id = self.mysql.select(sql)[0]['id']

            # 更新主体商品是否有FBA
            sql = 'select  id  from amazonshop_goods WHERE id = %s  AND  FBA is NULL ' % productId
            res = self.mysql.select(sql)

            if product_info['product_FBA'] and res:
                sql = 'update amazonshop_goods set FBA = %s WHERE id = %s '
                value = [(1, productId)]
                self.mysql.update(sql, value)

            # 更新主体商品是否有购物车
            sql = 'select  id  from amazonshop_goods WHERE id = %s  AND  IsCar is NULL ' % productId
            res = self.mysql.select(sql)

            if product_info['product_cart'] and res:
                sql = 'update amazonshop_goods set IsCar = %s WHERE id = %s '
                value = [(1, productId)]
                self.mysql.update(sql, value)

            return good_attr_id

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 将商品信息写入商品表
    def __save_productInfo__(self, product_info, user_id):
        '''
        :param product_info: 商品信息
        :return: 返回商品ID
        '''
        try:
            sql = 'insert ignore into amazonshop_goods (ASIN,brand_name,seller_volume,comment_volume,grade_star,product_name,price,upload_date,selling_point,product_description,user_id,img_url,img_dir,good_url,source_id) values ' \
                  '(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            value = [(product_info['productASIN'], product_info['brand_name'], product_info['productSellerNumbers'],
                      product_info['comment_volume'], product_info['grade_star'], product_info['product_name'],
                      product_info['price'], product_info['upload_date'], product_info['selling_point'],
                      product_info['product_description'], user_id, product_info['img_url'],
                      product_info['img_dir'], product_info['product_link'], self.source)]

            self.mysql.insert(sql, value)
            sql = 'select id from amazonshop_goods WHERE  ASIN = \'%s\' ' % product_info['productASIN']
            productId = self.mysql.select(sql)[0]['id']
            return productId

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 保存商品数据
    def __save_data__(self, product_info, productId, asin, type):
        '''
        :param product_info: 商品信息
        :param productId: 商品ID
        :param asin: 变体asin
        :param type: 存储类型（type=1，存储主体商品；type=2，存储变体商品）
        :return:
        '''
        try:

            if type == 1:
                # 保存商品信息，并返回商品id
                productId = self.__save_productInfo__(product_info, self.user_id)

                # 保存商品的分类排名信息
                categorySalesRank = product_info['categorySalesRank']
                self.__save_categorySalesRank__(productId, categorySalesRank, type)

                # 保存商品的属性信息
                # 属性名称对应的标签（例如：color对应color_name,size对应size_name）
                variationDisplayLabels = product_info['variationDisplayLabels']
                # 根据属性标签提取属性对应的值（例如：标签color_name对应属性color，其属性对应的值有black、white... ）
                variationValues = product_info['variationValues']
                if variationDisplayLabels:
                    for labal_name, dimension in variationDisplayLabels.items():
                        dimensionValues = variationValues[labal_name]
                        self.__save_dimensions__(dimension, dimensionValues)
                return productId

            elif type == 2:

                # 保存商品的属性值组合信息
                good_attr_id = self.__save_dimensionValues__(productId, asin, product_info)

                # 保存商品的分类排名信息
                categorySalesRank = product_info['categorySalesRank']
                self.__save_categorySalesRank__(good_attr_id, categorySalesRank, type)


        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    def __save_process__(self, num):

        # 更新数据库的采集进度
        sql = 'update amazonshop_usershopsurl set sum = %s, num = %s WHERE shop_url = %s'
        self.mysql.update(sql, [(self.product_total, num, self.url)])

        # 更新当前的采集进度（web展示）
        content = {"shop_url": self.url, "total": self.product_total, "number": num, "user_id": self.user_id}
        file_root = os.getcwd().replace('utils', '') + '/amazon1/amazon/amazon/static/file/'
        if not os.path.exists(file_root):
            os.makedirs(file_root)
        file_path = file_root + 'process.json'
        with open(file_path, 'w', encoding='utf-8') as json_file:
            json.dump(content, json_file, ensure_ascii=False)
        pass

    def run(self):

        try:
            print('启动：', self.threadName)
            while not flag_parse:
                try:
                    products_info = self.product_info_queue.get(timeout=3)
                except:
                    time.sleep(3)
                    continue
                # 主题商品
                product_info = products_info[1]
                if product_info['productASIN']:
                    productId = self.__save_data__(product_info, productId='', asin='', type=1)
                    print('写入商品：', product_info['productASIN'])
                    if len(products_info) > 1:
                        # 变体商品
                        products_attr_info = products_info[2]
                        seller_num_total = 0
                        for product_attr_info in products_attr_info:
                            # 卖家数
                            seller_num = int(product_attr_info['productSellerNumbers'])
                            seller_num_total += seller_num
                            product_attr_asin = product_attr_info['product_attr_asin']
                            self.__save_data__(product_attr_info, productId, product_attr_asin, type=2)

                        # 更新卖家数量
                        sql = 'update amazonshop_goods set seller_volume = %s WHERE id = %s '
                        self.mysql.update(sql, [(seller_num_total, productId)])

                    # 保存采集进度
                    global num
                    num += 1
                    self.__save_process__(num)

            print('退出：', self.threadName)
            self.mysql.close()

        except Exception as err:
            print(err)


class GetAllProductsLink(object):

    def __init__(self, url, product_link_queue):
        '''
        :param url: 店铺链接
        :param product_link_queue: 商品链接队列
        '''
        self.url = url
        self.product_link_queue = product_link_queue
        self.s = requests.session()

        # 代理服务器
        proxyHost = "http-dyn.abuyun.com"
        proxyPort = "9020"

        # 代理隧道验证信息
        proxyUser = "H19SC127905HL13D"
        proxyPass = "7942D1D850E8D5C5"

        cap = DesiredCapabilities.PHANTOMJS.copy()  # 使用copy()防止修改原代码定义dict
        service_args = [
            '--ssl-protocol=any',  # 任何协议
            '--cookies-file=False',  # cookies文件
            '--disk-cache=no',  # 不设置缓存
            # '--ignore-ssl-errors=true'  # 忽略https错误

            "--proxy-type=http",
            "--proxy=%(host)s:%(port)s" % {
                "host": proxyHost,
                "port": proxyPort,
            },
            "--proxy-auth=%(user)s:%(pass)s" % {
                "user": proxyUser,
                "pass": proxyPass,
            },
        ]

        headers = {
            "Host": "www.amazon.com",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
            "User-Agent": get_useragent(),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "Referer": "www.amazon.com",
            # "Accept-Encoding": "gzip, deflate, br",
            # "Accept-Language": "zh-CN,zh;q=0.9",
        }
        for key, value in headers.items():
            cap['phantomjs.page.customHeaders.{}'.format(key)] = value

        self.driver = webdriver.PhantomJS(executable_path="/home/phantomjs-2.1.1-linux-x86_64/bin/phantomjs",desired_capabilities=cap, service_args=service_args)
        # self.driver = webdriver.PhantomJS(desired_capabilities=cap, service_args=service_args)

        # 设置页面最大加载时间
        self.driver.set_page_load_timeout(60)

    # 打开当前页面，返回当前页面的商品列表
    def __getProductlink__(self, url):
        try:
            time.sleep(random.random())
            html_source, title, store_link = self.__request__(url)
            html = etree.HTML(html_source)
            products_list = html.xpath('//div[contains(@class,"sg-col-4-of-24 sg-col-4-of-12 sg-col-4-of-36 s-result-item sg-col-4-of-28 sg-col-4-of-16 sg-col sg-col-4-of-20 sg-col-4-of-32")]')
            return products_list, html, title
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 获取当前页面每件商品的链接
    def __clawer__(self, url):
        '''
        :param url: 店铺当前页的url
        :return: 返回当前页面的下一页链接
        '''
        try:
            # 获取产品列表（网站打开默认是中文，要转换成英文）
            url = url + '&language=en_US'
            products_list, html, title = self.__getProductlink__(url)
            if title == 'Robot Check':
                print('Robot Check')
                products_list, html = self.get_character_by_ocr(url)
            elif title == 'Sorry! Something went wrong!':
                print('Sorry! Something went wrong!--GetLink')
                return 'Sorry'

            next_url = self.__getNextPage__(html)
            for product in products_list:
                protuct_link = 'https://www.amazon.com' + str(product.xpath('./div/div/div/div[2]/div[1]/div/div/span/a/@href')[0]) + '&language=en_US'
                self.product_link_queue.put(protuct_link)

            return next_url

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 获取下一页链接
    def __getNextPage__(self, html):
        try:
            # 获取下一页url
            next_ = html.xpath('//a[contains(text(),"Next")]/@href')
            if next_:
                next_url = 'https://www.amazon.com' + str(html.xpath('//a[contains(text(),"Next")]/@href')[0])
                return next_url
            else:
                # 下一页不存在
                return False
        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 通过requests请求数据
    def __request__(self, store_link):
        try:

            headers = {
                "Host": "www.amazon.com",
                "Connection": "keep-alive",
                # "Connection": "close",
                "Upgrade-Insecure-Requests": "1",
                # "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36" ,
                "User-Agent": get_useragent(),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                # "Referer": "www.amazon.com",
                # "Accept-Encoding": "gzip, deflate, br",
                "Accept-Language": "zh-CN,zh;q=0.9",
                # "Cookie": 'i18n-prefs=USD; sp-cdn="L5Z9:HK"; x-wl-uid=1AN7TJNCV5Qcv++n7laTmt5KmoDqjJ534vOHCKWbNTSBe/uhX6z8Qwh5iDFms/D1sK3+oUCEagSo=; session-token=6MKPLKJmD/nFzNC8TeByqDOS2QerdZVfaZt4JpWonba0lKHVQyTn8Zq9NDUd16rKlhqN9gxhRd41GVAOnA+tW+Erf7HN+3u4mjGBtE4SXfzECwD2vsnu2zaCyf2n8fnDQVoFHsFXVOrPW0ZW3AIZ2I7VGhlKtEF8G6EGYg0Gsu+29Gumo4UpZe8SQrGYMra3; lc-main=en_US; ubid-main=133-3456038-7774832; session-id-time=2082787201l; session-id=136-8826518-8862022; csm-hit=tb:07S1KS9YNPEVEMD7CV35+s-N5ETT1RAPQ2J8Z9YGC73|1566893869614&t:1566893869614&adb:adblk_no',
            }

            proxies = get_proxy()

            try:

                res = self.s.get(store_link, headers=headers, verify=False, timeout=30, proxies=proxies)
            except:
                count = 1
                while count <= 5:
                    try:
                        res = self.s.get(store_link, headers=headers, verify=False, timeout=30, proxies=proxies)
                        break
                    except:
                        err_info = '__request__ reloading for %d time' % count if count == 1 else '__request__ reloading for %d times' % count
                        print(err_info)
                        count += 1
                if count > 5:
                    print("__request__ job failed!")
                    return

            html = res.text
            title = re.search(r'<title[ dir="ltr"]*>(.+)?</title>', res.text).group(1)

            return html, title, store_link

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            traceback.print_exc()

    # 识别验证码(baidu_ocr)
    def get_character_by_ocr(self, product_link):
        '''
        :param product_link: 商品链接
        :return:
        '''

        self.driver.get(product_link)

        try:
            print('出现验证码，正在验证！')
            # 最多进行20次验证
            for i in range(20):
                try:
                    element = self.driver.find_element_by_xpath('/html/body/div/div[1]/div[3]/div/div/form/div[1]/div/div/div[1]')
                except:
                    cookie = {}
                    for cookie_data in self.driver.get_cookies():
                        cookie[cookie_data['name']] = cookie_data['value']

                    print('验证通过！')
                    html = self.driver.page_source
                    # 把cookie值转换为cookiejar类型，然后传给Session
                    self.s.cookies = requests.utils.cookiejar_from_dict(cookie, cookiejar=None,overwrite=True)

                    try:
                        products_list = html.xpath('//div[contains(@class,"sg-col-4-of-24 sg-col-4-of-12 sg-col-4-of-36 s-result-item sg-col-4-of-28 sg-col-4-of-16 sg-col sg-col-4-of-20 sg-col-4-of-32")]')
                    except:
                        products_list = []

                    return products_list, html

                # 下载验证码图片
                img_root = os.getcwd() + '/verification/'
                if not os.path.exists(img_root):
                    os.makedirs(img_root)
                img_path = img_root + 'verification_code_' + str(random.randint(1, 1000)) + '.png'
                # 获取验证码的url
                Html = etree.HTML(self.driver.page_source)
                verification_url = \
                    Html.xpath("/html/body/div/div[1]/div[3]/div/div/form/div[1]/div/div/div[1]/img")[0].attrib[
                        'src']
                # 下载验证码图片
                r = requests.get(verification_url, timeout=30, verify=False)
                with open(img_path, 'wb') as f:
                    f.write(r.content)
                # 识别验证码
                character = recognition_character(img_path)
                print('识别验证码：', character)

                # 验证码输入
                self.driver.find_element_by_xpath('//*[@id="captchacharacters"]').send_keys(character)
                # 必须等3秒才能验证
                time.sleep(3)
                # 验证
                self.driver.find_element_by_xpath(
                    '/html/body/div/div[1]/div[3]/div/div/form/div[2]/div/span/span/button').click()
            else:
                print('验证失败！')
                return

        except Exception as err:
            mylog.logs().exception(sys.exc_info())
            print(err)
            traceback.print_exc()

    def run(self):
        '''
        :return: 返回店铺的所有商品链接
        '''
        # 店铺总链接（默认店铺的第一页链接）
        # print('正在爬取店铺：', self.url)
        next_url = self.url
        # 循环遍历店铺的所有商品页
        for i in range(1000):
            if next_url:
                if next_url == 'Sorry':
                    print('获取链接失败！')
                    break
                print('正在获取该页面下的所有商品链接：', next_url)
                next_url = self.__clawer__(next_url)

            else:
                self.driver.quit()
                break


def update_process():
    # 更新当前的采集进度（web展示）
    content = {}
    file_root = os.getcwd().replace('utils', '') + '/amazon1/amazon/amazon/static/file/'
    if not os.path.exists(file_root):
        os.makedirs(file_root)
    file_path = file_root + 'process.json'
    with open(file_path, 'w', encoding='utf-8') as json_file:
        json.dump(content, json_file, ensure_ascii=False)
    pass


def get_proxy():
    # 代理服务器
    proxyHost = "http-dyn.abuyun.com"
    proxyPort = "9020"

    # 代理隧道验证信息
    proxyUser = "H19SC127905HL13D"
    proxyPass = "7942D1D850E8D5C5"

    proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
        "host": proxyHost,
        "port": proxyPort,
        "user": proxyUser,
        "pass": proxyPass,
    }

    proxies = {
        "http": proxyMeta,
        "https": proxyMeta,
    }

    return proxies


def get_useragent():
    useragent_list = [
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.101 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2125.122 UBrowser/4.0.3214.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
        "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
    ]
    return random.choice(useragent_list)
    pass


# 采集是否完成的标志
flag_clawer = False
# 解析是否完成的标志
flag_parse = False
# 创建日志
mylog = Mylog('clawer_amazon_a')
# 商品采集数
num = 0


def main(url, user_id, source):
    # 商品链接队列
    product_link_queue = Queue()
    # 商品信息队列
    product_info_queue = Queue()

    get_all_products_link = GetAllProductsLink(url, product_link_queue)
    get_all_products_link.run()

    # 商品总数
    product_total = product_link_queue.qsize()

    if not product_link_queue.empty():
        print('已获取店铺所有商品的链接！')

        # 存储3个采集线程的列表集合
        threadcrawl = []
        for i in range(3):
            thread = ThreadClawerAmazon(i, product_link_queue, product_info_queue, user_id)
            thread.start()
            threadcrawl.append(thread)

        # 存储1个解析线程
        threadparse = []
        for i in range(1):
            thread = ThreadParse(i, user_id, product_info_queue, product_total, url, source)
            thread.start()
            threadparse.append(thread)

        # 等待队列为空，采集完成
        while not product_link_queue.empty():
            pass
        else:
            global flag_clawer
            flag_clawer = True

        for thread in threadcrawl:
            thread.join()

        # 等待队列为空，解析完成
        while not product_info_queue.empty():
            pass
        else:
            global flag_parse
            flag_parse = True

        for thread in threadparse:
            thread.join()

        # 更新采集进程，web显示进度
        update_process()

        print('数据采集完成！')
        flag_clawer = False
        flag_parse = False

        return  True
    else:
        print('数据采集失败！')
        return False

if __name__ == '__main__':
    # url = 'https://www.amazon.com/s?rh=n%3A7141123011%2Cp_4%3Augyly&ref=w_bl_sl_s_ap_web_7141123011' # 11页
    # url = 'https://www.amazon.com/s?rh=n%3A7141123011%2Cp_4%3ACoCosly&ref=w_bl_sl_s_ap_web_7141123011'  # 6页
    # url = 'https://www.amazon.com/s?rh=n%3A7141123011%2Cp_4%3ATOP-KKT&ref=w_bl_sl_s_ap_web_7141123011' # 3页
    url = 'https://www.amazon.com/s?rh=n%3A7141123011%2Cp_4%3ASANHUI&ref=w_bl_sl_s_ap_web_7141123011'  # 8页

    main(url, user_id=1, source=1)

