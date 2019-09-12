import time,os,json,datetime,re
# import BL_ThreadClawerAmazon_A as clawer_amazon_a
# import BL_ThreadClawerAmazon_A_Proxy as clawer_amazon_a
import BL_ThreadClawerAmazon_A_Request as clawer_amazon_a
# import BL_ThreadClawerAmazon_B as clawer_amazon_b
# import BL_ThreadClawerAmazon_B_Proxy as clawer_amazon_b
import BL_ThreadClawerAmazon_B_Request as clawer_amazon_b
import BL_ThreadClawerAliExpress_Request as clawer_aliexpress
import BL_ThreadClawerWish as clawer_wish
import BL_ThreadClawerEbay as clawer_ebay
from mysql_utils.mysql_db import MysqlDb
import threading
from threading import Thread
from  queue import Queue



class QueryAmazonShopUrl(Thread):

    def __init__(self, shopurl_queue):
        Thread.__init__(self)
        self.mysql = MysqlDb()
        self.threadName = '爬虫程序'
        self.shopurl_queue = shopurl_queue
        pass

    def update_status(self, shopurl,flag):
        sql = 'update amazonshop_usershopsurl set flag = %s WHERE shop_url = %s '
        self.mysql.update(sql,[(flag,shopurl)])


    def __get_source_id__(self,source):

        if source == 'amazon':
            source = '亚马逊'
        elif source == 'aliexpress':
            source = '速卖通'
        elif source == 'wish':
            source = 'Wish'
        elif source == 'ebay':
            source = 'Ebay'

        sql = 'insert into amazonshop_source (source_name) SELECT  \"%s\" FROM dual WHERE NOT EXISTS (SELECT  id FROM  amazonshop_source WHERE source_name = \"%s\" ) ' %(source,source)
        cur = self.mysql.mysql.cursor()
        cur.execute(sql)
        cur.execute('commit')
        cur.close()

        sql = 'select id from amazonshop_source WHERE source_name = \"%s\" ' % source
        source_id = self.mysql.select(sql)[0]['id']

        return source_id


    def run(self):

        print('启动：', self.threadName)

        while not flag_clawer:
            try:
                shopurl_data = self.shopurl_queue.get()
                shopurl = shopurl_data[0]
                user_id = shopurl_data[1]
                source = shopurl_data[2]
                print('正在爬取店铺：',shopurl)
                self.update_status(shopurl, flag=2)
                if source == 'amazon':
                    source_id = self.__get_source_id__(source)
                    flag = clawer_amazon_a.main(shopurl,user_id, source_id)
                    if not flag:
                        clawer_amazon_b.main(shopurl,user_id, source_id)
                elif source == 'aliexpress':
                    source_id = self.__get_source_id__(source)
                    clawer_aliexpress.main(shopurl, user_id, source_id)
                elif source == 'wish':
                    source_id = self.__get_source_id__(source)
                    clawer_wish.main(shopurl, user_id, source_id)
                elif source == 'ebay':
                    source_id = self.__get_source_id__(source)
                    clawer_ebay.main(shopurl, user_id, source_id)
                else:
                    print('无法采集该店铺：', shopurl)
                self.update_status(shopurl, flag=1)
            except:
                time.sleep(1)

        print('退出：', self.threadName)
        self.mysql.close()


flag_clawer = False


# 每五分钟获取一次数据库中待采集的店铺链接
def query_url(shopurl_queue):
    sql = 'select shop_url,user_id from amazonshop_usershopsurl WHERE  flag = 0 '
    mysql = MysqlDb()
    res = mysql.select(sql)

    if res:
        for data in res :
            shop_url = data['shop_url']
            user_id = data['user_id']
            source = re.search(r'[https://www]*\.(.+)\.[comn]+', shop_url)
            if source:
                source = source.group(1)
                shopurl_queue.put([shop_url, user_id, source])

    mysql.close()
    t = threading.Timer(300, query_url,[shopurl_queue])
    t.start()


def main():

    shopurl_queue = Queue()
    query_url(shopurl_queue)

    while 1:

        global flag_clawer
        flag_clawer = False

        print(datetime.datetime.now())
        if not shopurl_queue.empty():

            # 存储1个线程
            threadcrawl = []
            for i in range(1):
                thread = QueryAmazonShopUrl(shopurl_queue)
                thread.start()
                threadcrawl.append(thread)

            # 等待队列为空，采集完成
            while not shopurl_queue.empty():
                pass
            else:
                flag_clawer = True

            for thread in threadcrawl:
                thread.join()

            print('结束！')

        else:
            print('没有待采集的店铺链接！')
            time.sleep(300)

    pass



if __name__ == '__main__':
    main()