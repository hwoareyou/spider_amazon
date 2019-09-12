from aip import AipOcr
import re

def recognition_character(img_url):
    """ 创建应用后获取的APPID、AK、SK """
    APP_ID = '16675686'
    API_KEY = 'zVxKtmF1mjjWO5m9oTrqkHlv'
    SECRET_KEY = 'tWZgTGiGGkUBxzaDWYY5g9OltwNRZXLD'

    # 传入参数，生成对象
    client = AipOcr(APP_ID, API_KEY, SECRET_KEY)

    # 以二进制方式读取图片内容
    with open(img_url,"rb") as f:
        image=f.read()

    # 调用通用文字识别接口
    data=client.basicGeneral(image)
    data=str(data)  # 将接口返回数据转为字符串，便于后续正则处理

    # 定义正则，提取识别的文字内容
    pat=re.compile(r"{'words': '(.*?)'}")
    result=pat.findall(data)


    # 遍历输出图片中的每一行文字内容
    # for rs in result:
    #     print(rs)

    return result

if __name__ == '__main__':
    img_url = '9.png'
    character = recognition_character(img_url)
    print(character)