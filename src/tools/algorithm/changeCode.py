def encode(s):
    """
    将html编码，方便存入sql server数据库
    :param s: 待编码的html串
    :return: 编码后的html串
    """
    return ' '.join([bin(ord(c)).replace('0b', '') for c in s])


def decode(s):
    """
    将html串解码，方便显示
    :param s: 待解码的html串
    :return: 解码后的html串
    """
    return ''.join([chr(i) for i in [int(b, 2) for b in s.split(' ')]])