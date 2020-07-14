import time

nowtime = int(time.time())

def fivemins():
    return int(time.time()-(5 * 60))

def tenmins():
    return int(time.time()-(10 * 60))

def fifteenmins():
    return int(time.time()-(15 * 60))


print(nowtime)
print(fivemins(), tenmins(), fifteenmins())