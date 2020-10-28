def parse(obj):
    import random,json,time
    obj=json.loads(obj)
    num=obj['num']
#     time.sleep(random.randint(1,10))
    return {'num':num, 'num_rand':random.random() * num}
