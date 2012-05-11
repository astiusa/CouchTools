class simpleuuid(object):
    def __init__(self):
        import random
        self.hex = ''
        for x in range(1,32):
            self.hex += random.choice('abcde0123456789')

def uuid():
    return simpleuuid()
