class Index(object):

    def __init__(self, arg, **options):
        if isinstance(arg, basestring):
            arg = [(arg, 1)]
        self.arg = []
        for a in arg:
            if isinstance(a, basestring):
                self.arg.append((a, 1))
            else:
                self.arg.append(a)
        self.options = options

    def create(self, collection):
        return collection.create_index(self.arg, **self.options)
