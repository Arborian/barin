import six


class Index(object):

    def __init__(self, arg, **options):
        if isinstance(arg, six.string_types):
            arg = [(arg, 1)]
        self.arg = []
        for a in arg:
            if isinstance(a, six.string_types):
                self.arg.append((a, 1))
            else:
                self.arg.append(a)
        self.options = options

    def create(self, collection):
        return collection.create_index(self.arg, **self.options)
