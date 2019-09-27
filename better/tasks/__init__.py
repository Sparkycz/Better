
import abc


class HtmlObject():
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod
    def parse(self, sub_object):
        pass

    @staticmethod
    def get_text(path):
        try:
            value = path.text
            value = value.replace("\n", "")
            value = value.replace("\r", "")
            value = value.replace("\t", "")
            return value
        except AttributeError:
            return None