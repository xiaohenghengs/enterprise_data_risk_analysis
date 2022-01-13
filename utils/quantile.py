import numpy
from intervals import FloatInterval


class Quantile:

    def __init__(self, data_list):
        """
        处理数列的分位
        :param data_list:
        """
        data_list = [float(x) for x in data_list]
        self.array = numpy.array(data_list)
        self.decileIntervalList = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def median(self):
        """
        中位数
        :return:数列的中位数
        """
        return numpy.median(self.array)

    def percentile(self, percent):
        """
        分位数
        :param percent: 分位百分比
        :return:数列的百分比位数
        """
        return numpy.percentile(self.array, percent)

    def decile(self):
        """
        十分位值
        :return: 数列的0%分位，10%分位，20%分位...90%分位，100%分位
        """
        return [self.percentile(i * 10) for i in range(11)]

    def decileInterval(self):
        """
        十分位区间
        :return: 数列的十分位区间
        """
        result = []
        numbers = self.decile()
        for i in range(len(numbers) - 1):
            next_index_i = i + 1
            if i == 0:
                result.append(FloatInterval.closed(numbers[i], numbers[next_index_i]))
            else:
                result.append(FloatInterval.open_closed(numbers[i], numbers[next_index_i]))
        self.decileIntervalList = result
        return result

    def decileIntervalNum(self, target_number):
        """
        十分位区间数
        :return: 数列的十分位区间数
        """
        if self.decileIntervalList:
            decile_interval = self.decileIntervalList
        else:
            decile_interval = self.decileInterval()
        for index, interval in enumerate(decile_interval):
            if target_number in interval:
                return index
