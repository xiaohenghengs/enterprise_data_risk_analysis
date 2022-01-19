import numpy
from intervals import FloatInterval


class DynamicRangeQuantile:
    def __init__(self, data_array, quantile_num, range_num=None, frozen_top_quantiles=None,
                 frozen_bottom_quantiles=None):
        """
        动态极差分位方法
        :param data_array:目标数据集合
        :param quantile_num:百分比分位数
        :param range_num:极差分位数
        :param frozen_top_quantiles:冻结前 n 个分位数集合
        :param frozen_bottom_quantiles:冻结最后 n 个分位数集合
        """
        # ①获取数列 n 分位时的分位百分比
        percent = self.getSteps(0, 100, quantile_num)
        # ②去除 0%
        percent = percent[1:]
        # ③获取数列 n 分位数
        quantiles = [numpy.percentile(data_array, x) for x in percent]
        intervals = list()
        # ④获取数列 n 分位区间
        for i in range(len(quantiles) - 1):
            next_item = i + 1
            if i == 0:
                intervals.append(FloatInterval.closed(0, quantiles[i]))
                intervals.append(FloatInterval.open_closed(quantiles[i], quantiles[next_item]))
            else:
                intervals.append(FloatInterval.open_closed(quantiles[i], quantiles[next_item]))
        if range_num:
            # ⑤如果有极差分位需求，计算极差分位
            top_intervals = list()
            bottom_intervals = list()
            if frozen_top_quantiles and frozen_top_quantiles != 0:
                # 冻结分位区间前 x 个区间
                top_intervals = intervals[0: frozen_top_quantiles]
                intervals = intervals[frozen_top_quantiles:]
            if frozen_bottom_quantiles and frozen_bottom_quantiles != 0:
                # 冻结分位区间后 x 个区间
                bottom_intervals = intervals[-frozen_bottom_quantiles:]
                intervals = intervals[:-frozen_bottom_quantiles]
            min_value = intervals[0].lower
            max_value = intervals[-1].upper
            # 剩余数列进行 j 个极差分位
            range_intervals = self.rangeIntervals(min_value, max_value, range_num)
            self.quantile = top_intervals + range_intervals + bottom_intervals
        else:
            self.quantile = intervals

    def rangeIntervals(self, min_value, max_value, num):
        """
        极差分位
        :param min_value:最小值
        :param max_value: 最大值
        :param num: 分位数
        :return: 分位区间
        """
        quantiles = self.getSteps(min_value, max_value, num)
        intervals = list()
        for i in range(len(quantiles) - 1):
            intervals.append(FloatInterval.open_closed(quantiles[i], quantiles[i + 1]))
        return intervals

    @staticmethod
    def getSteps(begin, end, num):
        step = (end - begin) / num
        steps = list()
        while True:
            steps.append(begin if begin < end else end)
            if begin >= end:
                break
            else:
                begin += step
        return steps

    def intervalNum(self, value):
        for index, interval in enumerate(self.quantile):
            if value in interval:
                return index
