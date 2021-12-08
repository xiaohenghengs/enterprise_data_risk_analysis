import functools

import orangecontrib.associate.fpgrowth as oaf  # 进行关联规则分析的包


class Association:
    def __init__(self, analysis_list, support_rate, confidence_rate):
        self.analysis_list = analysis_list
        self.support_rate = support_rate
        self.confidence_rate = confidence_rate
        self.final_result = []
        strSet = set(functools.reduce(lambda a, b: a + b, self.analysis_list))
        self.encode_str = dict(zip(strSet, range(len(strSet))))  # 编码字典
        self.decode_str = dict(zip(self.encode_str.values(), self.encode_str.keys()))  # 解码字典

    def numerization(self):
        """
        数值化列表字段
        :return: 数值化之后的全部列表数据
        """
        return [list(map(lambda item: self.encode_str[item], row)) for row in self.analysis_list]

    def generateRules(self):
        """
        生成规则
        :return: 规则列表
        """
        itemSets = dict(oaf.frequent_itemsets(self.numerization(), self.support_rate))
        return list(oaf.association_rules(itemSets, self.confidence_rate))

    def generateResult(self):
        """
        生成全部结果
        :return:
        """
        numerization_list = self.numerization()
        itemSets = dict(oaf.frequent_itemsets(numerization_list, self.support_rate))
        return list(oaf.rules_stats(self.generateRules(), itemSets, len(numerization_list)))

    def filteringAndDecodingResult(self, item, rule_len=None, target_len=None):
        """
        过滤和解码规则数据
        :param item:数据
        :param rule_len:最小规则数量
        :param target_len: 最大推出数量
        :return: XXX&XXX ==> xxx
        """
        rules = item[0]
        targets = item[1]
        if rule_len and len(rules) < rule_len:
            return None
        elif target_len and len(targets) > target_len:
            return None
        else:
            rules_list = self.decode(rules)
            targets_list = self.decode(targets)
            result = [str.join('&', rules_list), str.join('&', targets_list), item[2], item[3], item[4],
                      item[6], item[7]]
            self.final_result.append(result)
            return result

    def decode(self, encode_list):
        """解码"""
        result_list = []
        for encode_str in encode_list:
            result_list.append(self.decode_str[encode_str])
        return result_list
