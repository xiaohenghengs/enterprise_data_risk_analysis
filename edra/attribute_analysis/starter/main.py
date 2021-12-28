import sys

sys.path.append(r'../../../../enterprise_data_risk_analysis')
from edra.attribute_analysis.core.aggregation import Aggregation

if __name__ == '__main__':
    Aggregation(300000, 10000).getItems()
