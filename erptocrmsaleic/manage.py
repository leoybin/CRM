from erptocrmsaleic.saleicSaveCrm import get_data
from erptocrmsaleic.salesicToCRM import ERP2CRM

if __name__ == '__main__':
    acc = ERP2CRM()
    acc.ERP2DMS()
    get_data()
