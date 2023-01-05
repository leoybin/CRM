from erp2crmsaleout.saleOutToDMS import ERP2CRM
from erp2crmsaleout.salesOutStock import get_data

if __name__ == '__main__':
    acc = ERP2CRM()
    acc.ERP2DMS()
    get_data()
