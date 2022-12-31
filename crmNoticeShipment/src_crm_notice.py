import datetime
from urllib import parse
from pymssql import ProgrammingError
import pandas as pd
from pyrda.dbms.rds import RdClient
from sqlalchemy import create_engine
import string

import pymssql

bad_password1 = 'rds@2022'
conn2 = {'DB_USER': 'dms',
         'DB_PASS': parse.quote_plus(bad_password1),
         'DB_HOST': '115.159.201.178',
         'DB_PORT': 1433,
         'DATABASE': 'cprds',
         }
bad_password2 = 'lingdangcrm123!@#'
conn3 = {'DB_USER': 'lingdang',
         'DB_PASS': parse.quote_plus(bad_password2),
         'DB_HOST': '123.207.201.140',
         'DB_PORT': 33306,
         'DATABASE': 'ldcrm',
         }


class CrmToDms():
    # 出库
    def __init__(self):
        # 连接数据库

        crm_conn = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(conn3['DB_USER'], conn3['DB_PASS'],
                                                                        conn3['DB_HOST'],
                                                                        conn3['DB_PORT'], conn3['DATABASE'])
        self.new_con = pymssql.connect(host='115.159.201.178', database='cprds', user='dms', port=1433,
                                       password='rds@2022', charset='utf8')
        self.new_cursor = self.new_con.cursor()
        self.crm_engine = create_engine(crm_conn)

        # self.new_con = pymssql.connect(host='115.159.201.178', database='cprds', user='dms', port=1433,
        #                                password='rds@2022', charset='utf-8')
        # self.new_cursor = self.new_con.cursor()

    def get_sale_out(self):
        sql = """
        select FSaleorderno,FDelivaryNo,FBillTypeIdName,Fdeliverystatus,Fdeliverydate,Fstock,FCustId,FCustName,
        FprNumber,FName,Fcostprice,FPrice,Fqty,Flot,FProductdate,FEffectivedate,FUnit,FdeliverPrice,Ftaxrate,
        FUserName,FOnlineSalesName,FCheakstatus,FMofidyTime,FIsDo,FIsFree,FDATE,FArStatus,FOUTID,FCurrencyName,FDocumentStatus
        from rds_crm_shippingadvice
        """
        df = pd.read_sql(sql, self.crm_engine)
        return df

    def getFinterId(self, app2, tableName):
        '''
        在两张表中找到最后一列数据的索引值
        :param app2: sql语句执行对象
        :param tableName: 要查询数据对应的表名表名
        :return:
        '''
        sql = f"select isnull(max(FInterId),0) as FMaxId from {tableName}"
        res = app2.select(sql)
        return res[0]['FMaxId']

    def sale_out_to_dms(self, app3):
        df_sale_order = self.get_sale_out()
        cust_list = app3.select('select FDELIVERYNO from RDS_CRM_SRC_sal_delivery')
        cus_list = []
        for i in cust_list:
            cus_list.append(i['FDELIVERYNO'])
        for i, r in df_sale_order.iterrows():
            if r['FDelivaryNo'] not in cus_list and r['FDelivaryNo']:
                if r['FDocumentStatus'] == '已批准':
                    try:
                        sql1 = f"""insert into RDS_CRM_SRC_sal_delivery(FINTERID,FTRADENO,FDELIVERYNO,FBILLTYPE,FDELIVERYSTATUS,FSTOCK,FCUSTNUMBER,FCUSTOMNAME,
                                                        FORDERTYPE,FPRDNUMBER,FPRDNAME,FCOSTPRICE,FPRICE,FNBASEUNITQTY,FLOT,FPRODUCEDATE,FEFFECTIVEDATE,
                                                        FMEASUREUNIT,DELIVERYAMOUNT,FTAXRATE,FSALER,FAUXSALER,FCHECKSTATUS,UPDATETIME,FIsDo,FIsFree,FDATE,FArStatus,FOUTID,FCurrencyName,FDocumentStatus ) values
                                                      ({self.getFinterId(app3, 'RDS_CRM_SRC_sal_delivery') + 1},'{r['FSaleorderno']}','{r['FDelivaryNo']}',
                                                      '{r['FBillTypeIdName']}','{r['Fdeliverystatus']}','{r['Fstock']}','{r['FCustId']}','{r['FCustName']}','含预售',
                                                      '{r['FprNumber']}','{r['FName']}',{r['FCostPrice']},{r['FPrice']},{r['Fqty']},'{r['Flot']}','{r['FProductdate']}',
                                                      '{r['FEffectivedate']}','{r['FUnit']}',100,'{r['Ftaxrate']}','{r['FUserName']}','{r['FOnlineSalesName']}',
                                                      '{r['FCheakstatus']}','{r['FMofidyTime']}',0,0,'{r['FDATE']}',0,'{r['FOUTID']}','{r['FCurrencyName']}','{r['FDocumentStatus']}')"""
                        self.new_cursor.execute(sql1)
                        self.new_con.commit()
                    except:
                        print("{}该发货通知单数据异常".format(r['FDelivaryNo']))
                else:
                    print("{}该发货通知单未批准".format(r['FDelivaryNo']))
            else:
                print("{}该发货通知单已存在".format(r['FDelivaryNo']))


if __name__ == '__main__':
    token_erp = '9B6F803F-9D37-41A2-BDA0-70A7179AF0F3'
    app3 = RdClient(token=token_erp)
    c = CrmToDms()
    c.sale_out_to_dms(app3)
