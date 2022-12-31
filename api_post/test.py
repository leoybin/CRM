import datetime
import time
from urllib import parse
from pymssql import ProgrammingError
import pandas as pd
from pyrda.dbms.rds import RdClient
from sqlalchemy import create_engine

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
    def __init__(self):
        # 连接数据库
        dms_conn = 'mssql+pymssql://{}:{}@{}:{}/{}?charset=utf8'.format(conn2['DB_USER'], conn2['DB_PASS'],
                                                                        conn2['DB_HOST'],
                                                                        conn2['DB_PORT'], conn2['DATABASE'])
        crm_conn = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(conn3['DB_USER'], conn3['DB_PASS'],
                                                                        conn3['DB_HOST'],
                                                                        conn3['DB_PORT'], conn3['DATABASE'])
        self.dms_engine = create_engine(dms_conn)
        self.crm_engine = create_engine(crm_conn)

    def get_dms_saledelievry(self):
        sql = """select * from RDS_CRM_SRC_sal_delivery
        """
        df = pd.read_sql(sql, self.dms_engine)
        return df.columns

    def get_sale_out(self):
        sql = """
        select FSaleorderno,FDelivaryNo,FBillTypeIdName,Fdeliverystatus,FDeliveryDate,Fstock,FCustId,FCustName,FCustName,
        FprNumber,FName,Fcostprice,FPrice,Fqty,Flot,FSUMSUPPLIERLOT,FProductdate,FEffectivedate,FUnit,FdeliverPrice,Ftaxrate,
        FUserName,FOnlineSalesName,FCheakstatus,FMofidyTime,FIsDo,FIsFree,FDATE
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
        for i, r in df_sale_order.iterrows():
            print(r)
            sql1 = f"""insert into RDS_CRM_SRC_sal_delivery(FTRADENO,FDELIVERYNO,FBILLTYPE,FDELIVERYSTATUS,FDELIVERYDATE,FSTOCK,FCUSTNUMBER,FCUSTOMNAME,FORDERTYPE,FPRDNUMBER,FPRDNAME,FCOSTPRICE,FPRICE,FNBASEUNITQTY,FPRODUCEDATE,FEFFECTIVEDATE,FMEASUREUNIT,DELIVERYAMOUNT,FTAXPRICE,FSALER,FAUXSALER,FCHECKATATUS,UPDATETIME,FIsDo,FIsFree,FDATE) values
                      ({self.getFinterId(app3, 'RDS_CRM_SRC_sal_delivery') + 1},'{r['FSaleorderno']}','{r['FDelivaryNo']}',
                      '{r['FBillTypeIdName']}','{r['Fdeliverystatus']}','{datetime.datetime(*map(str, str(r['Fdeliverydate'])))}',
                      '{r['Fstock']}','{r['FCustId']}','{r['FCustName']}','changgui','{r['FprNumber']}','{r['FName']}','{r['Fcostprice']}',
                      '{r['FPrice']}','{r['Fqty']}','{r['Flot']}','{r['FSUMSUPPLIERLOT']}',
                      '{datetime.date(*map(int, str(r['FProductdate']).split('-')))}',
                      '{datetime.date(*map(int, str(r['FEffectivedate']).split('-')))}','{r['FUnit']}','159.0','{r['Ftaxrate']}',
                      '{r['FUserName']}','test','{r['FCheakstatus']}','{datetime.date(*map(int, r['FMofidyTime'][:10].split('-')))}',
                      '{r['FIsDo']}','{r['FIsFree']}','{datetime.date(*map(int, str(r['FDATE']).split('-')))}',getdate())"""
            app3.insert(sql1,code='utf-8')


if __name__ == '__main__':
    token_erp = '9B6F803F-9D37-41A2-BDA0-70A7179AF0F3'
    app3 = RdClient(token=token_erp)
    c = CrmToDms()
    c.sale_out_to_dms(app3)
