import datetime
from urllib import parse

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

    def get_dms_sale(self):
        sql = """select * from RDS_ECS_SRC_sales_order
        """
        df = pd.read_sql(sql, self.dms_engine)
        return df.columns

    def get_sale_order(self):
        sql = """
        select FSaleorderno,FBillTypeIdName,FDate,FCustId,FCustName,FSaleorderentryseq,FPrdnumber,Fprname,Fqty,Fprice,
        Ftaxrate,Ftaxamount,FTaxPrice,FAllamountfor,FSaleDeptName,FSaleGroupName,FUserName,Fdescription,FIsfree,
        FIsDo,Fpurchasedate,FSalePriorityName,FSaleTypeName,Fmoney,FCollectionTerms
        from rds_crm_sales_saleorder
        """
        df = pd.read_sql(sql, self.crm_engine)
        return df

    def get_custom_form(self):
        sql = """select * from rds_crm_md_customer
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

    def sale_order_to_dms(self, app3):
        df_sale_order = self.get_sale_order()
        for i, r in df_sale_order.iterrows():
            print(r)
            sql1 = f"""insert into RDS_ECS_SRC_sales_order(FInterId,FSALEORDERNO,FBILLTYPEIDNAME,FSALEDATE,FCUSTCODE,FCUSTOMNAME,FSALEORDERENTRYSEQ,FPRDNUMBER,FPRDNAME,FQTY,FPRICE,FMONEY,FTAXRATE,FTAXAMOUNT,FTAXPRICE,FALLAMOUNTFOR,FSALDEPT,FSALGROUP,FSALER,FDESCRIPTION,FIsfree,FIsDO,FPurchaseDate,FUrgency,FSalesType,FCollectionTerms,FUpDateTime) values 
                   ({self.getFinterId(app3, 'RDS_ECS_SRC_sales_order') + 1},'{r['FSaleorderno']}','{r['FBillTypeIdName']}','{datetime.date(*map(int, r['FDate'][:10].split('-')))}','{r['FCustId']}','{r['FCustName']}','{r['FSaleorderentryseq']}','{r['FPrdnumber']}','{r['Fprname']}',
                    {r['Fqty']},'{r['Fprice']}','{r['FMoney']}','{r['Ftaxrate']}','{r['Ftaxamount']}','{r['FTaxPrice']}','{r['FAllamountfor']}','{r['FSaleDeptName']}','{r['FSaleGroupName']}','{r['FUserName']}','{r['Fdescription']}','{r['FIsfree']}','{r['FIsDo']}','2022-11-29','{r['FSalePriorityName']}','{r['FSaleTypeName']}','{r['FCollectionTerms']}',getdate())"""
            app3.insert(sql1)

    def get_delivery_notice(self):
        sql = """
        select FSaleorderno,FBillTypeIdName,FDate,FCustId,FCustName,FSaleorderentryseq,FPrdnumber,Fprname,Fqty,Fprice,
        Ftaxrate,Ftaxamount,FTaxPrice,FAllamountfor,FSaleDeptName,FSaleGroupName,FUserName,Fdescription,FIsfree,
        FIsDo,Fpurchasedate,FSalePriorityName,FSaleTypeName,Fmoney,FCollectionTerms
        from rds_crm_sales_saleorder
        """
        df = pd.read_sql(sql, self.crm_engine)
        return df


if __name__ == '__main__':
    token_erp = '9B6F803F-9D37-41A2-BDA0-70A7179AF0F3'
    app3 = RdClient(token=token_erp)
    c = CrmToDms()
    c.sale_order_to_dms(app3)
