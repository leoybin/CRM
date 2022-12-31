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
    # 销售订单
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

    def get_sale_order(self):
        sql = """
        select FSaleorderno,FBillTypeIdName,FDate,FCustId,FCustName,FSaleorderentryseq,FPrdnumber,FPruName,Fqty,Fprice,
        Ftaxrate,Ftaxamount,FTaxPrice,FAllamountfor,FSaleDeptName,FSaleGroupName,FUserName,Fdescription,FIsfree,
        FIsDo,Fpurchasedate,FSalePriorityName,FSaleTypeName,Fmoney,FCollectionTerms,FDocumentStatus
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
        sOrder_lis = app3.select('select FSaleorderno from RDS_CRM_SRC_sales_order')
        Saleorderentryseq_lis = []
        for i in sOrder_lis:
            Saleorderentryseq_lis.append(i['FSaleorderno'])
        for i, r in df_sale_order.iterrows():
            if r['FSaleorderno'] not in Saleorderentryseq_lis:
                if r['FDocumentStatus'] == '已批准':
                    try:
                        print(r)
                        sql1 = f"""insert into RDS_CRM_SRC_sales_order(FInterId,FSALEORDERNO,FBILLTYPEIDNAME,FSALEDATE,FCUSTCODE,FCUSTOMNAME,FSALEORDERENTRYSEQ,FPRDNUMBER,FPRDNAME,FQTY,FPRICE,FMONEY,FTAXRATE,FTAXAMOUNT,FTAXPRICE,FALLAMOUNTFOR,FSALDEPT,FSALGROUP,FSALER,FDESCRIPTION,FIsfree,FIsDO,FPurchaseDate,FUrgency,FSalesType,FCollectionTerms,FUpDateTime,FDocumentStatus) values 
                               ({self.getFinterId(app3, 'RDS_ECS_SRC_sales_order') + 1},'{r['FSaleorderno']}','{r['FBillTypeIdName']}','{datetime.date(*map(int, r['FDate'][:10].split('-')))}','{r['FCustId']}','{r['FCustName']}',{r['FSaleorderentryseq']},'{r['FPrdnumber']}','{r['FPruName']}',
                                {r['Fqty']},'{r['Fprice']}','{r['FMoney']}','{r['Ftaxrate']}','{r['Ftaxamount']}','{r['FTaxPrice']}','{r['FAllamountfor']}','{r['FSaleDeptName']}','{r['FSaleGroupName']}','{r['FUserName']}','{r['Fdescription']}',0,0,'{r['Fpurchasedate']}','{r['FSalePriorityName']}','{r['FSaleTypeName']}','{r['FCollectionTerms']}',getdate(),'{r['FDocumentStatus']}')"""
                        app3.insert(sql1)
                    except:
                        print("{}该销售订单数据异常".format(r['FSaleorderentryseq']))
                else:
                    print("{}该销售订单未批准".format(r['FSaleorderentryseq']))

            else:
                print("{}该销售订单已存在".format(r['FSaleorderentryseq']))

    def get_saleorder(self):
        sql = """
            select FBillNo from rds_crm_sales_saleorder_list
        """
        df = pd.read_sql(sql, self.crm_engine)
        sql1 = """
            select FBillNo from RDS_CRM_SRC_saleOrderList
        """
        df_bill = pd.read_sql(sql1, self.dms_engine)
        d_lis = list(df_bill["FBillNo"])
        for i, d in df.iterrows():
            if d[0] in d_lis:
                df = df.drop(i, axis=0)
        if not df.empty:
            df.loc[:, "FIsDo"] = '0'
            df = df.drop_duplicates('FBillNo', keep='first', )
            df.to_sql("RDS_CRM_SRC_saleOrderList", self.dms_engine, if_exists='append', index=False)


if __name__ == '__main__':
    token_erp = '9B6F803F-9D37-41A2-BDA0-70A7179AF0F3'
    app3 = RdClient(token=token_erp)
    c = CrmToDms()
    c.sale_order_to_dms(app3)
    c.get_saleorder()
