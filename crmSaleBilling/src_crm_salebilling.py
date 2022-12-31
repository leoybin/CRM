import pymssql
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
    # 发票
    def __init__(self):
        # 连接数据库
        dms_conn = 'mssql+pymssql://{}:{}@{}:{}/{}?charset=utf8'.format(conn2['DB_USER'], conn2['DB_PASS'],
                                                                        conn2['DB_HOST'],
                                                                        conn2['DB_PORT'], conn2['DATABASE'])
        crm_conn = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(conn3['DB_USER'], conn3['DB_PASS'],
                                                                        conn3['DB_HOST'],
                                                                        conn3['DB_PORT'], conn3['DATABASE'])
        self.new_con = pymssql.connect(host='115.159.201.178', database='cprds', user='dms', port=1433,
                                       password='rds@2022', charset='utf8')
        self.new_cursor = self.new_con.cursor()
        self.dms_engine = create_engine(dms_conn)
        self.crm_engine = create_engine(crm_conn)

    # def get_dms_saledelievry(self):
    #     sql = """select * from RDS_CRM_SRC_sal_billreceivable
    #     """
    #     df = pd.read_sql(sql, self.dms_engine)
    #     return df.columns

    def get_sale_out(self):
        sql = """
        select FInvoiceid,FSaleorderno,FDelivaryNo,FBillNO,FBillTypeNumber,FInvoiceType,FCustId,FSaleorderentryseq,FCustName,
        FPrdNumber,FName,Fqty,FUnitprice,Fmoney,FBillTypeId,FNoteType,FBankBillNo,FBillCode,FTaxrate,FInvoicedate,FUpdatetime,FIspackingBillNo,
        FIsDo,FCurrencyName,FDocumentStatus
        from rds_crm_sales_invoice
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
        invoiceId_lis = app3.select("select FBILLNO from RDS_CRM_SRC_sal_billreceivable")
        invoice_lis = []
        for i in invoiceId_lis:
            invoice_lis.append(i['FBILLNO'])
        for i, r in df_sale_order.iterrows():
            if r['FBIllNo'] not in invoice_lis:
                if r['FDocumentStatus'] == '已批准':
                    try:
                        print(r)
                        sql1 = f"""insert into RDS_CRM_SRC_sal_billreceivable(FInterID,FCUSTNUMBER,FOUTSTOCKBILLNO,FSALEORDERENTRYSEQ,FBILLTYPEID,
                        FCUSTOMNAME,FBANKBILLNO,FBILLNO,FPrdNumber,FPrdName,FQUANTITY,FTAXRATE,FTRADENO,FNOTETYPE,FISPACKINGBILLNO,
                        FBILLCODE,FINVOICEID,FINVOICEDATE,UPDATETIME,FIsDo,FCurrencyName,FDocumentStatus)values
                                  ({self.getFinterId(app3, 'RDS_CRM_SRC_sal_billreceivable') + 1},'{r['FCustId']}','{r['FDelivaryNo']}',
                                  {r['FSaleorderentryseq']},'{r['FBillTypeNumber']}','{r['FCustName']}','{r['FBankBillNo']}','{r['FBIllNo']}','{r['FPrdNumber']}','{r['FName']}',{r['Fqty']},
                                  '{r['FTaxrate']}','{r['FSaleorderno']}','{r['FNoteType']}','{r['FIspackingBillNo']}','{r['FBillCode']}','{r['FInvoiceid']}',
                                  '{r['FInvoicedate']}','{r['FUpdatetime']}',0,'{r['FCurrencyName']}','{r['FDocumentStatus']}')"""
                        self.new_cursor.execute(sql1)
                        self.new_con.commit()
                    except:
                        print("{}该发票数据异常".format(r['FBIllNo']))
                else:
                    print("{}该发票数据未批准".format(r['FBIllNo']))
            else:
                print("{}该发票数据已存在".format(r['FBIllNo']))


if __name__ == '__main__':
    token_erp = '9B6F803F-9D37-41A2-BDA0-70A7179AF0F3'
    app3 = RdClient(token=token_erp)
    c = CrmToDms()
    c.sale_out_to_dms(app3)
