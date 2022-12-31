from urllib import parse

from pyrda.dbms.rds import RdClient
from requests import *
from sqlalchemy import create_engine
import pandas as pd

from ERPtoCRM.config import conn3

'''
* http://123.207.201.140:88/test/crmapi-demo/outboundorder.php
'''

bad_password1 = 'rds@2022'
conn = {'DB_USER': 'dms',
        'DB_PASS': parse.quote_plus(bad_password1),
        'DB_HOST': '115.159.201.178',
        'DB_PORT': 1433,
        'DATABASE': 'cprds',
        }
dms_conn = 'mssql+pymssql://{}:{}@{}:{}/{}?charset=utf8'.format(conn['DB_USER'], conn['DB_PASS'],
                                                                conn['DB_HOST'],
                                                                conn['DB_PORT'], conn['DATABASE'])
dms_engine = create_engine(dms_conn)

token_china = '9B6F803F-9D37-41A2-BDA0-70A7179AF0F3'
app3 = RdClient(token=token_china)


def back2CRM(data):
    r = post(url="http://123.207.201.140:3000/crmapi/add", json=data)
    res = r.json()
    return res


def get_data():
    sql = """
            select * from RDS_CRM_SRC_saleout where FIsdo =0
        """
    data = pd.read_sql(sql, dms_engine,)
    saleorderno_lis = data['salesorder_no'].values
    d_lis = set(saleorderno_lis)
    for i in d_lis:
        d = data[data['salesorder_no'] == i]
        d = d.set_index('index')
        print(d)
        materisals = materials_no(d)
        save_saleout(d, materisals)


def materials_no(data):
    data_lis = []
    for i,d in data.iterrows():
        model = {
            "product_no": d['FMATERIALID'],
            "salesorder_no": d['salesorder_no'],
            "quantity": str(d['FREALQTY']),
            "name": '其他仓' if d['FSTOCKID'] else '赠品仓',
            "sf2080": str(d["FCUSTMATID"]),
            # "sf2291": d['CustMatName'],
            "sf2713": str(d['FMUSTQTY']),
            "sf2924": str(d['FISFREE'])
        }
        data_lis.append(model)
    return data_lis


def save_saleout(d, materials):
    """
    从DMS回写到CRM
    :return:
    """
    model = {
        "module": "outboundorder",
        "data": [
            {
                "mainFields": {
                    "out_no": d['FBILLNO'][0],
                    "account_no": str(d['FCUSTOMERID'][0]),
                    "approvestatus": d['FDOCUMENTSTATUS'][0],
                    # "last_name": "系統管理員",
                    "createdtime": str(d['FCREATEDATE'][0]),
                    "modifiedtime": str(d['FMODIFYDATE'][0]),
                    "outdate": str(d['FDate'][0]),
                    "express_no": d['FCARRIAGENO'][0],
                    "cf_4755": str(d['FSTOCKORGID'][0]),
                    "cf_4749": str(d['FHEADLOCATIONID'][0]),
                    "cf_4750": str(d['FDELIVERYDEPTID'][0]),
                    "cf_4751": str(d['FCARRIERID'][0]),
                    "cf_4752": str(d['FSTOCKERGROUPID'][0]),
                    "cf_4756": str(d['FSTOCKERID'][0]),
                    "cf_4753": str(d['FSALEORGID'][0])
                },
                "detailFields": materials
            }
        ]
    }

    print(back2CRM(model))
    sql = "update a set a.FisDo=3 from RDS_CRM_SRC_saleout a where FBillNo = '{}'".format(
        d['FBILLNO'][0])
    app3.update(sql)


def save_salebilling(d):
    data = {
        "module": "invoice",
        "data": [
            {
                "mainFields": {
                    "invoice_no": 'INV3',
                    "account_no": 'CUST22033165',

                    "invoicetype": '增票' if d['F_SZSP_XSLX'] == '62d8b3a30d26ff' else '普票',
                    "invoice_num": d['F_SZSP_FPHM'],

                },
                "detailFields": [
                    {
                        "product_no": "300201-18",
                        "salesorder_no": d['salesorder_no'],

                    }
                ]
            }
        ]
    }
    sql = "update a set a.FisDo=1 from RDS_CRM_SRC_saleout a where FBillNo = '{}'".format(
        d['salesorder_no'][0])
    app3.update(sql)
    print(back2CRM(data))


get_data()



