import json
from k3cloud_webapi_sdk.main import K3CloudApiSdk
from pyrda.dbms.rds import RdClient


class CommUtility(object):

    def __init__(self, erp_token, china_token):
        self.api_sdk = K3CloudApiSdk()
        self.app2 = RdClient(token=erp_token)
        self.app3 = RdClient(token=china_token)

    def ERP_audit(self, table_name, FNumber):
        """
        :param table_name: 表名
        :param FNumber: 编码
        :return:
        """

        model = {
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "InterationFlags": "",
            "NetworkCtrl": "",
            "IsVerifyProcInst": "",
            "IgnoreInterationFlag": "",
        }

        res = json.loads(self.api_sdk.Audit(table_name, model))

        return res['Result']['ResponseStatus']['IsSuccess']

    def ERP_submit(self, table_name, FNumber):
        """
        :param table_name: 表名
        :param FNumber: 编码
        :return:
        """
        model = {
            "CreateOrgId": 0,
            "Numbers": [FNumber],
            "Ids": "",
            "SelectedPostId": 0,
            "NetworkCtrl": "",
            "IgnoreInterationFlag": ""
        }

        res = json.loads(self.api_sdk.Submit(table_name, model))

        return res['Result']['ResponseStatus']['IsSuccess']

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

    def erp_select(self, wd, tb, condition):
        """
        :param wd: 字段
        :param tb: 表名
        :param condition: 条件
        :return:
        """
        sql = "select {} from {} where {}".format(wd, tb, condition)
        res = self.app2.select(sql)
        return res

    def china_select(self, wd, tb, condition):
        """
        :param wd: 字段
        :param tb: 表名
        :param condition: 条件
        :return:
        """
        sql = "select {} from {} where {}".format(wd, tb, condition)
        res = self.app3.select(sql)
        return res

    def changeStatus(self, status, tableName, param, param2):
        '''
        改变数据状态
        :param status: 状态
        :param tableName: 表名
        :param param: 条件名
        :param param2: 条件
        :return:
        '''
        sql = f"update a set a.Fisdo={status} from {tableName} a where {param}='{param2}'"

        self.app3.update(sql)

    def getStatus(self, fNumber, tableName):
        '''
        获得数据状态
        :param app2: sql语句执行对象
        :param fNumber: 编码
        :param tableName: 表名
        :return:
        '''

        sql = f"select Fisdo from {tableName} where FNumber='{fNumber}'"

        if self.app2.select(sql):

            res = self.app2.select(sql)[0]['Fisdo']

            if res == 1:
                return False
            elif res == 0:
                return True
            else:
                return 2

    def delivery_view(self, js_data):
        '''
        订单单据查询
        :param value: 订单编码
        :return:
        '''

        res = json.loads(
            self.api_sdk.ExecuteBillQuery(js_data))

        return res

    def code_conversion(self, tableName, param, param2):
        '''
        通过ECS物料编码来查询系统内的编码
        :param app2: 数据库操作对象
        :param tableName: 表名
        :param param:  参数1
        :param param2: 参数2
        :return:
        '''

        sql = f"select FNumber from {tableName} where {param}='{param2}'"

        res = self.app2.select(sql)

        if res:

            return res[0]['FNumber']

        else:

            return ""

    def iskfperiod(self, app2, FNumber):
        '''
        查看物料是否启用保质期
        :param app2:
        :param FNumber:
        :return:
        '''

        sql = f"select FISKFPERIOD from rds_vw_fiskfperiod where F_SZSP_SKUNUMBER='{FNumber}'"

        res = app2.select(sql)

        if res:

            return res[0]['FISKFPERIOD']

        else:

            return ""

    def isbatch(self, app2, FNumber):
        sql = f"select FISBATCHMANAGE from rds_vw_fisbatch where F_SZSP_SKUNUMBER='{FNumber}'"

        res = app2.select(sql)

        if res == []:

            return ""

        else:

            return res[0]['FISBATCHMANAGE']

    def getCode(self):
        '''
        查询出表中的编码
        :return:
        '''

        sql = "select FBillNo,Fseq,Fdate,FDeptName,FItemNumber,FItemName,FItemModel,FUnitName,Fqty,FStockName,Flot,Fnote,FPRODUCEDATE,FEFFECTIVEDATE,FSUMSUPPLIERLOT,FAFFAIRTYPE,FIsdo from RDS_ECS_ODS_DISASS_DELIVERY where FIsdo=0"

        res = self.app3.select(sql)

        return res

    def check_exists(self, table_name, api_sdk, FNumber):
        '''
        查看订单是否在ERP系统存在
        :param api: API接口对象
        :param FNumber: 订单编码
        :return:
        '''

        model = {
            "CreateOrgId": 0,
            "Number": FNumber,
            "Id": "",
            "IsSortBySeq": "false"
        }

        res = json.loads(api_sdk.View(table_name, model))

        return res['Result']['ResponseStatus']['IsSuccess']

    def code_conversion_org(self, tableName, param, param2, param3, param4):
        '''
        通过ECS物料编码来查询系统内的编码
        :param app2: 数据库操作对象
        :param tableName: 表名
        :param param:  参数1
        :param param2: 参数2
        :return:
        '''

        sql = f"select {param4} from {tableName} where {param}='{param2}' and FOrgNumber='{param3}'"

        res = self.app2.select(sql)

        if res:

            return res[0][param4]

        else:

            return ""


if __name__ == '__main__':
    token_erp = '4D181CAB-4CE3-47A3-8F2B-8AB11BB6A227'
    token_china = '9B6F803F-9D37-41A2-BDA0-70A7179AF0F3'
    comm = CommUtility(token_erp, token_china)
    print(comm.getCode())
