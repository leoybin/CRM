import json
from crmNoticeShipment import operation as nro
from crmNoticeShipment import utility as nru


def associated(app2, api_sdk, option, data, app3):
    api_sdk.InitConfig(option['acct_id'], option['user_name'], option['app_id'],
                       option['app_sec'], option['server_url'])

    for i in data:

        if check_deliveryExist(api_sdk, i[0]['FDELIVERYNO']) != True:

            model = {
                "Model": {
                    "FID": 0,
                    "FBillTypeID": {
                        "FNUMBER": "FHTZD01_SYS"
                    },
                    "FBillNo": str(i[0]['FDELIVERYNO']),
                    "FDate": str(i[0]['FDATE']),
                    "FSaleOrgId": {
                        "FNumber": "101"
                    },
                    "FCustomerID": {
                        "FNumber": "C003142" if i[0]['FCUSTNUMBER'] == "苏州亚通生物医疗科技有限公司" else nro.code_conversion(app2,
                                                                                                                 "rds_vw_customer",
                                                                                                                 "FNAME",
                                                                                                                 i[0][
                                                                                                                     'FCUSTOMNAME'])
                    },
                    "FSalesManID": {
                        "FNumber": nro.code_conversion_org(app2, "rds_vw_salesman", "FNAME", i[0]['FSALER'], '101',
                                                           "FNUMBER")
                    },
                    "FDeliveryOrgID": {
                        "FNumber": "101"
                    },
                    "FOwnerTypeIdHead": "BD_OwnerOrg",
                    "F_SZSP_XSLX": {
                        "FNumber": "1"
                    },
                    "SubHeadEntity": {
                        "FSettleOrgID": {
                            "FNumber": "101"
                        },
                        "FSettleCurrID": {
                            "FNumber": "PRE001" if i[0]['FCurrencyName'] == "" else nro.code_conversion(app2,
                                                                                                        "rds_vw_currency",
                                                                                                        "FNAME", i[0][
                                                                                                            'FCurrencyName'])
                        },
                        "FLocalCurrID": {
                            "FNumber": "PRE001"
                        },
                        "FExchangeTypeID": {
                            "FNumber": "HLTX01_SYS"
                        },
                        "FExchangeRate": 1.0,
                        "FOverOrgTransDirect": False
                    },
                    "FEntity": nru.data_splicing(app2, api_sdk, i)
                }
            }

            res = json.loads(api_sdk.Save("SAL_DELIVERYNOTICE", model))
            print(res)
            if res['Result']['ResponseStatus']['IsSuccess']:

                FNumber = res['Result']['ResponseStatus']['SuccessEntitys'][0]['Number']

                submit_res = ERP_submit(api_sdk, FNumber)

                if submit_res:

                    audit_res = ERP_Audit(api_sdk, FNumber)

                    if audit_res:

                        nro.changeStatus(app3, str(i[0]['FDELIVERYNO']), "3")

                    else:
                        pass
                else:
                    pass
            else:

                nro.changeStatus(app3, str(i[0]['FDELIVERYNO']), "2")
                print(res)
                print(str(i[0]['FDELIVERYNO']))


def ERP_submit(api_sdk, FNumber):
    model = {
        "CreateOrgId": 0,
        "Numbers": [FNumber],
        "Ids": "",
        "SelectedPostId": 0,
        "NetworkCtrl": "",
        "IgnoreInterationFlag": ""
    }

    res = json.loads(api_sdk.Submit("SAL_DELIVERYNOTICE", model))

    return res['Result']['ResponseStatus']['IsSuccess']


def ERP_Audit(api_sdk, FNumber):
    '''
    将订单审核
    :param api_sdk: API接口对象
    :param FNumber: 订单编码
    :return:
    '''

    model = {
        "CreateOrgId": 0,
        "Numbers": [FNumber],
        "Ids": "",
        "InterationFlags": "",
        "NetworkCtrl": "",
        "IsVerifyProcInst": "",
        "IgnoreInterationFlag": ""
    }

    res = json.loads(api_sdk.Audit("SAL_DELIVERYNOTICE", model))

    return res['Result']['ResponseStatus']['IsSuccess']


def saleOrder_view(api_sdk, value, materialID):
    '''
    销售订单单据查询
    :param value: 订单编码
    :return:
    '''

    res = json.loads(api_sdk.ExecuteBillQuery(
        {"FormId": "SAL_SaleOrder", "FieldKeys": "FDate,FBillNo,FId,FSaleOrderEntry_FEntryID,FMaterialId",
         "FilterString": [{"Left": "(", "FieldName": "FMaterialId", "Compare": "=", "Value": materialID, "Right": ")",
                           "Logic": "AND"},
                          {"Left": "(", "FieldName": "FBillNo", "Compare": "=", "Value": value, "Right": ")",
                           "Logic": "AND"}], "TopRowCount": 0}))

    return res


def check_deliveryExist(api_sdk, FNumber):
    model = {
        "CreateOrgId": 0,
        "Number": FNumber,
        "Id": "",
        "IsSortBySeq": "false"
    }

    res = json.loads(api_sdk.View("SAL_DELIVERYNOTICE", model))

    return res['Result']['ResponseStatus']['IsSuccess']