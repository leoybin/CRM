#' CRM新增客户到ERP
#'
#'
#' @return 返回值
#' @export
#'
#' @examples
#' crm_customer()
crm_customer <- function() {
  #注册python模板
  mdl <- tsda::import('crmCustomer.test_customer')
  #调用python函数，将.替代为$
  res <- mdl$run()
  #返回结果
  return(res)
}
#print(crm_customer())

#' CRM新增物料
#'
#'
#' @return 返回值
#' @export
#'
#' @examples
#' crm_material()
crm_material <- function() {
  #注册python模板
  mdl <- tsda::import('crmMaterial.manage')
  #调用python函数，将.替代为$
  res <- mdl$run()
  #返回结果
  return(res)
}
#crm_material()


#' CRM发货通知单
#'
#'
#' @return 返回值
#' @export
#'
#' @examples
#' crm_material()
crm_noticeshipment <- function() {
  #注册python模板
  mdl <- tsda::import('crmNoticeShipment.mainModel')
  #调用python函数，将.替代为$
  res <- mdl$run()
  #返回结果
  return(res)
}
#crm_noticeshipment()

#' CRM销售订单
#'
#'
#' @return 返回值
#' @export
#'
#' @examples
#' crm_material()
crm_saleorder <- function() {
  #注册python模板
  mdl <- tsda::import('crmSaleOrder.mainModel')
  #调用python函数，将.替代为$
  res <- mdl$run()
  #返回结果
  return(res)
}
#crm_saleorder()


#' CRM销售出库
#'
#'
#' @return 返回值
#' @export
#'
#' @examples
#' crm_saleout()
crm_saleout <- function() {
  #注册python模板
  mdl <- tsda::import('erp2crmsaleout.manage')
  #调用python函数，将.替代为$
  res <- mdl$run()
  #返回结果
  return(res)
}
#crm_saleout()


#' CRM销售开票
#'
#'
#' @return 返回值
#' @export
#'
#' @examples
#' crm_material()
crm_salebilling <- function() {
  #注册python模板
  mdl <- tsda::import('crmSaleBilling.mainModel')
  #调用python函数，将.替代为$
  res <- mdl$run()
  #返回结果
  return(res)
}
crm_salebilling()


#' CRM销售开票
#'
#'
#' @return 返回值
#' @export
#'
#' @examples
#' crm_material()
crm_saleic <- function() {
  #注册python模板
  mdl <- tsda::import('erptocrmsaleic.manage')
  #调用python函数，将.替代为$
  res <- mdl$run()
  #返回结果
  return(res)
}
crm_saleic()
