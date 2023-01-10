#' CRM日志查询
#'
#'
#' @param FNumber 编码
#' @param FOccurrenceTime_min 最小日期
#' @param FOccurrenceTime_max 最小日期
#' @param FCompanyName 公司代码
#'
#' @return 返回值
#' @export
#'
#' @examples
#' crm_customer()
crm_log <- function(FNumber, FOccurrenceTime_min, FOccurrenceTime_max, FCompanyName) {
  #注册python模板
  mdl <- tsda::import('log_return.ret_log')
  #调用python函数，将.替代为$
  res <- mdl$run_log(FNumber=FNumber, FOccurrenceTime_min=FOccurrenceTime_min, FOccurrenceTime_max=FOccurrenceTime_max, FCompanyName=FCompanyName)
  #返回结果
  return(res)
}
crm_log('3.2.2.000014','2023-01-02','2023-01-05','')
