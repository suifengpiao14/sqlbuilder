# SQL拼接
todo
 Field.Applay,Field.SetDelayApply,Field.SetScene 需要梳理重新定义，目前比较混乱
 现状:
  Field.Applay 立即执行
  Field.SetDelayApply 构造SQL语句前执行
  Field.SetScene 获取值时执行

  3个函数的入参签名完全一致，本质上是Field 需要支持分层设置(和value 分层设置 类似)




