# SQL拼接
todo
1.  Field.Applay,Field.SetDelayApply,Field.SetScene 需要梳理重新定义，目前比较混乱
 现状:
  Field.Apply 立即执行
  Field.SetDelayApply 构造SQL语句前执行
  Field.sceneFns   (Field.SceneInsert,Field.SceneSave,Field.SceneUpdate,Field.SceneSelect,Field.SceneInit,SceneFinal)获取值时执行

  3个函数的入参签名完全一致，本质上是Field 需要支持分层设置(和value 分层设置 类似)

  2. Field.Schema 迁移到Column.Schema 配置 字段约束跟表走

  3. TableConfig.DDL 生成DDL语句



备注：
Field.ValueFns.Reset() 函数，历史上会将字段值设置为nil，但是自从增加Field.value 后，失去这个作用，所以历史书写Field.ValueFns.Reset() 是为了忽略字段值，则需要改成 Field.SetValue(nil) 替代。


