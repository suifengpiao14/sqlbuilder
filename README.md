# SQL拼接
从2个角度看问题：
column 是从模型、表的角度出发，对表进行增改珊查操作，以及建表等操作，将表拆分成列，在此基础上以列为最小基本单元组合
params 是从接口操作角度出发，对接口字段拆分成字段，在此基础上以字段为最小基本单元组合

二者区别：
表模型角度：增改删查是一体的，是一个模型的基本操作，要求每个操作对应的column是固定不变的
params 角度：新增、修改、删除、查询 是各自独立的操作，之间没有关系，不能融合在一起，每个操作的column可以不一样
表模型和接口模型不是一一对应，包含或者被包含关系，它们可以互相交叉，一个接口操作多个表的部分数据，一个表可以提供多个接口数据操作
二者联系：
1. 表模型的列和接口模型的字段是同一个东西，2种不同的叫法。列更偏向描述业务模型中的属性，定义是什么，接口字段更偏向描述这个属性的特征检测，如手机号——phone, 表模型确定名称:phone,接口代码更偏向确保这个字段的值符合手机号规则，确定其是否可以为空等、根据具体业务验证否唯一等逻辑，有时在不同的接口(场景下)其验证规则可能不一样(比如要求唯一：新增时，表内不存在即可，更新时要求表内除了当前记录外不存在才行)
2. 最简单、通用的 增加、修改、删除、查询操作按照表模型全量字段进行，不做任何验证、检测，这样的接口非常通用，但是其业务约束力差

根据以上分析，表模型定义了对象及其属性，负责对象、属性命名，及其存储。接口模型定义了对象在特定场景(指定接口)部分属性的特征检测，这些属性既有通用属性检测(手机号格式)，又有具体业务属性检测(是否可空、是否唯一、在定义的枚举值范围内、字符串、整型)等


表、接口都是解决业务问题，特定业务本身具有一定的内涵属性，比如唯一标识，只能新增、不可修改，条件查询时支持全匹配。手机号、邮箱号原本就对应固定格式,删除字段新增默认非删除，查询接口都要携带非删除条件等。这些在属性可以分层固定下来，以便减轻具体业务开发时的心智负担

经过上述分析，分类总结如下：
1. 表模型： 固定字段名称、标题、基于表类型、大小等增加验证，支持新增、修改、查询操作，表模型只负责字段名、基于表的数据验证以及SQL的生成，不负责数据、where部分
2. 业务模型：定义业务含义，增加业务验证，值赋值部分数据校验，不负责动态数据、Where条件
2. 接口模型：结合业务验证参数、动态配置字段、查询条件等，将接口模型定义为业务场景，业务场景只负责验证数据，动态配置写入、查询字段以及where条件，不负责生成SQL



关于表模型使用 接口-类方式还是直接使用类-实例方式：
接口-类：优点是能创建多个实例,提高复用率；缺点是实例是最后提供的，类继承后其方法还是作用在父类上，无法读取子类数据,父类(中间件)所有的操作结果都需要存储在已有的实例上
类-实例：优点是灵活性强，通过重写属性值函数实现复用，缺点是只存在一个实例，不能复用

所以最终只能选择 接口-类-实例方式，并且需要选择一个底层实例，收集所有中间件生成的动态数据及条件集合

说明:
目前代码设计比较混乱.主要因为对整个事物的认知不够，没有形成整体思维、无法形成整体解决方案,目前属于收集各种场景状态下,逐步优化、重构的过程中.目前又个计划方案: 将apply,scene,valueFn,whereFn 合并成可定制化的基本单元,然后再支持特定场景下选择指定单元执行,可选择的力度可以达到基本单元中的每个函数.


