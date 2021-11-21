# 目录

[TOC]

## 背景

1. 业务不断增多，数据流复杂，没有人能说清楚具体的上下游关系，制约了遇到问题时的排查速度
2. 下线某个业务的时候，不知道能够下线相关的哪些操作，最后背负了一些不必要的历史包袱
3. 为优化DW/DM模型提供数据支撑，数据治理的前提
4. Datahub / Purview 提供的是元数据管理，核心功能除了支持检索，另一个溯源，但当前咱们的血缘关系需要手动维护，存在数据缺失，维护力度不足等问题，如果想用好该系统，自动更新血缘关系是必要的
5. 我们的工作模式，核心的数据整理逻辑多是依据 PL/pgSQL，要想从用户自定义的 function 中获取表之间的关系，就需要解析器支持解析 function

## 调研参考

- [Open Source SQL Parsers](https://hackernoon.com/14-open-source-sql-parsers)
    - 主流有三种实现
        - 基于正则规则做SQL解析
        - 基于ANTLR生成AST(抽象语法树)，然后解析AST
        - 基于数据库的语法解析器构建出的独立模块，方便做语法解析
    - 从主流的三种实现来说，正则方式对于复杂SQL支持度不好，ANTLR方式需要编写语法解析规则，遇到复杂SQL同样头痛。第三种可以说是对数据库的开源模块的封装，所以支持度较好。但之前社区提供的语法解析模块不支持 PL/pgSQL，一个月前发布的新版才宣布支持了该功能，虽然尚属于实验阶段，不过亲测满足使用。
- [比开源快30倍的自研SQL Parser设计与实践](https://mp.weixin.qq.com/s/q86lPDWMM4NeIkQ4ilMwGg) 阿里实现的
    - 自研语法解析器，但只大体讲了实现原理，没有开源
- [自研血缘关系](https://www.cxyzjd.com/article/YcoeXu/114242239)
    - 基于 ANTLR，但只大体讲了实现原理，没有开源
    - [数仓开发流程](https://www.processon.com/view/link/5fed67095653bb21c1b1acaf)
- [SQLFlow](http://support.sqlparser.com/gsp-overview.html)
    - 网上搜到的一款商业产品，宣称的功能很强大，实测不支持 function
- 翻了 Github 上相关的开源项目，并没有找到可以支持 PL/pgSQL 血缘解析的项目，都支持的是常规简单SQL
    - 几个看着还不错的，作为学习借鉴的来源
        - Python 实现
            - ~~https://github.com/reata/sqllineage~~ 命令行工具，很实用的小工具，基于 sqlparse 库实现
            - ~~https://github.com/elementary-data/elementary-lineage~~ 面向 Snowflake 与 BigQuery
                - 基于 sqlparse(正则匹配)，Graph 实现基于 networkx 库
            - https://github.com/tokern/data-lineage 面向 AWS Redshift 与 Snowflake，方案完整
                - SQL 解析基于 pglast，但社区不太活跃
        - Java 实现，基于 Druid Parser 
            - ~~https://github.com/L11168032/sql-lineage-parser~~ 仅支持简单的 SELECT 操作，且只是一个 parser
            - https://github.com/JupiterMouse/data-lineage-parent 基于 Spring，看着实现的比较完整，模型定义值得参考

## 难点及进展

- [x] 支持 PL/pgSQL 解析拆解
- [x] 支持解析各种常见 SQL 语法
- [x] 将解析结果，生成一张“图”
    - [ ] 要的时候需要剔除图中部分节点，生成一张精简后的图，否则就需要解决临时表的描述问题
- [ ] 入库 Neo4j
    - [x] 点
    - [ ] 边
- [ ] 前端可视化，支持从 Neo4j 读数据，然后生成血缘关系图
    - Neo4j 建模的时候需要考虑如何方便查询检索

## 核心流程图

![](http://assets.processon.com/chart_image/6182a739637689771d63f061.png)

核心三个模块:

- SQL 历史收集
- 语法解析模块
- Graph 生成

