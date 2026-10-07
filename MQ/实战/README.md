# MQ 实战代码

[课程目录](../README.md) · [第 07 篇](../04-实践篇/07-可靠消息故障实验.md) · [第 08 篇](../04-实践篇/08-RabbitMQ真实消息实验.md)

Python 3.11+。两个实验都只生成确定性的本地结果，不会调用模型、发送邮件或创建沙箱。

| 文件 | 作用 |
|---|---|
| [reliable_queue.py](reliable_queue.py) | 标准库教学模型：任务与 Outbox、独立 Broker 数据库、租约、故障注入、结果幂等 |
| [test_reliable_queue.py](test_reliable_queue.py) | 9 个测试，包括实际子进程崩溃及恢复 |
| [rabbitmq_lab.py](rabbitmq_lab.py) | 真实 RabbitMQ 客户端，发布确认、手动 ACK、永久失败转死信 |
| [test_rabbitmq_handler.py](test_rabbitmq_handler.py) | 4 个本地业务处理测试，不连接 Broker |
| [requirements.txt](requirements.txt) | 真实 Broker 实验所需 Pika 1.3.2 |
| [compose.yaml](compose.yaml) | 单节点 RabbitMQ 4.1 系列教学环境 |

从本目录运行，无第三方依赖的验证命令为：

```powershell
$env:PYTHONDONTWRITEBYTECODE = '1'
python -m unittest discover -v
```

详细实验命令、每步预期结果和故障解释见对应章节。代码中的 `--crash` 会让**该 Python 实验进程**以 86 退出，这是设计好的故障注入，不会终止其他程序。

## 交付时验证记录

核验日期：2026-10-07。环境：Windows、Python 3.11.1。

- 13 项 Python 测试通过：9 项教学模型测试、4 项 RabbitMQ 本地处理函数测试。
- 四种崩溃使用子进程与 `os._exit(86)`，验证了重新打开数据库后的状态。
- Python 文件语法、CLI 帮助入口、Pika 依赖安装和 Compose 配置解析已检查。
- RabbitMQ Broker 联调未执行：主机 Docker Linux engine pipe 不存在。真实发布、网络重投、DLX 转发和节点重启均未标记为实测通过。

## 教学边界

本地模型用两个 SQLite 文件模拟独立事务资源，没有网络、复制和生产调度；单个 relay，没有并发发布领取协议；lease 仅支持领取接管，不实现续租；receipt token 只保护消息确认，不是完整的外部资源 fencing。业务效果全部在本地数据库内，不能据此推断邮件、支付或模型计费的 exactly-once。

本地模型的重试预算按投递记录计数；重复发布可能创建新的投递预算。dead 标记与任务 failed 标记也分属两个数据库，生产中需增加对账恢复。`status` 分别读取两个数据库，不是跨库一致性快照。实验没有声称解决这些边界。

真实客户端不包含 Outbox、通用重连循环、定时重试、自动死信回放、真实长任务执行或鉴权网关。意外异常会使消费者退出，未确认投递交由 Broker 处理；有明确永久失败的消息才会被拒绝。SQLite 中的 inbox 与业务效果在一个事务中，且检查重复键的载荷一致性。

## 存储与清理

默认 `.data/` 保存实验记录，`.test-tmp/` 保存自动测试临时文件，`.venv/` 保存可选客户端依赖，这些都不提交 Git。仓库位于 G 盘时，它们随项目落在 G 盘。自动测试只清理自己新建的临时目录，不删除手工实验数据。

Docker 具名卷和镜像跟随 Docker 引擎的数据根目录；应先把 Docker 的可增长存储配置到合适磁盘，再执行拉取。不要直接移动正在使用的 Docker/WSL 虚拟磁盘。
