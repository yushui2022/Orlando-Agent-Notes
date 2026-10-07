# 08｜接入真实 RabbitMQ，观察确认和重投

[← 上一篇](07-可靠消息故障实验.md) · [学习目录](../README.md) · [术语与自测](../附录/术语与自测.md)

> 前置：第 07 篇；可运行的 RabbitMQ 和 Python 环境。目标：区分 Broker 的真实行为与前一篇教学模型的简化。

这一篇用 [rabbitmq_lab.py](../实战/rabbitmq_lab.py) 连接真实 Broker。生产者发布 `ReportRequested`，消费者把模拟报告保存到 SQLite，再 ACK。没有实际模型调用和沙箱创建，因此可以重复制造故障而不产生模型费用。

**验证状态：** 本次交付已通过 Python 业务事务测试、命令入口检查和 Compose 配置解析；当前主机的 Docker 引擎未运行，下面的 Broker 启动、重投与死信现象属于待在可运行环境验证的步骤。最终核验记录见[实验说明](../实战/README.md)。

## 准备环境，先确认数据存放位置

仓库和 Python 环境放在 G 盘。Docker 的镜像、具名卷和 Linux 虚拟磁盘由 Docker 引擎管理，**不会因为 compose.yaml 位于 G 盘就自动搬过去**。首次拉取镜像前检查 G 盘空间，并在 Docker Desktop 的存储设置中确认镜像/磁盘位置；不要直接剪切已有虚拟磁盘文件。

```powershell
Set-Location 'G:\Projects\Orlando-Agent-Notes\MQ\实战'
Get-PSDrive G
$env:PIP_CACHE_DIR = 'G:\DevCache\pip'
$env:TEMP = 'G:\DevCache\Temp\orlando-mq'
$env:TMP = $env:TEMP
New-Item -ItemType Directory -Force $env:TEMP | Out-Null
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker version
docker compose config --quiet
docker compose up -d --wait
```

若 `docker version` 只有客户端信息或报 engine pipe 不存在，先启动自己的 Docker 引擎；该错误不是教程代码发送失败。若不使用 Docker，也可连接已经部署的 RabbitMQ，通过 `AMQP_URL` 环境变量指定连接地址。

Compose 使用 `rabbitmq:4.1-management`，这是会随补丁更新的 minor 标签，不是不可变版本。成功拉取后记录镜像信息，复现实验时使用同一 digest：

```powershell
docker image inspect rabbitmq:4.1-management --format '{{json .RepoDigests}}'
docker compose exec rabbitmq rabbitmq-diagnostics server_version
```

这是单节点教学环境，quorum queue 在这里没有多节点冗余。管理页面为 [localhost:15672](http://localhost:15672)，本地实验使用默认 guest 账号；端口只绑定 127.0.0.1。不要把这个实验连接配置直接部署到公网。

## 正常路径：发布确认与消费确认分开观察

在终端 A 启动消费者：

```powershell
.\.venv\Scripts\python.exe rabbitmq_lab.py consume
```

在同目录终端 B 发布消息：

```powershell
.\.venv\Scripts\python.exe rabbitmq_lab.py publish report-001 "first report"
.\.venv\Scripts\python.exe rabbitmq_lab.py inspect
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

生产者的 `confirmed: report:report-001` 表示 publisher confirm 已完成。消费者的 `succeeded` 表示本地事务提交并执行了 ACK 调用。inspect 中应有一条结果；队列统计可能需要短暂刷新才反映变化。

代码同时使用 durable queue、persistent message、publisher confirm 和 `mandatory=True`。它们分别处理不同问题：声明持久性、要求消息持久保存、等待发布确认、发现无法路由等异常。`mandatory` 能检查是否到达至少一个队列，不保证所有业务上期待的订阅队列都存在。[Pika 确认示例](https://pika.readthedocs.io/en/1.3.2/examples/blocking_delivery_confirmations.html)展示了 confirms 的客户端用法。

本篇发布命令没有 Outbox。若没有收到确认，保留同一 task_id 和内容重新执行 publish；如果要做成长期服务，应接入第 04 篇的持久发送意图，不要把一条 shell 命令当作完整可靠入口。

## 重复发布：Broker 不会替你的业务去重

再次发布完全相同的命令：

```powershell
.\.venv\Scripts\python.exe rabbitmq_lab.py publish report-001 "first report"
```

消费者应打印 `duplicate`，results 仍只有一条。同一个 task_id 在该实验中生成相同 event_id。如果换了标题却保留 task_id，消费者会拒绝这个冲突，而不是静默覆盖原结果。

`message_id` 属性本身不是 Broker 业务去重开关。真正保证此处结果只写一次的，是 SQLite 唯一约束、载荷一致性检查和事务。

## 提交结果后立刻崩溃

先在终端 A 用 Ctrl+C 停止正常消费者。实验时只保留这个消费者，否则其他实例可能先领取测试消息。

```powershell
# 终端 A
.\.venv\Scripts\python.exe rabbitmq_lab.py consume --crash after-commit
```

```powershell
# 终端 B
.\.venv\Scripts\python.exe rabbitmq_lab.py publish report-002 "crash test"
```

终端 A 预期打印 CRASH 并以 86 退出。此时 inspect 应能看到 report-002 已保存；Broker 在检测到连接关闭后会重新排队尚未确认的消息。重新启动正常消费者：

```powershell
.\.venv\Scripts\python.exe rabbitmq_lab.py consume
```

预期输出 `duplicate`，重投标记通常为 `redelivered=True`。检测和重投不是严格五秒倒计时；这正是与上一篇租约模型的差异。[RabbitMQ Work Queues](https://www.rabbitmq.com/tutorials/tutorial-two-python)介绍了未确认投递在消费者退出后的处理。

把故障参数换成 `before-commit`，使用新的任务编号再实验：这次崩溃后不应有新结果，重投后正常执行。每次崩溃实验只使用一个消费者和新的任务编号，便于将日志与消息对应。

## 慢消费：同时看 ready 和 unacked

停止现有消费者，启动带模拟延迟的消费者：

```powershell
.\.venv\Scripts\python.exe rabbitmq_lab.py consume --delay 5
```

另一个终端发送 10 条新的任务：

```powershell
1..10 | ForEach-Object {
    .\.venv\Scripts\python.exe rabbitmq_lab.py publish "slow-$_" "report $_"
}
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

消费者设置 `prefetch_count=1`，所以一个消费者通常最多有一条未确认投递，其他消息留在 ready。启动第二个同样的消费者，再观察队列消化速度。这个工作队列实验不受 Kafka 常规消费组的分区数限制。

模拟延迟使用 `connection.sleep()`，让 Pika 继续处理连接事件。把真实模型或沙箱的长阻塞调用直接塞进回调，会产生心跳和连接维护问题；实际系统应把长工作与连接 I/O 线程协调好，并在正确的 channel 上确认。Pika 的线程与事件循环要求见[官方 BlockingConnection 文档](https://pika.readthedocs.io/en/1.3.2/modules/adapters/blocking.html)。

## 永久失败：进入死信队列

正常消费者运行时发送：

```powershell
.\.venv\Scripts\python.exe rabbitmq_lab.py publish bad-001 "bad input" --poison
docker compose exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged
```

消费者将打印 rejected，并执行 `basic_nack(requeue=False)`。本实验队列绑定了 DLX，预期消息进入 `mq.course.report.dead.v1`；没有 DLX 配置时，拒绝且不重新入队可能直接丢弃消息，不能脱离队列配置照抄这行代码。

这里只演示永久失败的隔离，不实现生产级自动重试拓扑或死信回放。默认死信转发也没有在本实验中升级为跨故障的至少一次转发保证。真实使用前应按队列类型和故障要求配置，参考 [DLX 文档](https://www.rabbitmq.com/docs/4.1/dlx)。

## 把观察结果写成证据

| 实验 | 应记录的证据 | 不足以证明的结论 |
|---|---|---|
| 正常消费 | 发布确认、消费者输出、结果记录 | 多副本容灾能力 |
| 同消息发两次 | 两次投递、一次业务结果 | 外部 API 自动幂等 |
| after-commit 崩溃 | 退出码 86、重投、结果仍一条 | 所有程序故障都可恢复 |
| before-commit 崩溃 | 无新结果、重投后生成结果 | 整机断电零丢失 |
| 慢消费 | ready/unacked 随时间变化 | 生产环境容量结论 |
| poison | 拒绝日志、DLQ 增量、无新结果 | 死信链路绝不丢失 |

可用 `docker compose stop` 暂停实验，具名卷仍保留数据。不要为“重跑方便”顺手删除卷；下一轮用新的任务编号，或在确认数据仅是可丢弃实验数据后自行清理。

当你能解释表中每个“观察到了什么”和“还没证明什么”，就具备继续做真实 Agent Worker 的基础。下一步可以把确定性结果函数替换为一个受限的实际任务，再逐步加入检查点、工具幂等、租约续期和取消；每增加一种副作用，都重新画一次失败窗口。
