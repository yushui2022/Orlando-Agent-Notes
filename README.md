# Orlando Agent Notes

围绕 Agent 开发补齐后端与系统工程知识。每个专题尽量从一个真实问题出发，解释工作机制、故障边界，再用可以复现的实验验证。

## 学习专题

- **[从 0 到 1 学 MQ：从消息传递到可靠的 Agent 任务](MQ/README.md)**：8 篇连续文章，覆盖基础、入门、进阶与实践；附 Python 故障实验和 RabbitMQ 实验。
- [缓存穿透、缓存击穿、缓存雪崩在 Agent 里面的场景](缓存穿透、缓存击穿、缓存雪崩在agent里面的场景.md)：理解缓存失效如何影响数据库、外部 API 与模型调用。

## 关联仓库

[How-to-learn-Agent-Sandbox](https://github.com/yushui2022/How-to-learn-Agent-Sandbox) 讨论 Agent 在什么环境中执行、怎样隔离与回收；MQ 专题讨论任务怎样被接收、分配、重试并形成可查询的结果。两条学习线在任务生命周期、多 Agent 协作与失败恢复处相接。
