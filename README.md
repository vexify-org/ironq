# ⚡ ironq

Node.js 分布式任务队列，基于 Redis。

## 特性

- 基于 Redis，支持分布式
- 支持优先级队列
- 失败重试 + 死信队列
- 延迟任务
- 并发控制

## 快速开始

```js
import { createQueue } from 'ironq'

const queue = createQueue({ url: 'redis://localhost:6379' })
await queue.enqueue('send-email', { to: 'test@example.com', subject: 'Hello' })
```

## License

Apache-2.0
