# 到厅事件 JSON 文本序列的连续接收

Python 3.11+ 的授权来源读取项目。当前提供单响应读取、已有结构提取、本地资料保存，以及 RFC 7464 流式接收与逐段核验；协议口径见 docs/source-protocol.md。使用标准库运行，不需要安装运行依赖。

## 本地运行

启动演练来源：`python -m acquire.origin`，终端显示实际监听地址。该服务仅监听本机回环地址，记录实际 HTTP 请求，不连接政务系统。响应可加 `gzip`、`split_at`（按字节拆成 HTTP 分块）、`chunk_delay` 与 `hold_open`，用来演练压缩、分块、慢速和长时间不关闭的连接。

`acquire.service.collect_one(url, database, method='GET', headers=None, body=None, max_bytes=1048576)` 读取一份完整响应。`python -m acquire.service` 的帮助显示位置参数；需要按来源契约传入实际 URL 与本地 SQLite 路径。来源状态非 2xx 时不写资料。

流式接收：

```
python -m acquire.service 'http://127.0.0.1:PORT/stream?from=s1' data.sqlite3 --source hall
python -m acquire.service 'http://127.0.0.1:PORT/stream?from=s1' data.sqlite3 --show --segment s1
python -m acquire.service ignored data.sqlite3 --show --attempt 3
```

`acquire.service.stream_collect(url, database, source=None, from_segment=None, ..., should_stop=None, on_segment=None)`：

- 边读边解压边分帧，不等连接关闭；`end` 记录通过条数、逐条 seq 与摘要校验后立即落库，`on_segment` 在该刻回调。
- `from_segment` 缺省取持久游标；游标只在段新通过校验时前进，未完成段不会被后续游标跳过。
- 三套独立预算：`max_wire_bytes`（线路原始字节，若 Content-Length 已超限则连接后即拒绝）、`max_decoded_bytes`（解压后字节）、`max_open_record`（单条未闭合记录）。触顶抛 `BudgetExceeded`，已验证段保留，原始线路字节作为证据留在 attempt 上。
- `should_stop` 在每次网络读取前轮询，返回真时抛 `AcquisitionCancelled`；已达到的段与未完成范围都先落库。
- 返回 attempt 摘要、来源地址、游标与各段状态；异常时同份信息可从 `stream_show` 读取。

段状态：`verified`（begin/event/end 自洽，可能为空段且 `empty` 为真）、`corrupt`（段内出现损坏、计数/摘要/seq 不符或缺少 end）、`partial`（尚无 end 且未发现损坏）。`stream_show(database, segment=...)` 给出解码字节范围 `source_range`、未闭合范围 `open_record`、损坏偏移与原因、原始事件记录与 attempt 原始字节。

测试：`python -m unittest discover -s tests -v`。
编译检查：`python -m compileall -q acquire`。
如需构建 wheel，可先安装构建依赖再执行 `python -m pip wheel --no-deps .`；日常运行和测试不需要 wheel。

## 资料

Store.documents() 返回当前键值与收据；Store.receipt(identifier) 返回当时 URL、状态、响应头和原字节。当前同 key 的新资料覆盖旧资料；同一响应重复保存不会增加相同键。

流式资料表（`stream_attempts`、`stream_segments`、`stream_events`、`stream_corruptions`、`stream_cursor`）与单响应资料表共用同一 SQLite 文件但语义独立：已验证段写入一次后不会被后续损坏重放降级；重放可刷新损坏段与未完成段。原始事件保存不带 RS 的 JSON 文本，校验摘要按“逐条原始文本 + LF”计算，与协议的段摘要口径一致。

fixtures/response.bin 是合成来源原始响应，scenario.json 给出可修改的本地来源路由及响应序列。可通过 responses 数组按命中次数返回不同响应，require_headers/body_contains 检查交互上下文，body_base64 表达任意字节；没有匹配的请求返回404。该回放服务不模拟未在 scenario 中配置的协议行为，复杂联调需要按协议增加对应交互或使用另一个可控来源，不能把回放缺少路由解释成真实业务结果。
