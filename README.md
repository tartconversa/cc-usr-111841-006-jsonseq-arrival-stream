# 到厅事件 JSON 文本序列的连续接收

Python 3.11+ 的授权来源读取项目。当前提供单响应读取、已有结构提取和本地资料保存；协议口径见 docs/source-protocol.md。使用标准库运行，不需要安装运行依赖。

## 本地运行

启动演练来源：`python -m acquire.origin`，终端显示实际监听地址。该服务仅监听本机回环地址，记录实际 HTTP 请求，不连接政务系统。

`acquire.service.collect_one(url, database, method='GET', headers=None, body=None, max_bytes=1048576)` 读取一份响应。`python -m acquire.service` 的帮助显示位置参数；需要按来源契约传入实际 URL 与本地 SQLite 路径。来源状态非 2xx 时不写资料。

测试：`python -m unittest discover -s tests -v`。
编译检查：`python -m compileall -q acquire`。
如需构建 wheel，可先安装构建依赖再执行 `python -m pip wheel --no-deps .`；日常运行和测试不需要 wheel。

## 资料

Store.documents() 返回当前键值与收据；Store.receipt(identifier) 返回当时 URL、状态、响应头和原字节。当前同 key 的新资料覆盖旧资料；同一响应重复保存不会增加相同键。
fixtures/response.bin 是合成来源原始响应，scenario.json 给出可修改的本地来源路由及响应序列。可通过 responses 数组按命中次数返回不同响应，require_headers/body_contains 检查交互上下文，body_base64 表达任意字节；没有匹配的请求返回404。该回放服务不模拟未在 scenario 中配置的协议行为，复杂联调需要按协议增加对应交互或使用另一个可控来源，不能把回放缺少路由解释成真实业务结果。
