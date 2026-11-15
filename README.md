# 带噪计数预算服务（Private Noisy-Count Budget Service）

一个只用 **Python 标准库** 与 **SQLite** 实现的、按受试者（subject）核算差分隐私
预算的服务。计数由一个**独立的持久私有进程**完成，服务与计数进程之间**只经 HTTP
通信**。整套系统支持并发、应答丢失、进程重启与强退恢复。

```
┌────────────┐   HTTP (JSON)   ┌───────────────────────┐   HTTP (loopback,    ┌──────────────────┐
│  客户端     │ ─────────────▶  │  预算服务 (privbudget) │ ── bearer token ──▶  │ 计数进程 (独立)   │
│ (任意语言)  │ ◀─────────────  │  budget.sqlite3 (WAL)  │ ◀──────────────────  │ counter.sqlite3  │
└────────────┘                 └───────────────────────┘   z = Σbits + noise   └──────────────────┘
```

两者是**两个独立的操作系统进程**、**两个独立的数据库文件**，不共享内存，也不共享
连接或事务；唯一的通道是带令牌校验的 loopback HTTP。

---

## 1. 运行

```bash
# 启动服务（数据目录必须是新的或此前已初始化的目录）
python3 -m privbudget --data /var/lib/privbudget [--host 127.0.0.1] [--port 0]

# 启动成功后，stdout 第一行是握手 JSON：
# {"port": 44847}
```

`--port 0` 由系统分配端口；握手行给出实际端口。收到 `SIGTERM`/`SIGINT` 会干净退出。
计数进程由服务在首次需要时启动，随服务退出而退出（父进程看门狗），数据保留在
`<data>/counter.sqlite3`；服务重启后计数进程会用同一数据库重新拉起，历史结果不变。

数据目录和其中的数据库文件在创建时会尽力收紧为 `0700` / `0600`。

服务以 `python -m privbudget` 运行；HTTP API 见第 3 节，`privbudget.client.Client`
是测试/示例用的最小客户端，`privbudget.api.PrivateBudget` 是同进程的 Python 门面
（计数进程仍是独立进程）。

跑验收测试：

```bash
python3 -m unittest -v tests.test_acceptance
python3 examples/demo.py        # 端到端演示（题面例子、重叠组、重启持久化）
```

## 2. 初始化（仅一次）

`POST /initialize`

```json
{
  "bits":   {"alice": 0, "bob": 1},
  "groups": {"team": ["alice", "bob"], "empty": []},
  "limits": {"alice": "1/1", "bob": "1/1"}
}
```

* 成功：`{"state": "initialized"}`；此后任何再次初始化（即使内容相同）都返回
  `{"error": "$"}`。第一次成功即永久生效，重启后仍保持。
* 校验规则（失败返回 `{"error": "<JSONPath>"}`，且**不改动任何状态**，服务保持未初始化）：
  * `bits`、`groups`、`limits` 三键必需，且都必须是对象；
  * 受试者名、组名、组成员均为**非空 ASCII 字母数字串**；
  * `bits` 的值必须是整数 `0` 或 `1`（**布尔视为非法**）；
  * `groups` 的每个值是无重复受试者的数组，且每个成员必须出现在 `bits` 中；
  * `limits` 对 **`bits` 中每个** 受试者都必须给出，键必须是已存在的受试者，
    值为**正分子、正分母**的 `"p/q"` 字符串（如 `"3/4"`；`"0/1"`、`"-1/1"`、
    `"1"`、`"1.5"` 均非法）；`limits` 中多余的键也非法。
  * 允许空组（成员数组为空），也允许 `groups` 为空对象。

错误路径是**零基 JSONPath**，相对请求体（`initialize` 相对配置对象本身）。例如
`$.bits.bob`、`$.groups.team[1]`、`$.limits.alice`；键不是合法标识符时使用引号形式
`$.groups['bad name']`。多个错误时任选其一报告。

## 3. 接口

| 方法 | 路径 | 请求 | 成功响应 |
|---|---|---|---|
| POST | `/initialize` | 配置对象 | `{"state":"initialized"}` |
| POST | `/start` | `{"job","group","epsilon"}` | `{"state":"pending"}` |
| POST | `/status` | `{"job"}`（可选 `"wait": 秒`，最长 60） | `{"state": ...}` |
| POST | `/read` | `{"job"}` | `{"value": <实数>}` |
| POST | `/cancel` | `{"job"}` | `{"state":"cancelled"}` |
| GET | `/budgets` | — | `{"subject": {"spent":"p/q","reserved":"p/q"}, ...}` |

失败一律 HTTP 200 + `{"error":"<JSONPath>"}`；HTTP 500 只用于内部不可恢复错误。
`status` 的 `"wait"` 是可选的便利长轮询，语义不变。

`start` 的合法参数：`job`、`group` 为非空 ASCII 字母数字串，`epsilon` 为正分子正分母
的 `"p/q"` 串。`group` 必须存在于配置中。

## 4. 预算语义

* **预留在组内每个人身上原子地进行**：`start(job, group, ε)` 在同一次 SQLite 写事务里
  检查组内**每个**受试者 `spent + reserved + ε ≤ limit`，全部满足才写入 `job` 与
  逐人预留；否则整笔拒绝，返回 `{"error": "$.epsilon"}`，状态不动。
* **`bits = 0` 同样收费**：预留与扣费只与 `epsilon` 有关，与组内 bit 值无关。
* 新 `job` 状态为 `pending`（待算）。
* **同 `job`、同 `group`、`epsilon` 数值相同**重复调用 `start`：返回**当前状态**
  （`pending`/`ready`/`published`/`cancelled`），不重复预留。`"2/2"` 与 `"1/1"` 数值相同，
  视为相同。同 `job` 但 `group` 或 `epsilon` 不同：拒绝（`$.group` / `$.epsilon`）。
* **编号永久保留**：任务一旦成功创建，标识永远不会被释放或复用；取消后再次 `start`
  同名任务返回 `{"state":"cancelled"}`。
* `budgets` 把所有已配置受试者映射到 `{spent, reserved}`，两者均为**约分后的
  `"p/q"` 字符串**（如 `"1/3"`、`"0/1"`）。

## 5. 任务状态机

```
          start               计数进程返回结果
   (无) ─────────▶ pending ────────────────────▶ ready
                     │                            │  │
                     │ cancel                     │  │ read（原子：预留→已花，ready→published）
                     ▼                            │  ▼
                 cancelled ◀─────────────────────  │ published
                     ▲         cancel             │  │
                     └────────────────────────────┘  │ read 重复返回同一 value
                                                     （cancel 被拒绝）
```

* `status` 只返回 `{"state": ...}`，绝不泄露噪声值。
* `read`：仅在 `ready` 时成功，**原子地**把该任务的全部预留转为已花并置为
  `published`，然后返回 `{"value": <实数>}`。已发布任务重复 `read` 返回**同一个值**。
  其他状态（`pending`/`cancelled`/不存在）返回 `{"error": "$.job"}`。
* `cancel`：`pending` 或 `ready` → `cancelled`，释放预留；重复 `cancel` 成功并返回
  `{"state":"cancelled"}`；`published` 拒绝；不存在拒绝。
* **迟到的结果不得复活任务**：结果只对仍处于 `pending` 的任务生效；任务若已取消/已发布，
  计数进程的回复被丢弃，状态与预算都不变。

## 6. 噪声

计数进程对组内 `bits` 求和 `s = Σ bits`，加上独立噪声 `N`，返回 `z = s + N` 的
**精确有理数**。噪声服从拉普拉斯分布

```
f(x) = (ε/2) · exp(−ε|x|)
```

即尺度 `1/ε`、方差 `2/ε²` 的拉普拉斯分布，ε 取本次 `job` 的 `epsilon`。实现上取
`U ~ Uniform(0,1]`、`M = −ln(U)/ε`，随机符号，`N = ±M`。空组的和为 0，仍然加噪。

**同一 `job` 的结果持久固定**：计数进程在 `counter.sqlite3` 里按 job 落盘唯一值，
服务则把收到的值写进自己的数据库。重启服务或计数进程都不会重新抽噪声。

**仅预算服务可取结果**：计数进程只监听 loopback，并要求服务启动时随机生成的
bearer token。它从不对外暴露查询接口；未发布（`ready`，未 `read`）的值只存在于服务
和计数进程内部，客户端只能通过 `read` 拿到。

## 7. 并发、丢失应答与重启恢复

* **串行等价**：服务所有状态迁移都在 `BEGIN IMMEDIATE` 写事务中完成（`synchronous=FULL`，
  `busy_timeout=30s`），因此表现为某个串行顺序。并发 `start` 不会超卖：测试里 8 个并发
  请求争抢只够 1 个的预算，恰好 1 个成功。
* **写前意图（outbox）**：`start` 在同一个事务里把 `job`、预留和一条待发送的计算请求
  一起落盘。之后才由后台投递线程联系计数进程。因此"服务崩溃"不会丢任务。
* **应答可丢**：投递使用**固定请求 id**，连接断开/超时只当成"结果未知"，用同一个 id
  重发。计数进程按 `(req_id)` 和 `(job)` 做**幂等去重**，重发既不会二次抽噪声，也不会
  重复计数。测试用 `--drop-first N`（算完就挂断）与 `--die-after-compute N`（算完直接
  `_exit`）覆盖这两种情形。
* **重启补办未决请求**：未投递成功的 outbox 记录保持 `pending`，服务的投递线程或服务
  重启后会继续用原请求 id 重发。
* **扣账提交前后强退**：
  * `read` 在 `COMMIT` **之前**崩溃：预留仍在、任务仍是 `ready`，重启后重试 `read`
    正好扣一次。
  * `read` 在 `COMMIT` **之后**崩溃：预留已转为已花、任务已 `published`，重启后重复
    `read` 返回同一值，绝不二次扣费。
  这两种情形都由 `PRIVBUDGET_TEST_CRASH_READ=before_commit|after_commit` 注入并在验收
  测试中覆盖。

## 8. 可注入噪声的验收

测试时用 `--test-noise` 启动服务，`start` 请求可额外携带 `"noise": "p/q"`（或整数）。
计数进程会用它代替随机抽样，于是期望值完全可判定：`value = Σbits + noise`。
这是**只在测试模式**下开启的钩子；生产模式下携带 `noise` 会以 `{"error":"$.noise"}`
拒绝，计数进程也默认拒绝该字段。

其余故障注入（同样只在测试中使用）：

| 启动参数 / 环境变量 | 作用 |
|---|---|
| `--test-noise` | 允许 `start` 注入噪声 |
| 计数进程 `--drop-first N` | 前 N 个请求算完即断连、不回包 |
| 计数进程 `--die-after-compute N` | 前 N 个请求算完即 `_exit(70)` |
| 计数进程 `--drop-replies id1,id2` | 指定请求 id 第一次不回包 |
| `PRIVBUDGET_TEST_DROP_FIRST` 等 | 由服务透传给计数进程 |
| `PRIVBUDGET_TEST_CRASH_READ` | `read` 提交前/提交后 `os._exit` |
| `--hold-delivery` | 服务不自动联系计数进程（便于精确观察 pending） |

这些一次性故障在计数进程重启后不会重复（服务会在计数库旁写标记文件）。

## 9. 测试覆盖

`tests/test_acceptance.py`（33 个用例，全部通过）：

* 初始化仅一次、重启后仍拒绝再初始化、非法配置不产生副作用、各类 JSONPath；
* 题面例子（限额 1/1，j 预留后 k 被拒，取消 j 后 k 可受理，读取 k 后取消失败）；
* `bits=0` 仍收费；**重叠组**（b 同时属于 g 与 h）原子预留；分数 ε 累加；
* 任务标识永久保留、同参数返回当前状态、冲突参数拒绝；
* `status` 只回状态、`read` 状态机与幂等、`cancel` 幂等、迟到结果不复活；
* 噪声注入下值精确等于 `Σbits + noise`；拉普拉斯密度均值/方差/分箱统计检验；
* 并发 `start` 不超卖、并发 `read` 只扣一次；
* **丢失应答**（首次回包丢弃）仍然只扣一次；取消与丢失回包竞争；
* 服务/计数进程重启；**扣账提交前强退**与**提交后强退**；
* 计数进程的隔离性（无令牌请求一律 401）与**被 SIGKILL 后自动重启**；
* 空组（值为纯噪声）。

## 10. 源码结构

```
privbudget/
  __main__.py      命令行入口（启动 HTTP 服务、打印握手、信号处理）
  server.py        HTTP API（标准库 http.server）
  core.py          预算状态机、SQLite 事务、投递线程、配置校验
  counter_worker.py 独立计数进程（HTTP + 自有 SQLite + 拉普拉斯噪声）
  rpc.py           服务→计数进程的 HTTP 传输与进程管理（重启、令牌）
  errors.py        JSONPath 错误与严格参数解析
  api.py           可选的进程内 Python 门面（背后仍是独立计数进程）
  client.py        验收测试用的最小 HTTP 客户端
tests/
  test_acceptance.py
examples/
  demo.py          端到端演示（题面例子、重叠组、持久化、重启）
```
