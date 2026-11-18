# Device Fault Log Ops — 在线建索引平台

设备故障日志持续写入，运维通过 API 在线构建 `(device_id, occurred_at)` 索引（`CREATE INDEX CONCURRENTLY`），构建期间日志照常写入。两个无状态 API 实例通过 PostgreSQL 自身协调，保证同一索引的并发申请只启动一次构建。

## 架构

```
                ┌────────────┐        ┌────────────┐
   客户端 ─────▶│  api1      │        │  api2      │◀───── 客户端
   (:8001/docs) │ FastAPI    │        │ FastAPI    │ (:8002/docs)
                └─────┬──────┘        └─────┬──────┘
                      │      同一个数据库    │
                      └─────────┬───────────┘
                                ▼
                     ┌─────────────────────┐
                     │  PostgreSQL 16 (db) │
                     │  device_fault_logs  │  预置 400 万行故障日志
                     │  index_build_jobs   │  构建任务台账（唯一约束）
                     └─────────────────────┘
```

协调机制（全部落在数据库内，实例无状态、无领导者）：

| 需求 | 机制 |
| --- | --- |
| 并发申请只构建一次 | `INSERT ... ON CONFLICT (index_name) DO NOTHING`（唯一约束保证跨实例只有一个赢家）；临界区再套一层会话级 advisory lock |
| 构建期间日志照常写入 | `CREATE INDEX CONCURRENTLY`（不阻塞 DML），构建在独立后台线程 + 独立连接上执行 |
| 构建中的无效目录项不得误删 | 清理前检查 `pg_stat_progress_create_index` / `pg_stat_activity` 是否有活跃构建；有则绝不动无效索引；新认领的任务在宽限期内同样受保护 |
| 确认有效才报告就绪 | 构建完成后校验 `pg_index.indisvalid && indisready` 且 `pg_get_indexdef` 定义匹配 `(device_id, occurred_at)`，才置 `ready`；`GET /indexes/status` 每次也实时核对目录 |
| 失败/崩溃后清理重试 | 宽限期后无活跃构建的 `building` 记录判定为孤儿 → `failed`；重试时先 `DROP INDEX CONCURRENTLY IF EXISTS` 清掉无效残件再重新认领 |
| 重启状态一致 | 实例启动时 `reconcile()`：把台账与 `pg_catalog` 实际状态对齐（孤儿→failed、已完成→ready、目录矛盾→failed） |
| 迟到旧作业不得覆盖新结果 | 所有终态写入带围栏条件 `WHERE id = <job> AND status = 'building'`；任务行被清理重建后旧作业的 UPDATE 命中 0 行 |

已知限制：实例崩溃后，其 `building` 记录需等 `ORPHAN_GRACE_SECONDS`（默认 20s，compose 中设 15s）宽限期过后才被判定为孤儿——这是区分“认领后尚未在 `pg_stat_activity` 可见”与“构建者已死”的必要窗口。

## 运行（Docker Compose）

```bash
docker compose up -d --build
# Swagger UI: http://localhost:8001/docs  或  http://localhost:8002/docs
# PostgreSQL: localhost:5433 (ops/ops, db=faultlogs)
```

首次启动时 `db/init.sql` 预置 400 万行日志（约需几十秒），db 健康检查会等到 schema 就绪后才放行 API 容器。

## API

| 方法/路径 | 说明 |
| --- | --- |
| `POST /logs` | 写入一条故障日志（`device_id`、`message` 必填；`severity`、`occurred_at` 可选） |
| `GET /logs` | 按 `device_id` + 时间范围查询（走新索引） |
| `GET /logs/plan` | `EXPLAIN` 上述查询，验证索引被使用 |
| `POST /indexes/build` | 幂等申请建索引；并发调用只会有一个 `started`，其余 `building`；已就绪返回 `ready` |
| `GET /indexes/status` | 任务状态 + 实时目录核对（`catalog.exists/valid/ready/definition`、活跃构建后端数） |
| `GET /indexes/jobs/{task_id}/diagnostics` | 按任务 ID 的在线阻塞诊断（见下） |
| `POST /indexes/cleanup` | 确认无活跃构建后，删除无效残件索引并清理失败任务，允许重试 |
| `GET /health` | 健康检查 |

`POST /indexes/build` 返回的 `state`：`started`（本次认领成功）、`building`（他处已在构建）、`ready`（目录已确认有效）、`conflict`（同名索引定义不符，需人工处理）。

### 阻塞诊断 `GET /indexes/jobs/{task_id}/diagnostics`

值班员用任务 ID 定位“迟迟不结束”的在线建索引到底被谁挡住。诊断**每次请求都基于 PostgreSQL 实时视图现算，不缓存任何结论**，把台账任务与实际执行 DDL 的后端关联起来：

1. 按任务 ID 查 `index_build_jobs` 台账行；任务换代（旧行被删除重建）后旧 ID 直接 `not_found`，旧阶段/阻塞者无从归给新任务。
2. 构建线程在执行 DDL 前，把本后端的 `backend_pid` + `backend_start` 围栏式写入台账（`UPDATE ... WHERE id=? AND status='building'`，命中 0 行说明已换代，放弃 DDL）。PID 会被复用，因此必须 `pid` 与 `backend_start` 同时匹配才认可后端身份。
3. 构建阶段取自 `pg_stat_progress_create_index`（含 lockers/blocks/tuples 进度指标）。
4. 阻塞者 PID 只取自 `pg_blocking_pids()`——这是数据库当次自己确认的答案；返回为空但后端确实在等待时，报 `waiting_unconfirmed` 并给出 wait_event，**绝不猜测阻塞者**。

返回的 `state` 区分如下情形：

| state | 含义 |
| --- | --- |
| `not_found` | 无此任务 ID（从未存在，或已被新一代任务替换） |
| `not_started` | 台账为 building，但构建线程尚未注册后端 |
| `initializing` / `scanning` | DDL 活跃，处于初始化/扫描或校验阶段，无阻塞 |
| `blocked` | `pg_blocking_pids()` 确认了至少一个阻塞者 PID，`blockers` 带其会话详情 |
| `waiting_unconfirmed` | 后端处于等待阶段但数据库给不出阻塞者 PID（不猜测） |
| `unconfirmable` | DDL 活跃但进度视图暂不可见，或处于孤儿宽限期内后端身份尚不能下结论 |
| `backend_unconfirmed` | 宽限期外，记录的后端已退出、PID 已复用、或该 PID 已不在执行本任务的 DDL |
| `ready` / `failed` | 终态任务：不沿用任何旧阶段或阻塞者，`phase/backend/blocker_pids` 一律为 null |

换代/退出/PID 复用的隔离保证：诊断只认真实台账行 + `(pid, backend_start)` 双重身份；任务就绪或失败后返回终态，不携带历史结论。诊断接口只读，不改变台账状态，日志写入与建索引流程不受影响。

## 自测

`tests/selftest.py` 端到端验证：并发去重、构建期写入、无效目录项保护、崩溃→孤儿检测→清理→重试、迟到作业围栏、重启一致性、查询计划命中索引。

`tests/test_diagnostics.py` 端到端验证阻塞诊断 API（D1–D7）：入参校验与未知 ID、健康构建生命周期（不臆造阻塞者、写入不中断）、真实锁冲突下 `blocked` 且阻塞者 PID 与 `pg_blocking_pids()` 一致、阻塞者消失后旧 PID 绝不沿用、任务换代后旧 ID `not_found`、后端退出/PID 复用防护、终态不携带旧结论。

`tests/test_diagnostics_unit.py` 用桩连接确定性覆盖全部诊断分类分支（含 HTTP 轮询无法稳定采样的亚毫秒级 `waiting_unconfirmed` 窗口），重点验证“数据库不确认就不报阻塞者”。

```bash
# Docker 模式（需要本机有 docker）：
docker compose up -d --build
python3 tests/selftest.py --mode docker \
  --dsn "postgresql://ops:ops@localhost:5433/faultlogs"

# 本地模式（无 docker，使用 pgserver 提供的 PostgreSQL 二进制）：
python3 -m venv /tmp/ops-venv && /tmp/ops-venv/bin/pip install -r requirements.txt pgserver
tests/run_local.sh

# 阻塞诊断测试（本地模式，数据库已在运行时）：
python3 tests/test_diagnostics.py      --mode local --dsn "<conninfo>"
python3 tests/test_diagnostics_unit.py
```

## 验证状态

**原有功能（`tests/selftest.py`）：64/64 检查通过**，覆盖：并发申请去重（12 并发仅 1 次构建、活跃构建后端数恒为 1）、构建期写入零失败、构建中的无效目录项全程未被删除、就绪前目录校验、崩溃后服务端完成构建的自愈、构建被中断后的无效残件清理与重试、迟到旧作业围栏（UPDATE 命中 0 行）、启动 reconcile 与重启一致性、查询计划命中索引。

**阻塞诊断（`tests/test_diagnostics.py`）：43/43 检查通过**，覆盖：错误 UUID 返回 422、未知/换代任务 ID 返回 `not_found`；健康构建经 `not_started→scanning→ready` 且全程不臆造阻塞者、构建期 100+ 次日志写入零失败；真实写事务持锁把构建钉在 `waiting for writers before build`（wait_event `Lock/virtualxid`），`blocked` 给出的阻塞者 PID 与直接调用 `pg_blocking_pids()` 完全一致，COMMIT 后构建恢复且 PID 消失；只读长事务制造 `waiting for old snapshots` 阻塞，终止后死 PID 在后续所有响应中不再出现；任务换代后旧 ID `not_found`、新 ID 独立报告；后端退出（宽限期内 `unconfirmable`、宽限期外 `backend_unconfirmed`）、PID 复用（`backend_start` 不符）、PID 不在执行本任务 DDL 三种身份失效均被拦截；终态任务不携带任何旧阶段/阻塞者。

**诊断分类单元测试（`tests/test_diagnostics_unit.py`）：14/14 通过**，用桩连接确定性覆盖全部 9 种 state 分支，包含 HTTP 轮询无法稳定采样的亚毫秒级 `waiting_unconfirmed`（等待阶段但 `pg_blocking_pids()` 为空时返回空阻塞列表而非猜测 PID）。

说明：本交付环境的沙箱没有 Docker，`docker-compose.yml`/`Dockerfile` 未实际构建运行（已做 YAML/语法校验，双实例 api1/api2、健康检查、端口映射保持不变）；上述验证以 local 模式（pgserver 提供的 PostgreSQL 16.2 + 两个本地 uvicorn 实例）完成，应用代码路径与 docker 模式完全一致。

## 目录

```
app/main.py                    FastAPI 接口
app/index_manager.py           建索引协调核心（认领/围栏/清理/核对/重启对齐）
app/diagnostics.py             按任务 ID 的实时阻塞诊断（阶段识别 + pg_blocking_pids）
app/db.py                      连接与 schema 等待
db/init.sql                    建表 + 400 万行预置 + 任务台账表（含后端身份列）
tests/selftest.py              端到端自测（local/docker 两种编排）
tests/test_diagnostics.py      阻塞诊断端到端测试（D1–D7）
tests/test_diagnostics_unit.py 阻塞诊断分类的确定性单元测试
tests/local_db.py              本地 pgserver 数据库引导
docker-compose.yml             db(postgres:16) + api1 + api2
```
