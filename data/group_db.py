"""按群配置库：SQLite 存 JSON + 跟随默认模板语义 + 按默认配置结构自动对齐。"""

import asyncio
import copy
import json

import aiosqlite
from astrbot.api import logger

from ..config import PluginConfig


class QQAdminDB:
    """
    群管插件数据库（极简 API + 动态字段 + 自动补齐）
    """

    FOLLOW_DEFAULT_MARKER = "__follow_default__"
    FOLLOW_MASTER_MARKER = "__follow_master__"

    # ================================================================

    def __init__(self, config: PluginConfig):
        self.db_path = config.db_path

        # 默认字段（动态配置核心）：以 schema 为键来源，保证新增字段有默认值，
        # 否则历史群配置里的新字段会被 _reconcile_group_data 当作已废弃键删掉
        try:
            self.default_cfg: dict = config.build_group_default_config()
        except Exception:
            self.default_cfg = config.default

        self._conn = None
        self._cache = {}
        self._initialized = False
        self._init_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()

    # ============================== 初始化 ==============================

    async def init(self):
        async with self._init_lock:
            if self._initialized:
                return

            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = await aiosqlite.connect(str(self.db_path))
            self._conn.row_factory = aiosqlite.Row

            await self._conn.execute("""
                CREATE TABLE IF NOT EXISTS groups (
                    group_id TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                );
            """)
            await self._conn.commit()

            # 加载缓存
            async with self._conn.execute("SELECT group_id, data FROM groups;") as cur:
                async for row in cur:
                    try:
                        self._cache[row["group_id"]] = json.loads(row["data"])
                    except Exception:
                        logger.exception("解析 group 数据失败: %s", row["group_id"])

            # 一次性迁移无 marker 的历史行：与默认值一致的删行（缺席即跟随），
            # 其余打上显式标记落库。此后跟随判定只看 marker，不再按值比较。
            for gid in list(self._cache.keys()):
                record = self._cache[gid]
                if isinstance(record, dict) and self.FOLLOW_DEFAULT_MARKER not in record:
                    if self._legacy_values_match_defaults(record):
                        self._cache.pop(gid, None)
                        await self._conn.execute("DELETE FROM groups WHERE group_id = ?", (gid,))
                    else:
                        record[self.FOLLOW_DEFAULT_MARKER] = False
                        await self._save_to_db(gid, record)
            await self._conn.commit()

            # 按当前默认配置结构对齐历史行：补新增字段、删已废弃字段
            # （含 perms 中被移除的旧权限键），一次性落库，之后读取时保持幂等。
            for gid, record in list(self._cache.items()):
                if isinstance(record, dict) and self._reconcile_group_data(record, self.default_cfg):
                    await self._save_to_db(gid, record)

            self._initialized = True
            logger.info("QQAdminDB initialized (%d groups)", len(self._cache))

    async def _save_to_db(self, gid: str, data):
        if not self._conn:
            raise RuntimeError("请先 init()")

        await self._conn.execute(
            """
            INSERT INTO groups(group_id, data)
            VALUES (?, ?)
            ON CONFLICT(group_id) DO UPDATE SET data=excluded.data;
            """,
            (gid, json.dumps(data, ensure_ascii=False)),
        )
        await self._conn.commit()

    def _strip_meta_fields(self, data: dict | None) -> dict | None:
        if data is None:
            return None
        return {
            key: copy.deepcopy(value)
            for key, value in data.items()
            if key not in (self.FOLLOW_DEFAULT_MARKER, self.FOLLOW_MASTER_MARKER)
        }

    def _legacy_values_match_defaults(self, data: dict) -> bool:
        """仅供 init 迁移：无 marker 历史行按值判定（迁移后不再使用）。"""
        clean = {key: value for key, value in data.items() if key != self.FOLLOW_DEFAULT_MARKER}
        if not clean:
            return True
        for key, value in clean.items():
            if key not in self.default_cfg:
                return False
            if value != self.default_cfg[key]:
                return False
        return True

    def _is_follow_default_data(self, data: dict | None) -> bool:
        # init 迁移已消除无 marker 行：显式记录必带 marker=False，其余一律跟随。
        # 不再按值比较——显式值恰好等于默认值也不再被误判为跟随。
        if data is None:
            return True
        return data.get(self.FOLLOW_DEFAULT_MARKER, True) is not False

    def is_group_follow_default(self, gid: str) -> bool:
        return self._is_follow_default_data(self._cache.get(gid))

    def follow_master_of(self, gid: str) -> str | None:
        """返回该群跟随的主群 id；未跟随主群返回 None。"""
        record = self._cache.get(gid)
        if not isinstance(record, dict):
            return None
        master = record.get(self.FOLLOW_MASTER_MARKER)
        return str(master) if master else None

    def _build_explicit_group_record(self, data: dict | None = None) -> dict:
        base = copy.deepcopy(data if data is not None else self.default_cfg)
        base[self.FOLLOW_DEFAULT_MARKER] = False
        return base

    def _reconcile_group_data(self, data: dict, defaults: dict) -> bool:
        """按默认配置的“形状”就地同步：补缺失键、删已废弃键，dict 递归下钻。

        `defaults` 是唯一来源（顶层字段 + `perms`/`vote_ban` 等嵌套），
        因此后续增删改配置字段只需改 `_conf_schema.json`，无需再动这里。
        只调整结构，不覆盖已存在的值（默认值变化不会抹掉用户的显式配置）。
        """
        changed = False
        meta_keys = (self.FOLLOW_DEFAULT_MARKER, self.FOLLOW_MASTER_MARKER)
        for key in [k for k in data if k not in meta_keys and k not in defaults]:
            data.pop(key, None)
            changed = True
        for key, default in defaults.items():
            if key not in data:
                data[key] = copy.deepcopy(default)
                changed = True
            elif isinstance(default, dict) and isinstance(data[key], dict):
                changed = self._reconcile_group_data(data[key], default) or changed
        return changed

    # ============================== 基础：确保配置存在 ==============================

    async def ensure_group(self, gid: str):
        """确保存在群配置，若没有则按 default_cfg 初始化"""
        async with self._write_lock:
            if gid not in self._cache or self._is_follow_default_data(self._cache.get(gid)):
                self._cache[gid] = self._build_explicit_group_record(self.get_group_snapshot(gid))
                await self._save_to_db(gid, self._cache[gid])

    def list_group_ids(self) -> list[str]:
        return sorted(
            self._cache.keys(),
            key=lambda gid: (
                not str(gid).isdigit(),
                int(gid) if str(gid).isdigit() else str(gid),
            ),
        )

    def get_group_snapshot(self, gid: str) -> dict:
        raw = self._cache.get(gid)
        if self._is_follow_default_data(raw):
            data = copy.deepcopy(self.default_cfg)
        else:
            data = self._strip_meta_fields(raw) or {}
        self._reconcile_group_data(data, self.default_cfg)
        return data

    def single_group_state(self, skip_members_of: str | None = None) -> dict[str, dict]:
        """计算“禁止多群加入”的群类型划分。

        只有显式配置（非跟随默认）且开启 join_single_group 的群才是主群，
        主群 join_single_groups 里勾选的群与之同类型。返回：
        {"masters": {主群: [同类群...]}, "locks": {被归类群: 主群}}
        冲突时先出现的主群优先；主群不能再被其他主群归类。
        `skip_members_of` 用于保存某个群时忽略它当前的归类关系（旧列表），
        但仍保留其“主群”身份，避免它被其他主群抢占。
        """
        raw_masters: dict[str, list[str]] = {}
        for gid in self.list_group_ids():
            if self.is_group_follow_default(gid):
                continue
            rec = self._strip_meta_fields(self._cache.get(gid)) or {}
            if not rec.get("join_single_group"):
                continue
            members: list[str] = []
            if gid != skip_members_of:
                for item in rec.get("join_single_groups") or []:
                    member = str(item).strip()
                    if member and member != gid and member.isdigit() and member not in members:
                        members.append(member)
            raw_masters[gid] = members

        masters = {m: [x for x in members if x not in raw_masters] for m, members in raw_masters.items()}
        locks: dict[str, str] = {}
        for master, members in masters.items():
            for member in members:
                if member in masters or member in locks:
                    continue
                locks[member] = master
        return {"masters": masters, "locks": locks}

    # ============================== API ==============================

    async def all(self, gid: str) -> dict:
        """
        获取整个配置，并自动补齐 default_cfg 的字段
        """
        if self.is_group_follow_default(gid):
            return self.get_group_snapshot(gid)

        data = self._cache[gid]

        changed = self._reconcile_group_data(data, self.default_cfg)

        if data.get(self.FOLLOW_DEFAULT_MARKER) is not False:
            data[self.FOLLOW_DEFAULT_MARKER] = False
            changed = True

        if changed:
            async with self._write_lock:
                await self._save_to_db(gid, data)

        return self.get_group_snapshot(gid)

    async def get(self, gid: str, field: str, default=None):
        """
        读字段，不存在则补齐 default
        """
        if self.is_group_follow_default(gid):
            snapshot = self.get_group_snapshot(gid)
            if field in snapshot:
                return snapshot[field]
            return copy.deepcopy(default)

        data = self._cache[gid]

        if field not in data:
            async with self._write_lock:
                # double-check after acquiring lock
                if field not in data:
                    data[field] = copy.deepcopy(default)
                    await self._save_to_db(gid, data)

        return data[field]

    async def set(self, gid: str, field: str, value):
        """
        写入字段
        """
        await self.ensure_group(gid)
        async with self._write_lock:
            self._cache[gid][field] = value
            await self._save_to_db(gid, self._cache[gid])

    async def replace_group(self, gid: str, data: dict):
        async with self._write_lock:
            self._cache[gid] = self._build_explicit_group_record(data)
            await self._save_to_db(gid, self._cache[gid])

    async def add(self, gid: str, field: str, value):
        """
        列表字段追加（自动创建列表）
        """
        lst = list(await self.get(gid, field, []))
        if value not in lst:
            lst.append(value)
            await self.set(gid, field, lst)

    # ============================== 删除群配置 ==============================

    async def delete_group(self, gid: str):
        """彻底删除群配置；原本跟随该群配置的群转为独立配置（保留最后同步值）。"""
        async with self._write_lock:
            if self._conn:
                await self._conn.execute("DELETE FROM groups WHERE group_id = ?", (gid,))
                await self._conn.commit()
            self._cache.pop(gid, None)
            for other_gid in [g for g in self._cache if self.follow_master_of(g) == gid]:
                record = self._cache[other_gid]
                if isinstance(record, dict):
                    record.pop(self.FOLLOW_MASTER_MARKER, None)
                    if self._conn:
                        await self._save_to_db(other_gid, record)

    # ============================== 关闭 ==============================

    async def close(self):
        if self._conn:
            await self._conn.close()
            self._conn = None
            self._initialized = False

    async def follow_default(self, gid: str | None = None):
        """让指定群（或全部群）重新跟随默认群配置"""
        async with self._write_lock:
            if gid is None:
                if self._conn:
                    await self._conn.execute("DELETE FROM groups")
                    await self._conn.commit()
                self._cache.clear()
                logger.info("所有群聊的群管配置已重新跟随默认值")
                return

            normalized_gid = str(gid)
            if self._conn:
                await self._conn.execute(
                    "DELETE FROM groups WHERE group_id = ?",
                    (normalized_gid,),
                )
                await self._conn.commit()
            self._cache.pop(normalized_gid, None)
        logger.info(f"群聊{normalized_gid}的群管配置已重新跟随默认值")
