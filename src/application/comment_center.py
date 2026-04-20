from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from re import search
from urllib.parse import parse_qs, urlparse
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from openpyxl import Workbook
from pydantic import BaseModel

try:
    from aiomysql import connect
except ImportError:  # pragma: no cover
    connect = None

from ..storage.mysql_config import DEFAULT_MYSQL_CONFIG


class TypeCreateRequest(BaseModel):
    name: str


class UrlCreateRequest(BaseModel):
    url: str = ""
    urls_text: str = ""
    fetch_type: str


class CollectOneRequest(BaseModel):
    task_id: int


class CollectAllRequest(BaseModel):
    fetch_type: str


class CommentCenterRepository:
    def __init__(self, parameter):
        self.parameter = parameter
        self._config = self._resolve_mysql_config(parameter)

    @staticmethod
    def _resolve_mysql_config(parameter) -> dict[str, Any]:
        settings_data = {}
        settings = getattr(parameter, "settings", None)
        if settings and hasattr(settings, "read"):
            try:
                loaded = settings.read()
                if isinstance(loaded, dict):
                    settings_data = loaded
            except Exception:
                settings_data = {}

        cfg: dict[str, Any] = {}
        for key, default in DEFAULT_MYSQL_CONFIG.items():
            if hasattr(parameter, key):
                cfg[key] = getattr(parameter, key)
            else:
                cfg[key] = settings_data.get(key, default)
        return cfg

    async def _create_database_if_not_exists(self):
        if connect is None:
            raise HTTPException(status_code=500, detail="未安装 aiomysql")
        try:
            db = await connect(
                host=self._config["mysql_host"],
                port=int(self._config["mysql_port"]),
                user=self._config["mysql_user"],
                password=self._config["mysql_password"],
                charset="utf8mb4",
                autocommit=True,
            )
        except RuntimeError as e:
            if "cryptography" in str(e):
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "MySQL 认证需要 cryptography 依赖，请执行："
                        " `uv pip install --python .venv/bin/python cryptography`"
                    ),
                ) from e
            raise
        try:
            async with db.cursor() as cursor:
                await cursor.execute("SET sql_notes = 0;")
                await cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self._config['mysql_database']}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
                )
                await cursor.execute("SET sql_notes = 1;")
        finally:
            db.close()

    @asynccontextmanager
    async def connection(self):
        if connect is None:
            raise HTTPException(status_code=500, detail="未安装 aiomysql")
        await self._create_database_if_not_exists()
        try:
            db = await connect(
                host=self._config["mysql_host"],
                port=int(self._config["mysql_port"]),
                user=self._config["mysql_user"],
                password=self._config["mysql_password"],
                db=self._config["mysql_database"],
                charset="utf8mb4",
                autocommit=True,
            )
        except RuntimeError as e:
            if "cryptography" in str(e):
                raise HTTPException(
                    status_code=500,
                    detail=(
                        "MySQL 认证需要 cryptography 依赖，请执行："
                        " `uv pip install --python .venv/bin/python cryptography`"
                    ),
                ) from e
            raise
        try:
            yield db
        finally:
            db.close()

    async def ensure_tables(self):
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute("SET sql_notes = 0;")
                try:
                    await cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS `comment_types` (
                            `id` BIGINT NOT NULL AUTO_INCREMENT,
                            `name` VARCHAR(64) NOT NULL,
                            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (`id`),
                            UNIQUE KEY `uniq_name` (`name`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """
                    )

                    await cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS `comment_url_tasks` (
                            `id` BIGINT NOT NULL AUTO_INCREMENT,
                            `url` VARCHAR(2048) NOT NULL,
                            `detail_id` VARCHAR(64) DEFAULT '',
                            `fetch_type` VARCHAR(64) NOT NULL,
                            `status` VARCHAR(32) NOT NULL DEFAULT '未处理',
                            `last_error` LONGTEXT,
                            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            `last_fetch_at` TIMESTAMP NULL DEFAULT NULL,
                            PRIMARY KEY (`id`),
                            KEY `idx_fetch_type` (`fetch_type`),
                            KEY `idx_status` (`status`),
                            KEY `idx_detail_id` (`detail_id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """
                    )

                    await cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS `comment_records` (
                            `id` BIGINT NOT NULL AUTO_INCREMENT,
                            `task_id` BIGINT NOT NULL,
                            `url` VARCHAR(2048) NOT NULL,
                            `detail_id` VARCHAR(64) NOT NULL,
                            `fetch_type` VARCHAR(64) NOT NULL,
                            `dedupe_key` CHAR(64) NOT NULL,
                            `cid` VARCHAR(128) NULL,
                            `comment_time` DATETIME NULL,
                            `comment_time_text` VARCHAR(64) DEFAULT '',
                            `nickname` VARCHAR(255) DEFAULT '',
                            `uid` VARCHAR(128) DEFAULT '',
                            `sec_uid` VARCHAR(255) DEFAULT '',
                            `ip_label` VARCHAR(255) DEFAULT '',
                            `text` LONGTEXT,
                            `digg_count` BIGINT DEFAULT 0,
                            `reply_comment_total` BIGINT DEFAULT 0,
                            `raw_json` LONGTEXT,
                            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            PRIMARY KEY (`id`),
                            UNIQUE KEY `uniq_dedupe_key` (`dedupe_key`),
                            KEY `idx_task_id` (`task_id`),
                            KEY `idx_fetch_type` (`fetch_type`),
                            KEY `idx_comment_time` (`comment_time`),
                            KEY `idx_detail_id` (`detail_id`)
                        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
                        """
                    )
                finally:
                    await cursor.execute("SET sql_notes = 1;")

                # 兼容已经创建过旧版本表结构的场景
                await self._ensure_column(cursor, "comment_records", "dedupe_key", "CHAR(64) NOT NULL DEFAULT ''")
                await cursor.execute(
                    """
                    UPDATE `comment_url_tasks`
                    SET `status` = CASE `status`
                        WHEN 'pending' THEN '未处理'
                        WHEN 'ready' THEN '未处理'
                        WHEN 'running' THEN '处理中'
                        WHEN 'done' THEN '处理完成'
                        WHEN 'failed' THEN '处理失败'
                        ELSE `status`
                    END
                    WHERE `status` IN ('pending', 'ready', 'running', 'done', 'failed');
                    """
                )
                await cursor.execute(
                    """
                    UPDATE `comment_records`
                    SET `dedupe_key` = SHA2(
                        CONCAT(
                            IFNULL(`detail_id`, ''), '|',
                            IFNULL(`cid`, ''), '|',
                            IFNULL(`comment_time_text`, ''), '|',
                            IFNULL(`nickname`, ''), '|',
                            IFNULL(`text`, ''), '|',
                            `id`
                        ),
                        256
                    )
                    WHERE `dedupe_key` IS NULL OR `dedupe_key` = '';
                    """
                )
                await self._ensure_unique_index(cursor, "comment_records", "uniq_dedupe_key", "dedupe_key")

    async def _ensure_column(self, cursor, table: str, column: str, ddl: str):
        await cursor.execute(
            """
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
            """,
            (self._config["mysql_database"], table, column),
        )
        row = await cursor.fetchone()
        if row and int(row[0]) == 0:
            await cursor.execute(f"ALTER TABLE `{table}` ADD COLUMN `{column}` {ddl};")

    async def _ensure_unique_index(self, cursor, table: str, index_name: str, column: str):
        await cursor.execute(
            """
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND INDEX_NAME=%s
            """,
            (self._config["mysql_database"], table, index_name),
        )
        row = await cursor.fetchone()
        if row and int(row[0]) == 0:
            await cursor.execute(
                f"CREATE UNIQUE INDEX `{index_name}` ON `{table}` (`{column}`);"
            )

    async def list_types(self) -> list[str]:
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute("SELECT name FROM `comment_types` ORDER BY id DESC")
                rows = await cursor.fetchall()
        return [r[0] for r in rows]

    async def add_type(self, name: str):
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    "INSERT IGNORE INTO `comment_types` (`name`) VALUES (%s)",
                    (name,),
                )

    async def delete_type(self, name: str):
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute("DELETE FROM `comment_types` WHERE name=%s", (name,))

    async def type_exists(self, name: str) -> bool:
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute("SELECT COUNT(*) FROM `comment_types` WHERE name=%s", (name,))
                row = await cursor.fetchone()
        return bool(row and int(row[0]) > 0)

    async def insert_url_task(self, url: str, fetch_type: str) -> tuple[int, bool]:
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    "SELECT id FROM `comment_url_tasks` WHERE url=%s LIMIT 1",
                    (url,),
                )
                row = await cursor.fetchone()
                if row:
                    return int(row[0]), False

                await cursor.execute(
                    "INSERT INTO `comment_url_tasks` (`url`, `fetch_type`) VALUES (%s, %s)",
                    (url, fetch_type),
                )
                return int(cursor.lastrowid), True

    async def list_url_tasks(
        self,
        fetch_type: str = "",
        status: str = "",
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 200))
        offset = (page - 1) * page_size
        sql = "SELECT id, url, detail_id, fetch_type, status, last_error, created_at, updated_at, last_fetch_at FROM `comment_url_tasks`"
        count_sql = "SELECT COUNT(*) FROM `comment_url_tasks`"
        params: list[Any] = []
        where: list[str] = []
        if fetch_type:
            where.append("fetch_type=%s")
            params.append(fetch_type)
        if status:
            where.append("status=%s")
            params.append(status)
        if where:
            where_clause = " WHERE " + " AND ".join(where)
            sql += where_clause
            count_sql += where_clause
        sql += " ORDER BY id DESC LIMIT %s OFFSET %s"
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(count_sql, tuple(params))
                total_row = await cursor.fetchone()
                total = int(total_row[0]) if total_row else 0
                query_params = tuple([*params, page_size, offset])
                await cursor.execute(sql, query_params)
                rows = await cursor.fetchall()
        return {
            "items": [
                {
                    "id": r[0],
                    "url": r[1],
                    "detail_id": r[2],
                    "fetch_type": r[3],
                    "status": r[4],
                    "last_error": r[5],
                    "created_at": str(r[6]) if r[6] else None,
                    "updated_at": str(r[7]) if r[7] else None,
                    "last_fetch_at": str(r[8]) if r[8] else None,
                }
                for r in rows
            ],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    async def get_task(self, task_id: int) -> dict[str, Any] | None:
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    "SELECT id, url, detail_id, fetch_type, status FROM `comment_url_tasks` WHERE id=%s",
                    (task_id,),
                )
                row = await cursor.fetchone()
        if not row:
            return None
        return {
            "id": row[0],
            "url": row[1],
            "detail_id": row[2],
            "fetch_type": row[3],
            "status": row[4],
        }

    async def update_task_resolved(self, task_id: int, detail_id: str):
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    "UPDATE `comment_url_tasks` SET detail_id=%s WHERE id=%s",
                    (detail_id, task_id),
                )

    async def update_task_status(self, task_id: int, status: str, error: str | None = None):
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    "UPDATE `comment_url_tasks` SET status=%s, last_error=%s, last_fetch_at=NOW() WHERE id=%s",
                    (status, error, task_id),
                )

    async def list_url_task_ids_by_type(self, fetch_type: str) -> list[int]:
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(
                    "SELECT id FROM `comment_url_tasks` WHERE fetch_type=%s ORDER BY id DESC",
                    (fetch_type,),
                )
                rows = await cursor.fetchall()
        return [int(r[0]) for r in rows]

    async def upsert_comments(
        self,
        task_id: int,
        url: str,
        detail_id: str,
        fetch_type: str,
        items: list[dict[str, Any]],
    ):
        async with self.connection() as db:
            async with db.cursor() as cursor:
                for item in items:
                    user = item.get("user") if isinstance(item.get("user"), dict) else {}
                    uid = (
                        item.get("uid")
                        or user.get("uid")
                        or user.get("user_id")
                        or ""
                    )
                    nickname = (
                        item.get("nickname")
                        or user.get("nickname")
                        or user.get("nick_name")
                        or ""
                    )
                    sec_uid = (
                        item.get("sec_uid")
                        or user.get("sec_uid")
                        or user.get("secUid")
                        or ""
                    )
                    ip_label = (
                        item.get("ip_label")
                        or item.get("ipLabel")
                        or user.get("ip_label")
                        or ""
                    )
                    text = item.get("text") or ""
                    comment_time_text = item.get("create_time") or ""
                    comment_time = self._parse_comment_time(comment_time_text)
                    dedupe_key = self._build_dedupe_key(detail_id, item)
                    await cursor.execute(
                        """
                        INSERT INTO `comment_records` (
                            task_id, url, detail_id, fetch_type, dedupe_key, cid, comment_time, comment_time_text,
                            nickname, uid, sec_uid, ip_label, text, digg_count, reply_comment_total, raw_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        AS new
                        ON DUPLICATE KEY UPDATE
                            task_id=new.task_id,
                            url=new.url,
                            fetch_type=new.fetch_type,
                            cid=new.cid,
                            comment_time=new.comment_time,
                            comment_time_text=new.comment_time_text,
                            nickname=new.nickname,
                            uid=new.uid,
                            sec_uid=new.sec_uid,
                            ip_label=new.ip_label,
                            text=new.text,
                            digg_count=new.digg_count,
                            reply_comment_total=new.reply_comment_total,
                            raw_json=new.raw_json
                        """,
                        (
                            task_id,
                            url,
                            detail_id,
                            fetch_type,
                            dedupe_key,
                            item.get("cid") or None,
                            comment_time,
                            comment_time_text,
                            str(nickname),
                            str(uid),
                            str(sec_uid),
                            str(ip_label),
                            text,
                            int(item.get("digg_count") or 0),
                            int(item.get("reply_comment_total") or 0),
                            str(item),
                        ),
                    )

    @staticmethod
    def _build_dedupe_key(detail_id: str, item: dict[str, Any]) -> str:
        seed = "|".join(
            [
                detail_id,
                str(item.get("cid") or ""),
                str(item.get("create_time") or ""),
                str(item.get("nickname") or ""),
                str(item.get("text") or ""),
            ]
        )
        return sha256(seed.encode("utf-8")).hexdigest()

    @staticmethod
    def _parse_comment_time(value: str | int | float | None):
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            ts = float(value)
            # 兼容毫秒时间戳
            if ts > 10_000_000_000:
                ts = ts / 1000.0
            try:
                return datetime.fromtimestamp(ts)
            except (ValueError, OSError):
                return None
        if not isinstance(value, str):
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        return None

    async def list_comments(
        self,
        detail_id: str = "",
        fetch_type: str = "",
        days: int | None = None,
        keyword: str = "",
        regions: list[str] | None = None,
        page: int = 1,
        page_size: int = 20,
    ) -> dict[str, Any]:
        page = max(1, int(page))
        page_size = max(1, min(int(page_size), 200))
        offset = (page - 1) * page_size
        where_sql, params = self._build_comment_filters(
            detail_id=detail_id,
            fetch_type=fetch_type,
            days=days,
            keyword=keyword,
            regions=regions or [],
        )
        sql = (
            "SELECT id, detail_id, uid, nickname, ip_label, comment_time, comment_time_text, text "
            f"FROM `comment_records` WHERE 1=1 {where_sql} "
            "ORDER BY comment_time DESC, id DESC LIMIT %s OFFSET %s"
        )
        count_sql = f"SELECT COUNT(*) FROM `comment_records` WHERE 1=1 {where_sql}"

        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(count_sql, tuple(params))
                total_row = await cursor.fetchone()
                total = int(total_row[0]) if total_row else 0
                query_params = tuple([*params, page_size, offset])
                await cursor.execute(sql, query_params)
                rows = await cursor.fetchall()

        return {
            "items": [
                {
                    "id": r[0],
                    "detail_id": r[1],
                    "uid": r[2] or "",
                    "nickname": r[3] or "",
                    "ip_label": r[4] or "",
                    "comment_time": str(r[5]) if r[5] else None,
                    "comment_time_text": r[6] or "",
                    "comment_time_display": self._format_comment_time_display(r[5], r[6]),
                    "text": r[7] or "",
                }
                for r in rows
            ],
            "total": total,
            "page": page,
            "page_size": page_size,
        }

    async def list_comments_for_export(
        self,
        detail_id: str = "",
        fetch_type: str = "",
        days: int | None = None,
        keyword: str = "",
        regions: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        where_sql, params = self._build_comment_filters(
            detail_id=detail_id,
            fetch_type=fetch_type,
            days=days,
            keyword=keyword,
            regions=regions or [],
        )
        sql = (
            "SELECT uid, nickname, ip_label, comment_time, comment_time_text, text "
            f"FROM `comment_records` WHERE 1=1 {where_sql} "
            "ORDER BY comment_time DESC, id DESC"
        )
        async with self.connection() as db:
            async with db.cursor() as cursor:
                await cursor.execute(sql, tuple(params))
                rows = await cursor.fetchall()
        return [
            {
                "uid": r[0] or "",
                "nickname": r[1] or "",
                "ip_label": r[2] or "",
                "comment_time_display": self._format_comment_time_display(r[3], r[4]),
                "text": r[5] or "",
            }
            for r in rows
        ]

    @staticmethod
    def _format_comment_time_display(comment_time: Any, comment_time_text: Any) -> str:
        if comment_time:
            if isinstance(comment_time, datetime):
                return comment_time.strftime("%Y-%m-%d %H:%M:%S")
            return str(comment_time)
        parsed = CommentCenterRepository._parse_comment_time(comment_time_text)
        if parsed:
            return parsed.strftime("%Y-%m-%d %H:%M:%S")
        return str(comment_time_text or "")

    @staticmethod
    def _build_comment_filters(
        detail_id: str,
        fetch_type: str,
        days: int | None,
        keyword: str,
        regions: list[str],
    ) -> tuple[str, list[Any]]:
        where: list[str] = []
        params: list[Any] = []
        if detail_id:
            where.append("detail_id=%s")
            params.append(detail_id)
        if fetch_type:
            where.append("fetch_type=%s")
            params.append(fetch_type)
        if days and days > 0:
            begin = datetime.now() - timedelta(days=days)
            where.append("comment_time IS NOT NULL AND comment_time >= %s")
            params.append(begin)
        if keyword:
            where.append("text LIKE %s")
            params.append(f"%{keyword}%")
        region_list = [r.strip() for r in regions if r and r.strip()]
        if region_list:
            region_sql = []
            for region in region_list:
                region_sql.append("ip_label LIKE %s")
                params.append(f"{region}%")
            where.append("(" + " OR ".join(region_sql) + ")")
        return (" AND " + " AND ".join(where)) if where else "", params


class CommentCenterController:
    def __init__(self, api_server):
        self.api_server = api_server
        self.repo = CommentCenterRepository(api_server.parameter)

    async def resolve_detail_id(self, task: dict[str, Any]) -> str:
        if task.get("detail_id"):
            return task["detail_id"]
        ids = await self.api_server.links.run(task["url"])
        detail_id = ids[0] if ids else self._extract_detail_id_from_url(task["url"])
        if not detail_id:
            raise HTTPException(status_code=400, detail=f"提取作品ID失败: {task['url']}")
        await self.repo.update_task_resolved(task["id"], detail_id)
        return detail_id

    @staticmethod
    def _extract_detail_id_from_url(url: str) -> str:
        try:
            parsed = urlparse(url)
            query = parse_qs(parsed.query)
        except Exception:
            parsed = None
            query = {}

        for key in ("modal_id", "item_id", "aweme_id"):
            value = query.get(key, [""])[0]
            if value and value.isdigit():
                return value

        target = parsed.path if parsed else url
        if m := search(r"/video/(\d+)", target or ""):
            return m.group(1)
        if m := search(r"/note/(\d+)", target or ""):
            return m.group(1)
        return ""

    async def process_task(self, task_id: int) -> dict[str, Any]:
        task = await self.repo.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail="URL任务不存在")

        try:
            detail_id = await self.resolve_detail_id(task)
            await self.repo.update_task_status(task_id, "处理中", None)
            data = await self.api_server.comment_handle_single(
                detail_id,
                source=True,
            )
            if data is None:
                data = []
            await self.repo.upsert_comments(
                task_id,
                task["url"],
                detail_id,
                task["fetch_type"],
                data,
            )
            await self.repo.update_task_status(task_id, "处理完成", None)
            return {
                "task_id": task_id,
                "detail_id": detail_id,
                "fetch_type": task["fetch_type"],
                "count": len(data),
            }
        except HTTPException:
            await self.repo.update_task_status(task_id, "处理失败", "采集失败")
            raise
        except Exception as e:
            await self.repo.update_task_status(task_id, "处理失败", str(e))
            raise HTTPException(status_code=500, detail=f"采集失败: {e}") from e


class CommentTaskDispatcher:
    def __init__(self, controller: CommentCenterController):
        self.controller = controller
        self.queue: asyncio.Queue[int] = asyncio.Queue()
        self.queued_ids: set[int] = set()
        self.worker_task: asyncio.Task | None = None
        self.lock = asyncio.Lock()

    async def ensure_worker(self):
        if self.worker_task is None or self.worker_task.done():
            self.worker_task = asyncio.create_task(self._worker_loop())

    async def enqueue(self, task_id: int) -> bool:
        await self.ensure_worker()
        async with self.lock:
            if task_id in self.queued_ids:
                return False
            self.queued_ids.add(task_id)
            await self.queue.put(task_id)
            return True

    async def close(self):
        if self.worker_task and not self.worker_task.done():
            self.worker_task.cancel()
            try:
                await self.worker_task
            except asyncio.CancelledError:
                pass

    async def _worker_loop(self):
        while True:
            task_id = await self.queue.get()
            try:
                await self.controller.process_task(task_id)
            except Exception:
                # 任务状态已在 process_task 内更新，这里吞掉异常避免 worker 退出
                pass
            finally:
                async with self.lock:
                    self.queued_ids.discard(task_id)
                self.queue.task_done()


def _split_urls(req: UrlCreateRequest) -> list[str]:
    urls: list[str] = []
    if req.url.strip():
        urls.append(req.url.strip())
    if req.urls_text.strip():
        for line in req.urls_text.splitlines():
            s = line.strip()
            if s:
                urls.append(s)
    # 保序去重
    seen = set()
    result = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            result.append(u)
    return result


def register_comment_center_routes(app: FastAPI, api_server):
    controller = CommentCenterController(api_server)
    dispatcher = CommentTaskDispatcher(controller)
    initialized = False

    async def ensure_storage_ready():
        nonlocal initialized
        if initialized:
            return
        await controller.repo.ensure_tables()
        initialized = True

    @app.on_event("shutdown")
    async def _shutdown_comment_center_dispatcher():
        await dispatcher.close()

    @app.get("/comment-center", tags=["评论中心"])
    async def comment_center_home():
        return RedirectResponse(url="/comment-center/urls")

    @app.get("/comment-center/urls", response_class=HTMLResponse, tags=["评论中心"])
    async def comment_center_urls_page():
        page = Path(__file__).resolve().parents[2] / "static" / "comment_center_urls.html"
        if not page.exists():
            return HTMLResponse("<h1>comment_center_urls.html not found</h1>", status_code=500)
        return HTMLResponse(page.read_text(encoding="utf-8"))

    @app.get("/comment-center/comments", response_class=HTMLResponse, tags=["评论中心"])
    async def comment_center_comments_page():
        page = Path(__file__).resolve().parents[2] / "static" / "comment_center_comments.html"
        if not page.exists():
            return HTMLResponse("<h1>comment_center_comments.html not found</h1>", status_code=500)
        return HTMLResponse(page.read_text(encoding="utf-8"))

    @app.get("/comment-center/api/types", tags=["评论中心"])
    async def list_types():
        await ensure_storage_ready()
        return {"data": await controller.repo.list_types()}

    @app.post("/comment-center/api/types", tags=["评论中心"])
    async def create_type(item: TypeCreateRequest):
        await ensure_storage_ready()
        name = item.name.strip()
        if not name:
            raise HTTPException(status_code=400, detail="类型不能为空")
        await controller.repo.add_type(name)
        return {"ok": True}

    @app.delete("/comment-center/api/types", tags=["评论中心"])
    async def remove_type(name: str = Query(...)):
        await ensure_storage_ready()
        n = name.strip()
        if not n:
            raise HTTPException(status_code=400, detail="类型不能为空")
        await controller.repo.delete_type(n)
        return {"ok": True}

    @app.get("/comment-center/api/url-list", tags=["评论中心"])
    async def list_urls(
        fetch_type: str = Query(default=""),
        status: str = Query(default=""),
        page: int = Query(default=1),
        page_size: int = Query(default=20),
    ):
        await ensure_storage_ready()
        return await controller.repo.list_url_tasks(
            fetch_type=fetch_type.strip(),
            status=status.strip(),
            page=page,
            page_size=page_size,
        )

    @app.post("/comment-center/api/url-list", tags=["评论中心"])
    async def create_url(item: UrlCreateRequest):
        await ensure_storage_ready()
        fetch_type = item.fetch_type.strip()
        if not fetch_type:
            raise HTTPException(status_code=400, detail="获取类型不能为空")
        if not await controller.repo.type_exists(fetch_type):
            raise HTTPException(status_code=400, detail="类型不存在，请先新增类型")

        urls = _split_urls(item)
        if not urls:
            raise HTTPException(status_code=400, detail="URL 不能为空")

        inserted = 0
        duplicated = 0
        ids = []
        for url in urls:
            task_id, created = await controller.repo.insert_url_task(url, fetch_type)
            ids.append(task_id)
            if created:
                inserted += 1
            else:
                duplicated += 1
        return {"inserted": inserted, "duplicated": duplicated, "ids": ids}

    @app.post("/comment-center/api/collect-one", tags=["评论中心"])
    async def collect_one(req: CollectOneRequest):
        await ensure_storage_ready()
        task = await controller.repo.get_task(req.task_id)
        if not task:
            raise HTTPException(status_code=404, detail="URL任务不存在")
        if task["status"] in {"等待中", "处理中"}:
            return {"ok": True, "message": "任务已在队列中或处理中"}
        await controller.repo.update_task_status(req.task_id, "等待中", None)
        queued = await dispatcher.enqueue(req.task_id)
        return {"ok": True, "queued": queued, "message": "任务后台处理中"}

    @app.post("/comment-center/api/collect-all", tags=["评论中心"])
    async def collect_all(req: CollectAllRequest):
        await ensure_storage_ready()
        fetch_type = req.fetch_type.strip()
        if not fetch_type:
            raise HTTPException(status_code=400, detail="获取类型不能为空")
        task_ids = await controller.repo.list_url_task_ids_by_type(fetch_type)
        queued = 0
        skipped = 0
        for task_id in task_ids:
            task = await controller.repo.get_task(task_id)
            if not task:
                continue
            if task["status"] in {"等待中", "处理中"}:
                skipped += 1
                continue
            await controller.repo.update_task_status(task_id, "等待中", None)
            if await dispatcher.enqueue(task_id):
                queued += 1
            else:
                skipped += 1
        return {"ok": True, "queued": queued, "skipped": skipped, "message": "任务后台处理中"}

    @app.get("/comment-center/api/comments", tags=["评论中心"])
    async def list_comments(
        detail_id: str = Query(default=""),
        fetch_type: str = Query(default=""),
        time_range: str = Query(default=""),
        keyword: str = Query(default=""),
        regions: str = Query(default=""),
        page: int = Query(default=1),
        page_size: int = Query(default=20),
    ):
        await ensure_storage_ready()
        days = None
        if time_range in {"1", "3", "7"}:
            days = int(time_range)
        region_list = [r.strip() for r in regions.split(",") if r.strip()]
        return await controller.repo.list_comments(
            detail_id=detail_id.strip(),
            fetch_type=fetch_type.strip(),
            days=days,
            keyword=keyword.strip(),
            regions=region_list,
            page=page,
            page_size=page_size,
        )

    @app.get("/comment-center/api/comments/export", tags=["评论中心"])
    async def export_comments(
        detail_id: str = Query(default=""),
        fetch_type: str = Query(default=""),
        time_range: str = Query(default=""),
        keyword: str = Query(default=""),
        regions: str = Query(default=""),
    ):
        await ensure_storage_ready()
        days = None
        if time_range in {"1", "3", "7"}:
            days = int(time_range)
        region_list = [r.strip() for r in regions.split(",") if r.strip()]
        rows = await controller.repo.list_comments_for_export(
            detail_id=detail_id.strip(),
            fetch_type=fetch_type.strip(),
            days=days,
            keyword=keyword.strip(),
            regions=region_list,
        )

        wb = Workbook()
        wb.remove(wb.active)
        chunk_size = 5000
        headers = ["UID", "用户昵称", "地区", "评论时间", "评论内容"]
        if not rows:
            ws = wb.create_sheet("Data_1")
            ws.append(headers)
        else:
            for idx in range(0, len(rows), chunk_size):
                chunk = rows[idx : idx + chunk_size]
                sheet_no = idx // chunk_size + 1
                ws = wb.create_sheet(f"Data_{sheet_no}")
                ws.append(headers)
                for row in chunk:
                    ws.append(
                        [
                            row["uid"],
                            row["nickname"],
                            row["ip_label"],
                            row["comment_time_display"],
                            row["text"],
                        ]
                    )

        bio = BytesIO()
        wb.save(bio)
        bio.seek(0)
        filename = f"comments_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return StreamingResponse(
            bio,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
