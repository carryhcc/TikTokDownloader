from pathlib import Path
from re import search, sub

try:
    from aiomysql import connect
except ImportError:  # pragma: no cover - runtime fallback
    connect = None

from ..tools import DownloaderError
from .sql import BaseSQLLogger

__all__ = ["MySQLLogger"]


class MySQLLogger(BaseSQLLogger):
    """MySQL 数据库保存数据（按 SQLite 字段规则建表）"""

    def __init__(
        self,
        root: Path,
        db_name: str,
        title_line: tuple,
        title_type: tuple,
        field_keys: tuple,
        old=None,
        name="Download",
        storage_type="detail",
        mysql_host="127.0.0.1",
        mysql_port=3306,
        mysql_user="root",
        mysql_password="",
        mysql_database="douk_downloader",
        mysql_table="",
        mysql_comment_table="comment_data",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.db = None
        self.cursor = None
        self.name = (old, name)
        self.file = db_name
        self.path = root.joinpath(self.file)
        self.title_line = title_line
        self.title_type = title_type
        self.field_keys = field_keys
        self.storage_type = storage_type

        self.mysql_host = mysql_host
        self.mysql_port = mysql_port
        self.mysql_user = mysql_user
        self.mysql_password = mysql_password
        self.mysql_database = mysql_database
        self.mysql_table = mysql_table
        self.mysql_comment_table = mysql_comment_table

    async def __aenter__(self):
        if connect is None:
            raise DownloaderError(
                "未安装 aiomysql 依赖，无法使用 MySQL 储存；请先安装 aiomysql。"
            )
        try:
            await self._create_database_if_not_exists()
            self.db = await connect(
                host=self.mysql_host,
                port=self.mysql_port,
                user=self.mysql_user,
                password=self.mysql_password,
                db=self.mysql_database,
                charset="utf8mb4",
                autocommit=True,
            )
            self.cursor = await self.db.cursor()
            if self.storage_type != "comment":
                await self.update_sheet()
            await self.create()
            return self
        except Exception as e:
            raise DownloaderError(
                f"MySQL 连接失败，请检查配置：{self.mysql_host}:{self.mysql_port}/"
                f"{self.mysql_database}，错误信息：{e}"
            ) from e

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.cursor:
            await self.cursor.close()
        if self.db:
            self.db.close()

    async def _create_database_if_not_exists(self):
        db = await connect(
            host=self.mysql_host,
            port=self.mysql_port,
            user=self.mysql_user,
            password=self.mysql_password,
            charset="utf8mb4",
            autocommit=True,
        )
        try:
            async with db.cursor() as cursor:
                await cursor.execute("SET sql_notes = 0;")
                await cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{self.mysql_database}` "
                    "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
                )
                await cursor.execute("SET sql_notes = 1;")
        finally:
            db.close()

    async def create(self):
        await self.cursor.execute("SET sql_notes = 0;")
        if self.storage_type == "comment":
            await self._create_comment_table()
            await self.cursor.execute("SET sql_notes = 1;")
            return
        columns = ", ".join(
            [f"`{col}` {self._map_type(tp)}" for col, tp in zip(self.title_line, self.title_type)]
        )
        create_sql = f"CREATE TABLE IF NOT EXISTS `{self.name}` ({columns});"
        await self.cursor.execute(create_sql)
        await self.cursor.execute("SET sql_notes = 1;")

    async def _save(self, data, *args, **kwargs):
        if self.storage_type == "comment":
            await self._save_comment(data)
            return
        cols_sql = ", ".join([f"`{col}`" for col in self.title_line])
        placeholders = ", ".join(["%s" for _ in self.title_line])
        insert_sql = f"REPLACE INTO `{self.name}` ({cols_sql}) VALUES ({placeholders});"
        await self.cursor.execute(insert_sql, data)

    async def _create_comment_table(self):
        comment_columns = ", ".join(
            [f"`{col}` {self._map_type(tp)} NULL" for col, tp in zip(self.title_line, self.title_type)]
        )
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{self.mysql_comment_table}` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `作品ID` VARCHAR(32) NOT NULL,
            {comment_columns},
            `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (`id`),
            KEY `idx_作品ID` (`作品ID`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        await self.cursor.execute(create_sql)

    async def _save_comment(self, data):
        work_id = self._extract_work_id(self.name[-1])
        columns = ["作品ID", *self.title_line]
        values = [work_id, *data]
        cols_sql = ", ".join([f"`{col}`" for col in columns])
        placeholders = ", ".join(["%s" for _ in values])
        await self.cursor.execute(
            f"INSERT INTO `{self.mysql_comment_table}` ({cols_sql}) VALUES ({placeholders});",
            values,
        )

    async def update_sheet(self):
        old_sheet, new_sheet = self.__clean_sheet_name(self.name)
        mark = new_sheet.split("_", 1)
        if not old_sheet or mark[-1] == old_sheet:
            self.name = new_sheet
            return
        mark[-1] = old_sheet
        old_sheet = "_".join(mark)
        if await self.__check_sheet_exists(old_sheet):
            await self.cursor.execute(f"RENAME TABLE `{old_sheet}` TO `{new_sheet}`;")
        self.name = new_sheet

    async def __check_sheet_exists(self, sheet: str) -> bool:
        await self.cursor.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """,
            (self.mysql_database, sheet),
        )
        exists = await self.cursor.fetchone()
        return exists[0] > 0

    @staticmethod
    def _map_type(sqlite_type: str) -> str:
        mapping = {
            "INTEGER": "BIGINT",
            "REAL": "DOUBLE",
            "TEXT": "LONGTEXT",
            "BLOB": "LONGBLOB",
        }
        return mapping.get((sqlite_type or "").upper(), "LONGTEXT")

    def __clean_sheet_name(self, name: tuple) -> tuple:
        return self.__clean_characters(name[0]), self.__clean_characters(name[1])

    def __clean_characters(self, text: str | None) -> str | None:
        if isinstance(text, str):
            text = self.SHEET_NAME.sub("_", text)
            text = sub(r"_+", "_", text)
        return text

    @staticmethod
    def _extract_work_id(table_name: str) -> str:
        if m := search(r"作品(\d+)_", table_name or ""):
            return m.group(1)
        return ""
