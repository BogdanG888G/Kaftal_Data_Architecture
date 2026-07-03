import pandas as pd
import clickhouse_connect
from config import config


class ClickHouseClient:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host=config.CH_HOST,
            port=config.CH_PORT,
            username=config.CH_USER,
            password=config.CH_PASSWORD,
            database=config.CH_DATABASE,
        )

    def query(self, sql: str) -> pd.DataFrame:
        result = self.client.query(sql)
        return pd.DataFrame(result.result_rows, columns=result.column_names)

    def get_schema(self) -> str:
        sql = f"DESCRIBE TABLE {config.CH_DATABASE}.{config.CH_TABLE}"
        df = self.query(sql)

        lines = [f"Таблица: {config.CH_DATABASE}.{config.CH_TABLE}", "Колонки:"]
        for _, row in df.iterrows():
            lines.append(f"- {row['name']}: {row['type']}")
        return "\n".join(lines)

    def get_sample(self, n: int = 3) -> str:
        sql = f"SELECT * FROM {config.CH_DATABASE}.{config.CH_TABLE} LIMIT {n}"
        df = self.query(sql)
        return df.to_string(index=False, max_cols=12)


db = ClickHouseClient()