from datetime import datetime
from typing import List

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from databuilder.models.table_stats import TableColumnStats


def get_numeric_stats(df: DataFrame, col: str, count: int) -> dict:
    stats = df.select(F.round(F.mean(col)).alias('mean'),
                      F.round(F.stddev(col)).alias('std dev'),
                      F.min(col).alias('min'),
                      F.max(col).alias('max'),
                      (F.count(F.when(F.isnan(col) | F.col(col).isNull(), col)) / count).alias('null %')) \
        .collect()[0].asDict()
    quantiles = df.approxQuantile(col, [0.25, 0.5, 0.75], 0.2)
    stats['25%'], stats['50%'], stats['75%'] = quantiles[0], quantiles[1], quantiles[2]
    return stats


def get_string_stats(df: DataFrame, col: str, count: int) -> dict:
    stats = df.select(F.approx_count_distinct(col).alias("distinct values"),
                      (F.count(F.when(F.isnan(col) | F.col(col).isNull(), col)) / count).alias('null %')) \
        .collect()[0].asDict()
    max_val = df.groupby(col).count().sort(F.desc('count')).collect()[0].asDict()
    stats['null %'] = round(stats['null %'], 2) * 100
    stats['most freq value'] = max_val[col]
    stats['most freq %'] = round(max_val['count'] / count, 2) * 100
    return stats


def get_datetime_stats(df: DataFrame, col: str, count: int) -> dict:
    stats = df.select(F.min(col).alias('min'),
                      F.max(col).alias('max'))
    return stats


def get_stats_by_column(bucket_name: str, table_name: str, col_name: str, df: DataFrame, count: int) -> List[TableColumnStats]:
    column_stats = []
    start_epoch = datetime.now().timestamp()
    schema = dict(df.dtypes)
    col_type = schema[col_name]
    stats = stat_func_by_type[col_type](df, col_name, count)
    for stat in stats:
        column_stat = TableColumnStats(table_name=table_name,
                                       col_name=col_name,
                                       stat_name=stat,
                                       stat_val='"' + str(stats[stat]) + '"',
                                       start_epoch=start_epoch,
                                       end_epoch=datetime.now().timestamp(),
                                       db='minio',
                                       cluster=bucket_name,
                                       schema='minio'
                                       )
        print("column stat: ", column_stat.__dict__)
        column_stats.append(column_stat)
    return column_stats


stat_func_by_type = {
    'double': get_numeric_stats,
    'int': get_numeric_stats,
    'string': get_string_stats,
    'datetime': get_datetime_stats
}
