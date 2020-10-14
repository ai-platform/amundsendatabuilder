import pyspark.sql.functions as F
from typing import List


def get_numeric_stats(df, col, count) -> List[dict]:
    stats = df.select(F.round(F.mean(col)).alias('mean'),
                      F.round(F.stddev(col)).alias('std dev'),
                      F.min(col).alias('min'),
                      F.max(col).alias('max'),
                      (F.count(F.when(F.isnan(col) | F.col(col).isNull(), col)) / count).alias('null pct')) \
        .collect()[0].asDict()
    quantiles = df.approxQuantile(col, [0.25, 0.5, 0.75], 0.2)
    stats['25 pct'], stats['50 pct'], stats['75 pct'] = quantiles[0], quantiles[1], quantiles[2]
    return stats


def get_string_stats(df, col, count) -> List[dict]:
    stats = df.select(F.approx_count_distinct(col).alias("distinct values"),
                      (F.count(F.when(F.isnan(col) | F.col(col).isNull(), col)) / count).alias('null pct')) \
        .collect()[0].asDict()
    max_val = df.groupby(col).count().sort(F.desc('count')).collect()[0].asDict()
    stats['most freq value'] = max_val[col]
    stats['most freq pct'] = round(max_val['count'] / count, 2)
    return stats


def get_datetime_stats(df, col, count) -> List[dict]:
    stats = df.select(F.min(col).alias('min'),
                      F.max(col).alias('max'))
    return stats


def get_stats_by_column(table_name, col_name, col_type, df, count) -> List[dict]:
    stat_objs = []
    stat_values = stat_func_by_type[col_type](df, col_name, count)
    for stat in stat_values:
        stat_obj = {
            'table_name': table_name,
            'column_name': col_name,
            'stat_name': stat,
            'stat_val': str(stat_values[stat])
        }
        stat_objs.append(stat_obj)
    return stat_objs


stat_func_by_type = {
    'double': get_numeric_stats,
    'int': get_numeric_stats,
    'string': get_string_stats,
    'datetime': get_datetime_stats
}
