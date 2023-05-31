import pandas as pd
from .src.dqc_template import DataQualityPipelineTemplate
from itertools import combinations


class DataQualityPipeline(DataQualityPipelineTemplate):

    def validation_report(self, relations):
        val_report = {}
        val_report["values"] = self._values_report(relations)
        val_report["sparcity"] = self._sparcity_report(relations)
        val_report["duplicates"] = self._duplicates(relations)

        return val_report

    def stats_report(self, relations):
        stats_report = {}
        stats_report["distribution"] = self._distribution_report(relations)
        stats_report["outliers"] = self._outliers_report(
            stats_report["distribution"])
        return stats_report

    def create_report(self, val, stats):
        report = {
            "validation report": val,
            "statistical report": stats
        }
        return report

    def _duplicates(self, relations):
        d_tables = []
        keys = sorted(relations.keys())
        for key in keys:
            d_tables.append(relations[key].duplicated().sum())
        res = pd.DataFrame(data=d_tables, index=keys, columns=["duplicates"])

        return res

    def _values_report(self, relations):
        report = []
        for t_name, df in relations.items():
            nan_report = df.isna().sum(0) / len(df)
            unique_report = df.nunique()
            types = df.infer_objects().dtypes.astype(str)
            total = [len(df) for _ in range(len(df.columns))]

            report.append(pd.DataFrame(
                data=[
                    nan_report.values,
                    unique_report.values,
                    types,
                    total
                ],
                index=['nan_report', 'unique', 'dtype', 'total'],
                columns=pd.MultiIndex.from_tuples([(t_name, col) for col in df.columns],
                                                  names=[
                                                  "table_name", "column_name"])
            ))

        return (pd.concat(report, axis=1).
                transpose().
                reset_index().
                sort_values('table_name').
                set_index(["table_name", "column_name"])
                )

    def _outliers_report(self, d_report):
        left_range = d_report["1%"] - d_report["min"]
        middle_range = d_report["99%"] - d_report["1%"]
        right_range = d_report["max"] - d_report["99%"]

        cols = ["1% left range", "98% middle range", "1% right range"]
        res = pd.concat([left_range, middle_range, right_range], axis=1)
        res.columns = cols
        return res

    def _distribution_report(self, relations):
        """function get relation dict and report DataFrame to create 
        DataFrame with percentile columns and for numeric features"""

        # since numerical features represented only by floats
        dtypes = ["float", "float16", "float32", "float64"]
        quantiles = [0.01, 0.25, 0.5, 0.75, 0.99]

        q_tables = []
        for key in relations:
            data = relations[key].select_dtypes(include=dtypes)
            if not data.empty:
                q_tables.append(data.describe(quantiles))

        return pd.concat(q_tables, axis=1).transpose()

    def _sparcity_report(self, relations):
        """function get relation dict to create
        DataFrame with 'left_only', 'intersect', 'right_only' count columns
        for each pair of tables, joined by the corresponding col"""

        data = {}
        joins = self._get_joins(relations)
        for join in joins:
            l_table, r_table = join["pair"]
            cols = join["keys"]
            for col in cols:
                cmp_key = f"{l_table}, {r_table} ON {col}"

                l_col = set(relations[l_table][col])
                r_col = set(relations[r_table][col])
                inner = l_col & r_col

                li = len(inner)
                row = (len(l_col) - li, li, len(r_col) - li)
                data[cmp_key] = row

        columns = ['left_only', 'intersect', 'right_only']
        return pd.DataFrame.from_dict(data,
                                      columns=columns,
                                      orient='index')

    def _get_joins(self, relations):
        joins = []

        for r1, r2 in combinations(relations.keys(), 2):
            r1_cols = set(relations[r1].columns)
            r2_cols = set(relations[r2].columns)
            intersection = r1_cols.intersection(r2_cols)
            if intersection:
                joins.append(
                    {"pair": (r1, r2),
                     "keys": intersection}
                )
        return joins
