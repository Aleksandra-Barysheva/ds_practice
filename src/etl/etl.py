import pandas as pd
import os
import sys
from .src.kaggle_data_loader import download_data
from .src.transform import Transform


RELEVANT_TABLES = ["sales_train",
                   "items",
                   "item_categories",
                   "shops",
                   "test"]


class RelationsTransform(Transform):
    def __init__(self, map_pipes=tuple(), filter_pipes=tuple(),):
        self.map_pipes = map_pipes
        self.filter_pipes = filter_pipes

    def apply_map(self, relations):
        return self._apply_pipes(relations, self.map_pipes)

    def apply_filter(self, relations):
        return self._apply_pipes(relations, self.filter_pipes)

    def apply_reduce(self,  relations):
        merged_relations_train = (relations["sales_train"]
                                  .merge(relations["items"], on='item_id', how='left')
                                  .merge(relations["item_categories"], on='item_category_id', how='left')
                                  .merge(relations["shops"], on='shop_id', how='left')
                                  )

        merged_relations_test = (
            relations["test"]
            .merge(relations["items"], on='item_id', how='left')
            .merge(relations["item_categories"], on='item_category_id', how='left')
            .merge(relations["shops"], on='shop_id', how='left')
        )
        return merged_relations_train, merged_relations_test

    def _apply_pipes(self, relations, pipe):
        for key in relations:
            if key in pipe:
                pipe = pipe[key]
                relations[key] = self._apply_pipe(relations[key], pipe)
        return relations

    def _apply_pipe(self, df, pipe):
        for f in pipe:
            df = f(df)
        return df


class ETL:
    def __init__(self, transform: Transform):
        self.transform = transform

    def extract(self):
        download_data()
        return self._get_relations()

    def load(self):
        "returns (train, test) tuple"
        relations = self.extract()
        relations = self.transform(relations)
        return relations

    def _get_relations(self):
        cwd = os.getcwd()
        src_dir = os.path.join(cwd, '..', 'src')
        sys.path.append(src_dir)
        dirname = f"{src_dir}/data"

        relations = {
            filename: pd.read_csv(f"{dirname}/{filename}.csv")
            for filename in RELEVANT_TABLES
        }

        return relations
