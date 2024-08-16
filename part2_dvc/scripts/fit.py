# scripts/fit.py

import os

import joblib
import pandas as pd
import yaml
from sklearn import svm
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


def fit_model():
    with open('params.yaml', 'r') as fd:
        params = yaml.safe_load(fd)

    data = pd.read_csv('data/initial_data.csv')

    num_features = data.select_dtypes(['float'])
    num_cols = num_features.columns.tolist()

    preprocessor = ColumnTransformer(
        [
            ('num', StandardScaler(), num_cols)
        ],
        remainder='drop',
        verbose_feature_names_out=False
    )

    model = svm.SVR()

    pipeline = Pipeline(
        [
            ('preprocessor', preprocessor),
            ('model', model)
        ]
    )

    pipeline.fit(data, data[params['target_col']])
   
    os.makedirs('models', exist_ok=True)
    with open('models/fitted_model.pkl', 'wb') as fd:
        joblib.dump(pipeline, fd)


if __name__ == '__main__':
    fit_model()
