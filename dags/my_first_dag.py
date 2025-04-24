from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split 
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error,mean_squared_error


default_args = {
    'owner':'coder2j',
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

def download_data():
    df = pd.read_csv("https://raw.githubusercontent.com/datasciencedojo/datasets/refs/heads/master/titanic.csv")
    df.to_csv("titanic.csv", index = False)
    print("df: ", df.shape)
    print('Succes download data!')

def preprocessing_data():
    df = pd.read_csv("titanic.csv")
    df = df.dropna(axis=0, subset=['Embarked'])
    df = df.drop(columns=['Cabin'])
    df['Age'] = df['Age'].fillna(df['Age'].median())
    cathegorical_columns = df.select_dtypes(include=['object']).columns


    for col in cathegorical_columns:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col])
    

    df.to_csv("prep_titanic.csv", index = False)
   
    print("Succes preprocessing_data!")

def train():
    df = pd.read_csv("prep_titanic.csv")
    X = df.drop(columns=['Survived'])
    y = df['Survived']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model  =LinearRegression()
    model = model.fit(X_train,y_train)
    
    mae = mean_absolute_error(y_test,model.predict(X_test))
    mse = mean_squared_error(y_test,model.predict(X_test))
    print(f"MAE:{mae}, MSE {mse}")
    print("Succes train model!")


with DAG(
    default_args = default_args,
    dag_id = 'fffff',
    description = "dggdgsg",
    start_date = datetime(2025,4,23),
    schedule_interval = '@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'download_data',
        python_callable = download_data
    )
    task2 = PythonOperator(
        task_id = "preprocessing_data",
        python_callable = preprocessing_data
    )
    task3 = PythonOperator(
        task_id = "train_model",
        python_callable = train
    )
    task1>>task2>>task3