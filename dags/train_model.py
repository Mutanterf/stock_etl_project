def train_model():
    from clickhouse_driver import Client
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import root_mean_squared_error
    import joblib

    client = Client(host='clickhouse', user='etl_user', password='strong_password_here')

    query = "SELECT timestamp, open, high, low, close, volume FROM stock.daily_stock"
    data = client.execute(query)
    df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    X = df[['open', 'high', 'low', 'volume']]
    y = df['close']

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    model = LinearRegression()
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    rmse = root_mean_squared_error(y_test, y_pred, squared=False)
    print(f"RMSE: {rmse}")

    joblib.dump(model, '/opt/airflow/dags/model.joblib')
