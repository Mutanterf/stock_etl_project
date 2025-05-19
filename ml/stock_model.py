import pandas as pd
from clickhouse_driver import Client
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

client = Client('localhost')

query = "SELECT open, high, low, close, volume FROM stock.daily_stock"
data = client.execute(query)
df = pd.DataFrame(data, columns=["open", "high", "low", "close", "volume"])

X = df[["open", "high", "low", "volume"]]
y = df["close"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

model = LinearRegression()
model.fit(X_train, y_train)

print("R^2 score:", model.score(X_test, y_test))
