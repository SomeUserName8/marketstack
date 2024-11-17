# Marketstack тестовое
Credentials:

Airflow:
User: admin
Passowd: admin

Postgres:
host: localhost
port: 5431
user: user
password: user
db: postgres

В DDS построен DataVault, потому что такая модель данных отлично подходит для масштабирования модели, в отличии от звезды или снежинки

![image](https://github.com/user-attachments/assets/53b4c7b5-7e63-476d-b393-06f398906d31)

В airflow добавлены проверки в stage слое на наличие пустых значений полей, которые должны быть ключами. Для тест кейсов отлично бы вписался фреймворк dbt, где dbt test можно дергать bash операторами из airflow и преиспользовать написанный тест для нескольких полей/таблиц

![image](https://github.com/user-attachments/assets/4872e1f4-8e32-4beb-92cd-8c9f083f3c19)






