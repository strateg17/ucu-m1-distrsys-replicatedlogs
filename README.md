# UCU Module 1 Distributed Systems Replication Logs Homework

## Запуск систем
У корені проекту виконуємо команду 

```bash
# 1. Зібрати образи та підняти контейнери
docker-compose up --build
```

Після цього мають піднятись 3 сервіси:
* master → порт 8000;
* secondary1 → порт 8001;
* secondary2 → порт 8002;

## Тестування 
### 4.1. Додати повідомлення з w=1 (миттєвий успіх)
```bash
curl -X POST http://localhost:8000/message \
  -H "Content-Type: application/json" \
  -d '{"text": "Hello with w=1", "w": 1}'

```
Очікувана відповідь:
```json
{"status":"ok","message_id":1,"confirmations":1,"required":1}
```

### 4.2 Додати повідомлення з w=2 (чекає хоча б одного Secondary)
```bash
curl -X POST http://localhost:8000/message \
  -H "Content-Type: application/json" \
  -d '{"text": "Hello with w=2", "w": 2}'

```

### 4.3 Додати повідомлення з w=3 (чекає всіх Secondary)
```bash
curl -X POST http://localhost:8000/message \
  -H "Content-Type: application/json" \
  -d '{"text": "Hello with w=3", "w": 3}'

```
### 4.4 4.4. Перевірити стан Master, Secondary1, Secondary2
```bash
curl http://localhost:8000/messages
curl http://localhost:8001/messages
curl http://localhost:8002/messages
```

## 5. Тест відмовостійкості
### 5.1 Зупиняємо Secondary1

```bash
docker stop secondary1
```
### 5.2 Відпраляємо повідомлення з w=1
```bash
curl -X POST http://localhost:8000/message \
  -H "Content-Type: application/json" \
  -d '{"text": "Msg while S1 down", "w": 1}'

```
➡️ Master одразу відповість успіхом, повідомлення піде у pending для Secondary1.


### 5.3 Запускаємо Secondary1 знову
```bash
docker start secondary1
```
➡️ Secondary1 при старті підтягне від Master пропущені повідомлення через /sync.
➡️ Master також відправить pending чергу.

Перевірка:

```bash
curl http://localhost:8001/messages
```

Там має бути і повідомлення, яке Secondary пропустив. ✅