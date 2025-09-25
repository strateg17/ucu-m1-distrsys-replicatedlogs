PROJECT=distributed_system

# ------------------------
# Управління кластером
# ------------------------
build:
	docker-compose build

up:
	docker-compose up --build -d

down:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f

logs-master:
	docker logs -f master

logs-s1:
	docker logs -f secondary1

logs-s2:
	docker logs -f secondary2

# ------------------------
# Тести реплікації
# ------------------------

# Тест w=1 (відповідь одразу від Master)
test-w1:
	curl -X POST http://localhost:8000/message \
		-H "Content-Type: application/json" \
		-d '{"text": "Message W1", "w": 1}'

# Тест w=2 (Master + один Secondary)
test-w2:
	curl -X POST http://localhost:8000/message \
		-H "Content-Type: application/json" \
		-d '{"text": "Message W2", "w": 2}'

# Тест w=3 (Master + два Secondaries)
test-w3:
	curl -X POST http://localhost:8000/message \
		-H "Content-Type: application/json" \
		-d '{"text": "Message W3", "w": 3}'

# ------------------------
# Перевірка consistency
# ------------------------
show-master:
	curl http://localhost:8000/messages

show-s1:
	curl http://localhost:8001/messages

show-s2:
	curl http://localhost:8002/messages
