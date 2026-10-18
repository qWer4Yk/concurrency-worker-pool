# Concurrency Worker Pool with Rate Limiting & Graceful Shutdown

[![Go Version](https://img.shields.io/badge/Go-1.24%2B-blue.svg)](https://golang.org)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)]()
[![Race Detector](https://img.shields.io/badge/race-safe-success.svg)]()

Легковесная библиотека и CLI-демон на Go для параллельного выполнения задач (Worker Pool) с поддержкой ограничения скорости (Rate Limiting), потокобезопасных метрик, изоляции паник и безопасного завершения работы (Graceful Shutdown).

Проект написан в рамках практики конкурентного программирования на Go и подготовки к бэкенд-интервью.

---

## 🏗 Архитектура

```
                     ┌──────────────────────────────────────────────┐
                     │                 Worker Pool                  │
                     │                                              │
Task Submissions ───►│ [Task Queue (chan)] ──► Rate Limiter (Ticker)│
                     │                                │             │
                     │           ┌────────────────────┴──────────┐  │
                     │           ▼           ▼                   ▼  │
                     │      [Worker 0]  [Worker 1]  ...    [Worker N│  │
                     │           │           │                   │  │
                     │     (Panic Catch) (Panic Catch)     (Panic Catch
                     │           │           │                   │  │
                     │           └───────────┬───────────────────┘  │
                     │                       ▼                      │
                     │            [Results Queue (chan)] ───────────┼──► Output Stream
                     │                       │                      │
                     │            [Atomic Metrics Engine]           │
                     └──────────────────────────────────────────────┘
```

### Ключевые возможности:
* **Изоляция паник в воркерах:** Если пользовательская функция бросает `panic`, воркер перехватывает её через `recover()`, инкрементирует счетчик `panics`, логирует ошибку и продолжает работу без падения всего процесса.
* **Rate Limiting:** Опциональное ограничение RPS через тикер (защита внешних API от превышения лимитов/429 Too Many Requests).
* **Потокобезопасная телеметрия:** Сбор статистики (`Submitted`, `Completed`, `Failed`, `Panics`) на примитивах `sync/atomic`.
* **Graceful Shutdown:** При вызове `pool.Stop()` закрывается входная очередь, воркеры дорабатывают оставшиеся задачи, закрывается очередь результатов и завершается контекст.

---

## 🚀 Быстрый старт

### Запуск демо
```bash
go run ./cmd/demo/main.go
```

### Запуск тестов (с проверкой на состояние гонки)
```bash
go test -v -race ./...
```

### Запуск бенчмарков
```bash
go test -bench=. ./pkg/workerpool
```
*Результаты на Ryzen 7 5800U: ~1 000 000 ops / 1044 ns/op.*

---

## ⚠️ Known Issues & Ограничения (Текущий техдолг)

Проект создавался для учебных и практических целей, поэтому имеет осознанные архитектурные компромиссы:

1. **In-Memory очередь (Потеря задач при аварии):**
   * *Проблема:* Задачи хранятся в буферизованном Go-канале в оперативной памяти. Если процесс убить через `kill -9` или упадет сервер, невыполненные задачи потеряются.
   * *Решение для продакшна:* Вынести очередь в Redis Streams или RabbitMQ с подтверждением обработки (ACK).
2. **Локальный Rate Limiter:**
   * *Проблема:* `time.Ticker` работает локально в памяти одного экземпляра сервиса. При горизонтальном масштабировании (например, 5 подов в Kubernetes) суммарный RPS на внешние API вырастет в 5 раз.
   * *Решение:* Реализовать распределенный Rate Limiter на Redis с алгоритмом Sliding Window или Token Bucket.
3. **Отсутствие Retry Policy с Exponential Backoff:**
   * *Проблема:* Если внешнее API вернуло временную сетевую ошибку (504 / Connection Reset), задача сразу переходит в статус `Failed`.
   * *Решение:* Добавить конфигурацию повторных попыток (Retries: 3, Backoff: 100ms * 2^attempt).
4. **Статический размер пула воркеров:**
   * *Проблема:* Количество воркеров задается при старте и не умеет автоматически масштабироваться вверх/вниз в зависимости от длины очереди (Queue Backlog).

---

## 🛠 Пример использования в коде

```go
package main

import (
	"context"
	"fmt"
	"github.com/dmitriy-dev/concurrency-worker-pool/pkg/workerpool"
)

func main() {
	pool := workerpool.New(workerpool.Config{
		Workers:   4,
		QueueSize: 100,
		RPS:       50,
	})

	_ = pool.Submit(workerpool.NewTask("task-1", func(ctx context.Context) (any, error) {
		return "data processed", nil
	}))

	go pool.Stop()

	for res := range pool.Results() {
		fmt.Printf("Task %s: result=%v, err=%v\n", res.TaskID, res.Output, res.Err)
	}
}
```
