// Ops Visibility Service — Фаза 8 плана закрытия API-пробелов
// (luminous-hugging-charm.md): агрегированный вид consumer-group lag по
// Kafka + грид /readyz по всем сервисам платформы в одном месте, вместо
// ручного обхода каждого сервиса/группы по отдельности. Опрашивает Kafka
// Admin API и /readyz каждого сервиса на интервале, пишет короткоживущий
// снапшот в Redis (без истории — см. README.md), отдаёт тот же снапшот
// напрямую через GET /snapshot на :9090.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/KimMachineGun/automemlimit"

	"mpp/ops-visibility-service/internal/health"
	"mpp/ops-visibility-service/internal/httpapi"
	"mpp/ops-visibility-service/internal/kafkalag"
	"mpp/ops-visibility-service/internal/readyz"
	"mpp/ops-visibility-service/internal/services"
	"mpp/ops-visibility-service/internal/store"
)

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		log.Printf("не удалось разобрать %s=%q как duration, использую %s: %v", key, v, fallback, err)
		return fallback
	}
	return d
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Printf("не удалось разобрать %s=%q как int, использую %d: %v", key, v, fallback, err)
		return fallback
	}
	return n
}

// resolveServiceList — базовый список из internal/services.List, опционально
// расширенный EXTRA_SERVICES (через запятую) или полностью заменённый
// SERVICES_OVERRIDE (тоже через запятую) — оговорено в задаче: список
// должен быть тестируемым/гибким без редеплоя нового образа. Если задан и
// EXTRA_SERVICES, и SERVICES_OVERRIDE — override побеждает (EXTRA к
// заведомо иному списку не имеет смысла).
func resolveServiceList() []string {
	if override := os.Getenv("SERVICES_OVERRIDE"); override != "" {
		return splitCSV(override)
	}
	list := append([]string{}, services.List...)
	if extra := os.Getenv("EXTRA_SERVICES"); extra != "" {
		list = append(list, splitCSV(extra)...)
	}
	return list
}

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

// SERVICE_DNS_SUFFIX — по умолчанию ".mpp.svc" (k8s Service FQDN в
// пределах namespace). Реальная находка: локальный docker-compose стенд
// резолвит сервисы по голому имени контейнера (Docker embedded DNS,
// 127.0.0.11) — с захардкоженным ".mpp.svc" ЛЮБОЙ SERVICES_OVERRIDE всё
// равно бил в несуществующий хост ("no such host"), даже если сузить сам
// список сервисов; сама схема URL, не только список имён, должна быть
// настраиваемой. Пусто — не добавлять суффикс вообще (docker-compose).
// resolveDNSSuffix — реальная находка: env(key, fallback) (см. выше)
// трактует ЯВНО заданную пустую строку так же, как "переменная не
// задана" (os.Getenv возвращает "" в обоих случаях) — SERVICE_DNS_SUFFIX=""
// в docker-compose молча откатывался на дефолт ".mpp.svc", подтверждено
// живым /snapshot после редеплоя (generated_at обновлялся, URL — нет).
// Тот же класс "пустая строка неотличима от unset", что уже есть в этом
// helper'е повсеместно в кодбейзе — здесь используется явный sentinel
// "none" вместо попытки переизобрести env() только под этот один случай.
func resolveDNSSuffix() string {
	if v := os.Getenv("SERVICE_DNS_SUFFIX"); v == "none" {
		return ""
	} else if v != "" {
		return v
	}
	return ".mpp.svc"
}

func buildTargets(svcNames []string, dnsSuffix string) []readyz.Target {
	targets := make([]readyz.Target, len(svcNames))
	for i, name := range svcNames {
		targets[i] = readyz.Target{
			Service: name,
			URL:     fmt.Sprintf("http://%s%s:9090/readyz", name, dnsSuffix),
		}
	}
	return targets
}

func main() {
	healthState := &health.State{}
	mux := health.Router(healthState)

	// Интервалы опроса и таймауты — по умолчанию 20с для обоих циклов
	// (Kafka lag и readyz-грид): в диапазоне 15-30с, который задача
	// оставляет на усмотрение реализации, и совпадающий интервал для
	// обоих циклов даёт оператору один согласованный "снимок времени" в
	// /snapshot, не два независимо дрейфующих. SNAPSHOT_TTL (по умолчанию
	// 3 минуты = 9 циклов при 20с) — запас на пропуск пары циклов подряд
	// (временный сетевой блип) без того, чтобы /snapshot начинал врать
	// "всё ок" по данным пятнадцатиминутной давности, если сам сервис
	// упал: TTL истекает достаточно быстро, чтобы это было видно.
	kafkaPollInterval := envDuration("KAFKA_LAG_POLL_INTERVAL", 20*time.Second)
	readyzPollInterval := envDuration("READYZ_POLL_INTERVAL", 20*time.Second)
	readyzRequestTimeout := envDuration("READYZ_REQUEST_TIMEOUT", 3*time.Second)
	readyzConcurrency := envInt("READYZ_POLL_CONCURRENCY", 8)
	snapshotTTL := envDuration("SNAPSHOT_TTL", 3*time.Minute)
	kafkaPollTimeout := envDuration("KAFKA_LAG_POLL_TIMEOUT", 15*time.Second)

	redisDB := envInt("REDIS_RUNTIME_DB", 0)
	st := store.NewClient(
		env("REDIS_RUNTIME_HOST", "localhost")+":"+env("REDIS_RUNTIME_PORT", "6379"),
		env("REDIS_RUNTIME_PASSWORD", ""),
		redisDB,
		snapshotTTL,
	)
	defer st.Close()

	mux.HandleFunc("/snapshot", httpapi.SnapshotHandler(st))

	// Health-сервер стартует немедленно — readyz=503 до готовности, та же
	// конвенция, что у остальных Go-сервисов этой сессии.
	healthSrv := &http.Server{Addr: ":9090", Handler: mux}
	go func() {
		if err := healthSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("health server failed: %v", err)
		}
	}()

	brokers := splitCSV(env("KAFKA_BOOTSTRAP_SERVERS", "kafka-bootstrap.mpp.svc:9092"))
	kafkaClient, err := kafkalag.NewClient(brokers)
	if err != nil {
		log.Fatalf("не удалось создать Kafka admin client: %v", err)
	}
	defer kafkaClient.Close()

	svcNames := resolveServiceList()
	targets := buildTargets(svcNames, resolveDNSSuffix())
	poller := readyz.NewPoller(readyzRequestTimeout, readyzConcurrency)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		runKafkaLagLoop(ctx, kafkaClient, st, brokers, kafkaPollInterval, kafkaPollTimeout)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runReadyzLoop(ctx, poller, st, targets, readyzPollInterval)
	}()

	healthState.SetDependencyChecks(map[string]func(context.Context) error{
		"kafka": kafkaClient.Ping,
		"redis": st.Ping,
	})
	healthState.SetReady(true)
	log.Printf("ops-visibility-service готов: %d сервисов в readyz-гриде, kafka brokers=%v", len(targets), brokers)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	log.Println("остановка ops-visibility-service")
	cancel()
	wg.Wait()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = healthSrv.Shutdown(shutdownCtx)
}

// runKafkaLagLoop — опрашивает Kafka lag сразу при старте (не ждёт первый
// тик тикера — иначе /snapshot был бы пуст первые kafkaPollInterval секунд
// без причины) и затем на каждом тике interval, пока ctx не отменён.
func runKafkaLagLoop(ctx context.Context, kc *kafkalag.Client, st *store.Client, brokers []string, interval, pollTimeout time.Duration) {
	poll := func() {
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
		defer cancel()
		snap := kc.FetchAll(pollCtx, brokers)
		if snap.Error != "" {
			log.Printf("kafka lag poll failed: %s", snap.Error)
		}
		if err := st.WriteKafkaLag(ctx, snap); err != nil {
			log.Printf("write kafka lag snapshot to redis failed: %v", err)
		}
	}

	poll()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}

// runReadyzLoop — тот же принцип: первый опрос сразу, затем по тикеру.
func runReadyzLoop(ctx context.Context, poller *readyz.Poller, st *store.Client, targets []readyz.Target, interval time.Duration) {
	poll := func() {
		snap := poller.PollAll(ctx, targets)
		notReady := 0
		for _, r := range snap.Services {
			if !r.Ready {
				notReady++
			}
		}
		if notReady > 0 {
			log.Printf("readyz poll: %d/%d сервисов не ready", notReady, len(snap.Services))
		}
		if err := st.WriteReadyz(ctx, snap); err != nil {
			log.Printf("write readyz snapshot to redis failed: %v", err)
		}
	}

	poll()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			poll()
		}
	}
}
