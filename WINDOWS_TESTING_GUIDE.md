# Руководство по тестированию MPP на Windows

*Пошаговый запуск стенда и корректный нагрузочный прогон*

**Версия документа 11 сентября 2026**

Этот документ предназначен для инженера, который переносит текущий стенд MPP на другую машину с Windows, поднимает его с нуля и должен получить результат, которому можно доверять. Основной путь использует WSL 2, Ubuntu и Docker Desktop. Все команды проекта выполняются в Ubuntu, а административные команды Windows - в PowerShell.

Главный вывод: сначала нужно доказать, что перенесён правильный код, созданы все Kafka топики, применены миграции, сервисы готовы и одно тестовое сообщение проходит до терминального статуса. Только после этого допустим нагрузочный замер.

**Критическое ограничение текущего репозитория**

Исходники или образ loadgen и исходники или образ SMSC симулятора в репозитории отсутствуют. Обычный git clone не даёт законченного инструмента для воспроизведения нагрузочного теста. До получения loadgen и доступа к тестовому SMSC можно полностью подготовить стенд и выполнить одиночный smoke тест, но нельзя заявлять воспроизведение показателей 300 или 1500 TPS.

| Основа | Значение |
| --- | --- |
| Ветка | main |
| Базовый commit | 0a2f721790133caf6cd90884791f912ef348deab |
| Состояние | На момент подготовки документа рабочее дерево содержит локальные изменения |
| Цель следующего теста | Базовый замер 300 TPS на тихой машине, 3-5 прогонов |

## Как пользоваться руководством

Проходите разделы строго по порядку. После каждого контрольного пункта должно быть получено ожидаемое состояние.

| Этап | Что должно получиться | Можно идти дальше |
| --- | --- | --- |
| 1 | Код на двух машинах идентичен | Совпадают commit и git status |
| 2 | WSL 2 и Docker работают | hello world завершился успешно |
| 3 | Инфраструктура готова | PostgreSQL, Redis, Kafka и ClickHouse healthy |
| 4 | Схема готова | Все миграции применены, 31 Kafka топик проверен |
| 5 | Приложения готовы | Readiness проверки успешны |
| 6 | Smoke тест успешен | Сообщение дошло до terminal и exec ключ удалён |
| 7 | Нагрузка разрешена | Есть loadgen, SMSC доступ и тихое окно |
| 8 | Результат валиден | Нет незавершённых сообщений, OOM и необъяснённых рестартов |

### Обозначения окон

| Окно | Как открыть | Какие команды выполнять |
| --- | --- | --- |
| PowerShell администратора | Пуск, PowerShell, Запуск от имени администратора | Установка и обновление WSL |
| PowerShell обычный | Windows Terminal или PowerShell | Проверка Windows и WSL, запись сведений о машине |
| Ubuntu | Пуск, Ubuntu | Все команды git, Docker, Compose и тесты проекта |

**Важно:** не смешивайте синтаксис PowerShell и Ubuntu в одном окне. Перед каждым блоком команд в документе указано нужное окно.

## 1 Подготовка исходного кода

### 1.1 Почему обычного клонирования может быть недостаточно

На исходной машине ветка main указывает на commit 0a2f721, но рабочее дерево не чистое. Локальные изменённые и новые файлы не появятся на другой машине после git clone. Файл infra/docker/.env также игнорируется Git и содержит внешние настройки; его нужно переносить отдельно и безопасно.

### 1.2 Рекомендуемый способ через отдельную ветку

На исходной машине выполните и сохраните вывод:

```bash
git branch --show-current
git rev-parse HEAD
git status --short
```

Создайте отдельную тестовую ветку, включите только осознанные изменения, проверьте diff и отправьте ветку в удалённый репозиторий. Не используйте слепой git add для всего дерева: среди локальных файлов могут быть незавершённые или чувствительные данные.

На Windows в Ubuntu после клонирования:

```bash
git clone https://github.com/allogolden8888/thebest_mpp.git ~/work/mpp
cd ~/work/mpp
git fetch --all --prune
git checkout <TEST_BRANCH_OR_COMMIT>
git rev-parse HEAD
git status --short
```

Ожидаемый результат: commit совпадает с переданным значением, а git status пуст. Если нужны локальные изменения, их список должен совпасть на обеих машинах файл в файл.

### 1.3 Если commit создавать нельзя

- Создайте архив всей рабочей копии без тяжёлых каталогов сборки target, node_modules и временных логов.
- Передайте архив по доверенному каналу и распакуйте внутрь файловой системы Ubuntu, например ~/work/mpp.
- Отдельно сохраните вывод git rev-parse HEAD, git status --short и git diff --stat с исходной машины.
- Сверьте эти три вывода после распаковки. Такой перенос хуже отдельного commit, потому что сложнее доказать идентичность стендов.

### 1.4 Секреты

Не помещайте infra/docker/.env в commit, общий архив, мессенджер или документ. Передайте SMSC host, port, system id и password через одобренное хранилище секретов. В отчёте фиксируйте только режим подключения и имя тестового контура, но не пароль.

## 2 Подготовка Windows

### 2.1 Требования к машине

| Ресурс | Для проверки корректности | Для сравнимого теста производительности |
| --- | --- | --- |
| Windows | Windows 10 22H2 или Windows 11 | Windows 11 актуальной поддерживаемой версии |
| Процессор | 8 логических CPU | 12 и более логических CPU |
| Память | 16 GB на хосте, около 10 GB доступно WSL | 32 GB на хосте, 16 GB доступно WSL |
| Диск | Не менее 50 GB свободно | 80-100 GB свободно на SSD |
| Фоновые задачи | Допустимы для smoke теста | Закрыты IDE сборки, синхронизация, сканирование и другие нагрузки |
| Сеть | Доступ к Git и registry | Плюс VPN или маршрут до тестового SMSC |

Ориентир из предыдущего успешного стенда: Docker получал 12 CPU и 15.16 GB RAM на Ryzen 5 7600X. Слабая машина пригодна для проверки корректности, но её latency нельзя напрямую сравнивать с этим ориентиром.

### 2.2 Установка WSL 2

Окно: PowerShell администратора

```powershell
wsl --install
```

Перезагрузите Windows. Затем снова откройте PowerShell администратора:

```powershell
wsl --update
wsl --version
wsl --list --verbose
```

Ожидаемый результат: Ubuntu присутствует в списке, столбец VERSION равен 2. При первом запуске Ubuntu создайте Linux имя пользователя и пароль.

### 2.3 Установка Docker Desktop

- Скачайте Docker Desktop только с официальной страницы Docker.
- Выберите backend WSL 2 и режим Linux containers.
- Запустите Docker Desktop и дождитесь состояния Engine running.
- В Settings откройте Resources и WSL Integration, затем включите интеграцию с Ubuntu.

На корпоративной машине заранее проверьте лицензионные правила Docker Desktop и политики IT.

### 2.4 Ограничение ресурсов WSL

Для хоста с 32 GB RAM и 12 или более логическими CPU можно создать файл %UserProfile%\.wslconfig со следующим содержимым. Не задавайте 16 GB на машине, где всего 16 GB: Windows останется без памяти.

```ini
[wsl2]
memory=16GB
processors=12
swap=4GB
```

Окно: PowerShell

```powershell
wsl --shutdown
```

После этого перезапустите Docker Desktop и Ubuntu. Для другого железа выделяйте WSL не более примерно 60-70 процентов физической RAM.

## 3 Подготовка Ubuntu и проекта

### 3.1 Установка базовых инструментов

Окно: Ubuntu

```bash
sudo apt update
sudo apt install -y git curl jq dos2unix netcat-openbsd
git config --global core.autocrlf input
```

### 3.2 Где хранить проект

Храните проект в Linux файловой системе, например ~/work/mpp. Не используйте /mnt/c/Users/... для этого стенда: Docker bind mounts и большое число мелких файлов работают там заметно медленнее, а нагрузочный результат искажается.

```bash
mkdir -p ~/work
cd ~/work
# Здесь выполните clone или распакуйте переданный архив
cd ~/work/mpp
```

### 3.3 Проверка Docker из Ubuntu

```bash
docker version
docker compose version
docker run --rm hello-world
nproc
free -h
df -h ~/work
```

Если docker недоступен из Ubuntu, включите интеграцию этой WSL distribution в Docker Desktop и перезапустите Docker Desktop.

### 3.4 Фиксация паспорта стенда

```bash
mkdir -p results/preflight
git rev-parse HEAD | tee results/preflight/git_commit.txt
git status --short | tee results/preflight/git_status.txt
docker version > results/preflight/docker_version.txt
docker compose version > results/preflight/compose_version.txt
nproc > results/preflight/wsl_cpu.txt
free -h > results/preflight/wsl_memory.txt
df -h ~/work > results/preflight/wsl_disk.txt
```

## 4 Внешний доступ и настройки

### 4.1 Создание файла окружения

Окно: Ubuntu, каталог ~/work/mpp

```bash
cp infra/docker/.env.example infra/docker/.env
nano infra/docker/.env
```

Заполните четыре значения, полученные через защищённый канал:

```bash
OPERATOR_SMSC_HOST=<SMSC_HOST>
OPERATOR_SMSC_PORT=<SMSC_PORT>
SMPP_SYSTEM_ID=<SMPP_SYSTEM_ID>
SMPP_PASSWORD=<SMPP_PASSWORD>
```

Проверьте, что файл не отслеживается Git:

```bash
git status --short --ignored infra/docker/.env
```

Ожидаемый результат: строка начинается с !!, то есть файл игнорируется.

### 4.2 Проверка сети до SMSC

```bash
nc -vz <SMSC_HOST> <SMSC_PORT>
```

Успех означает только доступность TCP порта. Реальная readiness проверка operator-smpp-session-manager дополнительно подтвердит bind с логином и паролем. Если используется VPN, подключите его до запуска Docker и не меняйте сеть во время прогона.

### 4.3 Тестовые номера и разрешение на отправку

Используйте только заранее согласованные тестовые MSISDN. Одиночный smoke запрос и нагрузочный тест могут отправлять реальные SMS, если стенд подключён к настоящему SMSC. Не используйте случайные номера и не повышайте TPS лимит реального оператора без отдельного разрешения.

### 4.4 Корпоративный TLS proxy

Сначала собирайте образы без дополнительного CA. Если сборка падает с x509 certificate signed by unknown authority и машина находится в той же корпоративной сети, повторите конкретную сборку с BuildKit secret из infra/docker/unitel-root-ca.crt. Не копируйте сертификат в готовый образ вручную.

```bash
docker build --secret id=extra_ca_cert,src=infra/docker/unitel-root-ca.crt \
  -f services/<SERVICE>/Dockerfile -t mpp/<SERVICE>:test <BUILD_CONTEXT>
```

## 5 Сборка образов

### 5.1 Что будет собрано

Для горячего контура нужны 16 прикладных образов. Rust и большинство Java сервисов собираются из корня репозитория. Go сервисы и scheduler-background-lane собираются из собственного каталога. Неверный build context приводит к отсутствующим protobuf или конфигурационным файлам.

| Контекст | Сервисы |
| --- | --- |
| Корень репозитория | partner-rest-receiver, pipeline-engine, destination-resolution-service, policy-service, billing-service, routing-service, delivery-service, operator-smpp-session-manager, delivery-reconciliation-service, message-state-resolver |
| Каталог сервиса | config-event-publisher, scheduler-background-lane, dlr-manager, dlr-correlation-writer, analytics-writer, lifecycle-writer |

### 5.2 Сборка без корпоративного CA

Окно: Ubuntu, каталог ~/work/mpp

```bash
set -euo pipefail
root_context_services=(
  partner-rest-receiver pipeline-engine destination-resolution-service
  policy-service billing-service routing-service delivery-service
  operator-smpp-session-manager delivery-reconciliation-service
  message-state-resolver
)
service_context_services=(
  config-event-publisher scheduler-background-lane dlr-manager
  dlr-correlation-writer analytics-writer lifecycle-writer
)
for s in "${root_context_services[@]}"; do
  docker build -f "services/$s/Dockerfile" -t "mpp/${s}:test" .
done
for s in "${service_context_services[@]}"; do
  docker build -f "services/$s/Dockerfile" -t "mpp/${s}:test" "services/$s"
done
```

Сборка может занять значительное время и скачать несколько гигабайт зависимостей. Не запускайте её параллельно с нагрузочным тестом.

### 5.3 Проверка тегов и возраста

```bash
for s in "${root_context_services[@]}" "${service_context_services[@]}"; do
  docker image inspect "mpp/${s}:test" \
    --format '{{.RepoTags}}  {{.Id}}  {{.Created}}'
done | tee results/preflight/images.txt
```

Ожидаемый результат: 16 строк, ни одной ошибки No such image. Сохранённый image id нужен, чтобы доказать, что между прогонами образ не менялся.

## 6 Чистый запуск инфраструктуры

### 6.1 Переход в каталог Compose

```bash
cd ~/work/mpp/infra/docker
export COMPOSE_PROJECT_NAME=docker
```

Переменная имени проекта нужна потому, что create_topics.sh и verify_topics.sh ожидают имя Kafka контейнера docker-kafka-1. Не меняйте её внутри серии измерений.

### 6.2 Полный сброс тестовых данных

**Внимание:** следующая команда удаляет тестовые PostgreSQL, Kafka и Redis данные этого Compose проекта. Выполняйте её только на отдельной тестовой машине, когда результаты предыдущего стенда уже сохранены.

```bash
docker compose down -v --remove-orphans
```

Не используйте --renew-anon-volumes для произвольного списка сервисов: ранее это удаляло схему ClickHouse и делало прогон недействительным.

### 6.3 Запуск базовых хранилищ

```bash
docker compose pull postgres redis-runtime redis-configuration redis-billing kafka clickhouse
docker compose up -d postgres redis-runtime redis-configuration redis-billing kafka clickhouse
docker compose ps
```

Подождите 30-120 секунд. У шести сервисов статус должен стать healthy. Если Kafka ещё starting, повторите docker compose ps через 15 секунд.

### 6.4 Применение миграций

Команда выполняется из ~/work/mpp/infra/docker и подаёт каждый SQL файл внутрь контейнера PostgreSQL:

```bash
for f in ../../migrations/V*.sql; do
  echo "Applying $f"
  docker compose exec -T postgres \
    psql -U mpp -d mpp -v ON_ERROR_STOP=1 < "$f"
done
```

Ожидаемый результат: цикл завершился без ERROR. На чистом томе применяются все текущие миграции V001-V034.

### 6.5 Создание и проверка Kafka топиков

```bash
bash create_topics.sh
bash verify_topics.sh
```

Обязательный ожидаемый результат:

```text
OK: все 31 топиков совпадают с create_topics.sh по числу партиций
```

Если есть хотя бы одно РАСХОЖДЕНИЕ или ОТСУТСТВУЕТ, не запускайте приложения и не проводите нагрузку. Для изменения числа партиций нужен новый чистый Kafka том.

## 7 Запуск прикладных сервисов

### 7.1 Сначала операторский сервис

После свежего Redis первым запускается operator-smpp-session-manager. Он регистрирует operator_route ключи, которые нужны delivery-service. Если delivery запустить раньше, сообщения могут уйти в SUBMISSION_OUTCOME_UNKNOWN.

```bash
docker compose up -d operator-smpp-session-manager
docker compose logs --tail=100 operator-smpp-session-manager
```

Проверьте маршрут:

```bash
docker compose exec -T redis-runtime \
  redis-cli --no-auth-warning -a mpp_local_dev \
  --scan --pattern 'operator_route:*'
```

Ожидаемый результат: минимум один ключ. Если список пуст, проверьте .env, VPN, сетевой доступ и логи operator-smpp-session-manager.

### 7.2 Запуск оставшегося горячего контура

```bash
docker compose up -d \
  config-event-publisher partner-rest-receiver pipeline-engine \
  destination-resolution-service policy-service billing-service \
  routing-service delivery-service delivery-reconciliation-service \
  message-state-resolver scheduler-background-lane \
  dlr-correlation-writer dlr-manager lifecycle-writer analytics-writer
```

### 7.3 Проверка состояния контейнеров

```bash
docker compose ps
docker compose logs --since=2m | tee ../../results/preflight/startup_logs.txt
```

Плохие признаки: Exited, Restarting, unhealthy, OOMKilled, UnknownTopicOrPartition, authentication failed, broken pipe, no partition of relation.

### 7.4 Readiness из Docker сети

```bash
services=(
  config-event-publisher partner-rest-receiver pipeline-engine
  destination-resolution-service policy-service billing-service
  routing-service delivery-service operator-smpp-session-manager
  delivery-reconciliation-service message-state-resolver
  scheduler-background-lane dlr-correlation-writer dlr-manager
  lifecycle-writer analytics-writer
)
for s in "${services[@]}"; do
  printf '%-42s ' "$s"
  if docker run --rm --network docker_default curlimages/curl:8.12.1 \
      -fsS --max-time 5 "http://$s:9090/readyz" >/dev/null; then
    echo OK
  else
    echo FAIL
  fi
done
```

Не считайте контейнер готовым только по статусу Up. Если readiness возвращает FAIL, откройте логи этого сервиса до продолжения.

## 8 Одиночный smoke тест

### 8.1 Отправка одного сообщения

Используйте согласованный тестовый номер. Команда выполняется в Ubuntu из каталога infra/docker.

```bash
export TEST_MSISDN=<APPROVED_TEST_MSISDN>
export IDEMPOTENCY_KEY=windows-smoke-$(date +%s)
RESPONSE=$(curl -fsS -X POST http://localhost:8080/v1/messages \
  -H 'Content-Type: application/json' \
  -H 'X-Partner-Id: click_uz' \
  -H 'X-Application-Id: click_uz_main' \
  -H 'X-Api-Key: local-dev-test-key' \
  -H "X-Idempotency-Key: $IDEMPOTENCY_KEY" \
  --data "{\"msisdn\":\"$TEST_MSISDN\",\"sender_id\":\"Click\",\"body\":\"MPP Windows smoke test\"}")
echo "$RESPONSE" | jq .
export MESSAGE_ID=$(echo "$RESPONSE" | jq -r .message_id)
echo "MESSAGE_ID=$MESSAGE_ID"
```

Ожидаемый результат: HTTP 202, непустые message_id и trace_id. Если curl с -f завершился ошибкой, повторите без -f и сохраните код и тело ответа.

### 8.2 Проверка терминального статуса

```bash
for i in $(seq 1 60); do
  row=$(docker compose exec -T postgres psql -U mpp -d mpp -Atc \
    "SELECT current_status || '|' || terminal FROM messaging.message_read_model WHERE message_id='$MESSAGE_ID'")
  echo "$i $row"
  echo "$row" | grep -q '|t$' && break
  sleep 2
done
```

Ожидаемый результат: terminal равен t. Статус зависит от тестового SMSC и политики, но строка не должна отсутствовать бесконечно.

### 8.3 Проверка очистки состояния

```bash
docker compose exec -T redis-runtime \
  redis-cli --no-auth-warning -a mpp_local_dev \
  EXISTS "exec:$MESSAGE_ID"
```

Ожидаемый результат после терминала: 0. Значение 1 означает, что сообщение ещё не завершено; нагрузочный прогон в таком состоянии засчитывать нельзя.

### 8.4 Проверка ошибок и рестартов

```bash
docker inspect $(docker compose ps -aq) \
  --format '{{.Name}} status={{.State.Status}} OOM={{.State.OOMKilled}} restarts={{.RestartCount}}'
docker compose logs --since=10m | grep -Ei \
  'error|fatal|panic|oom|unknown.*topic|authentication failed' || true
```

Smoke checkpoint пройден только если terminal=true, exec ключ удалён, OOM=false и нет циклических рестартов.

## 9 Подготовка нагрузочного теста

### 9.1 Что обязательно получить до теста

- [ ] Исходники loadgen с Dockerfile или готовый образ с неизменяемым digest.
- [ ] Вывод loadgen help с точными именами параметров target, rate, concurrency, duration, warmup, msisdns и out.
- [ ] SHA256 бинарника либо image digest контейнера loadgen.
- [ ] Доступ к тестовому SMSC или воспроизводимый SMSC simulator с версией и инструкцией запуска.
- [ ] Согласованный список тестовых MSISDN и разрешённый TPS.
- [ ] Описание формата результата loadgen и способ сопоставить принятые message id с терминальными сообщениями.

Стоп условие: если loadgen или SMSC контур отсутствует, закончите на smoke тесте и зафиксируйте статус environment ready, load test blocked. Не заменяйте отсутствующий open loop генератор циклом curl: такой тест измеряет другой профиль нагрузки.

### 9.2 Базовый профиль следующего измерения

| Параметр | Значение | Зачем |
| --- | --- | --- |
| Rate | 300 TPS | Текущая целевая базовая точка |
| Concurrency | 100 | Профиль предыдущих замеров |
| Полная длительность | 240 секунд | Достаточно для прогрева и установившегося окна |
| Исключить в начале | 45 секунд | Холодный старт не смешивается со steady state |
| Исключить в конце | 30 секунд | Хвост разгребания не смешивается с подачей нагрузки |
| Пул MSISDN | 100000 | Малый пул активирует anti spam и меняет профиль |
| Число прогонов | 3-5 | Сравнивается медиана, а не единичный удачный результат |

Документированный исторический интерфейс генератора содержал параметры rate, concurrency, duration, warmup, msisdns и out. Перед запуском обязательно сверяйте его фактический help; не угадывайте отсутствующие флаги. Генератор должен работать в Docker сети docker_default, чтобы его часы и часы контейнеров были согласованы.

### 9.3 Перед каждым прогоном

- [ ] bash verify_topics.sh завершился строкой OK для 31 топика.
- [ ] Все readiness проверки успешны.
- [ ] docker inspect показывает OOM=false и стабильное число рестартов.
- [ ] Kafka consumer lag до прогона равен нулю или объяснён и зафиксирован.
- [ ] На Windows закрыты сборки, обновления, синхронизация облаков и другие тяжёлые задачи.
- [ ] В Task Manager записаны общий CPU, память и активность Microsoft Defender перед стартом.
- [ ] Зафиксированы время начала, commit, image ids, режим SMSC и параметр PIPELINE_COMMIT_INTERVAL_MS.

## 10 Проведение нагрузочного прогона

### 10.1 Создание каталога результата

```bash
cd ~/work/mpp/infra/docker
RUN_ID=$(date -u +%Y%m%dT%H%M%SZ)-baseline-run01
RUN_DIR=../../results/$RUN_ID
mkdir -p "$RUN_DIR"
git rev-parse HEAD > "$RUN_DIR/git_commit.txt"
git status --short > "$RUN_DIR/git_status.txt"
docker compose ps > "$RUN_DIR/compose_ps_before.txt"
docker stats --no-stream > "$RUN_DIR/docker_stats_before.txt"
bash verify_topics.sh > "$RUN_DIR/topics.txt"
```

### 10.2 Мониторинг во время прогона

```bash
(while true; do
  date -u +%FT%TZ
  docker stats --no-stream --format \
    'table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.PIDs}}'
  sleep 5
done) > "$RUN_DIR/docker_stats_5s.txt" &
export STATS_PID=$!
```

Запустите полученный loadgen в Docker сети docker_default с профилем из раздела 9. Сохраните stdout, stderr и структурированный файл результатов внутрь RUN_DIR.

После завершения генератора:

```bash
kill "$STATS_PID" 2>/dev/null || true
wait "$STATS_PID" 2>/dev/null || true
```

### 10.3 Дождаться полного схождения

```bash
while true; do
  n=$(docker compose exec -T redis-runtime \
    redis-cli --no-auth-warning -a mpp_local_dev \
    --scan --pattern 'exec:*' | wc -l)
  echo "$(date -u +%FT%TZ) exec_keys=$n" | tee -a "$RUN_DIR/drain.txt"
  [ "$n" -eq 0 ] && break
  sleep 5
done
```

Не рассчитывайте квантили до exec_keys=0. Иначе выборка содержит только быстро завершившиеся сообщения и искусственно занижает latency. Если значение не уменьшается, сохраните логи и исследуйте причину; не объявляйте прогон успешным.

### 10.4 Снять финальное состояние

```bash
docker compose ps > "$RUN_DIR/compose_ps_after.txt"
docker stats --no-stream > "$RUN_DIR/docker_stats_after.txt"
docker inspect $(docker compose ps -aq) \
  --format '{{.Name}} status={{.State.Status}} OOM={{.State.OOMKilled}} restarts={{.RestartCount}}' \
  > "$RUN_DIR/container_state.txt"
docker compose logs --since=15m > "$RUN_DIR/compose_logs.txt"
docker compose exec -T kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --all-groups --describe \
  > "$RUN_DIR/kafka_lag.txt"
```

## 11 Проверка валидности результата

### 11.1 Обязательные условия

| Проверка | Условие успеха | Если не выполнено |
| --- | --- | --- |
| Топология Kafka | verify_topics сообщает OK для 31 топика | Прогон недействителен |
| Принятые запросы | Нет неожиданных HTTP ошибок | Отделить ожидаемые policy rejects от ошибок стенда |
| Полнота | Каждый принятый message id имеет terminal=true | Прогон недействителен |
| Redis | exec ключей после схождения 0 | Прогон недействителен |
| Контейнеры | OOM=false, нет необъяснённых рестартов | Прогон недействителен |
| Kafka lag | Лаг сошёл к нулю после теста | Дождаться или расследовать |
| Хост | Нет резкой внешней нагрузки | Результат latency пометить несравнимым |
| Окно | Исключены первые 45 и последние 30 секунд | Пересчитать метрики |

### 11.2 Проверка одного или списка message id

```bash
docker compose exec -T postgres psql -U mpp -d mpp -c \
  "SELECT message_id, current_status, terminal, updated_at
     FROM messaging.message_read_model
    WHERE message_id = '<MESSAGE_ID>';"
```

Для нагрузочного теста сравнивайте множества message id из loadgen и message_read_model, а не только агрегатные счётчики.

### 11.3 Расчёт e2e latency в ClickHouse

Задайте границы измеряемого steady state окна в UTC. RUN_START должен быть на 45 секунд позже фактического старта нагрузки, RUN_END - на 30 секунд раньше конца подачи нагрузки.

```bash
export RUN_START='2026-09-10 10:00:45'
export RUN_END='2026-09-10 10:03:30'
docker compose exec -T clickhouse clickhouse-client \
  --user mpp --password mpp_local_dev --database analytics \
  --query "
WITH per_message AS (
  SELECT
    message_id,
    minIf(occurred_at, event_type = 'incoming') AS started_at,
    maxIf(occurred_at, event_type = 'lifecycle' AND lifecycle_status IN
      ('DELIVERED','UNDELIVERABLE','DELIVERY_UNRESOLVED',
       'LATE_DELIVERY_CONFIRMED','REJECTED','FAILED','SYSTEM_UNAVAILABLE')) AS terminal_at
  FROM analytics.stage_events FINAL
  WHERE occurred_at >= parseDateTime64BestEffort('$RUN_START', 3)
    AND occurred_at < addMinutes(parseDateTime64BestEffort('$RUN_END', 3), 10)
  GROUP BY message_id
  HAVING started_at >= parseDateTime64BestEffort('$RUN_START', 3)
     AND started_at < parseDateTime64BestEffort('$RUN_END', 3)
     AND terminal_at > started_at
)
SELECT
  count() AS n,
  quantileExact(0.50)(dateDiff('millisecond', started_at, terminal_at)) AS p50_ms,
  quantileExact(0.95)(dateDiff('millisecond', started_at, terminal_at)) AS p95_ms,
  quantileExact(0.99)(dateDiff('millisecond', started_at, terminal_at)) AS p99_ms,
  max(dateDiff('millisecond', started_at, terminal_at)) AS max_ms
FROM per_message" | tee "$RUN_DIR/e2e_latency.txt"
```

n должно совпадать с числом принятых сообщений в измеряемом окне. Если не совпадает, сначала найдите отсутствующие message id, затем оценивайте latency.

## 12 Сравнительный тест периодического commit

### 12.1 Что сравнивается

Базовый вариант A использует PIPELINE_COMMIT_INTERVAL_MS=0. Вариант B использует 200. Это переключатель одного и того же образа pipeline-engine; образы между вариантами не пересобираются.

```bash
# Вариант A
PIPELINE_COMMIT_INTERVAL_MS=0 docker compose up -d --force-recreate pipeline-engine

# Вариант B
PIPELINE_COMMIT_INTERVAL_MS=200 docker compose up -d --force-recreate pipeline-engine
```

После каждого recreate дождитесь readiness и повторно проверьте фактическое значение:

```bash
cid=$(docker compose ps -q pipeline-engine)
docker inspect "$cid" --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep '^PIPELINE_COMMIT_INTERVAL_MS='
```

### 12.2 Порядок прогонов

Не выполняйте сначала все A, затем все B. Загрузка Windows может направленно меняться в течение дня и создать ложную победу второго варианта. Используйте контрбалансированный порядок, например A B B A, затем B A A B. Между точками сохраняйте одинаковую паузу и не меняйте ничего кроме переключателя.

| Точка | Серия 1 | Серия 2 |
| --- | --- | --- |
| 1 | A 0 ms | B 200 ms |
| 2 | B 200 ms | A 0 ms |
| 3 | B 200 ms | A 0 ms |
| 4 | A 0 ms | B 200 ms |

Сравнивайте медианы p99 внутри каждого варианта вместе с записанной загрузкой хоста. Одна лучшая точка не является доказательством.

## 13 Протокол результата

Создайте отдельную копию этой таблицы для каждого прогона. Поля со звёздочкой обязательны.

| Поле | Значение |
| --- | --- |
| Идентификатор прогона * |  |
| Дата и время UTC * |  |
| Модель CPU и RAM хоста * |  |
| Windows build * |  |
| WSL version * |  |
| Docker Desktop и Engine * |  |
| CPU RAM swap WSL * |  |
| Git commit * |  |
| Git status * | clean или приложенный diff |
| Image ids * | файл images.txt |
| SMSC режим * | реальный тестовый или simulator и версия |
| Rate concurrency duration * |  |
| Измеряемое окно UTC * |  |
| PIPELINE COMMIT INTERVAL * | 0 или 200 |
| Принято запросов * |  |
| Terminal сообщений * |  |
| exec ключей после drain * | должно быть 0 |
| p50 p95 p99 max * |  |
| OOM и рестарты * |  |
| Kafka lag после drain * |  |
| CPU Windows и Defender |  |
| Итог * | VALID или INVALID с причиной |

### Что приложить к отчёту

- Каталог RUN_DIR целиком.
- Структурированный результат loadgen и его SHA256 или image digest.
- topics.txt, compose_ps_before.txt, compose_ps_after.txt, container_state.txt и kafka_lag.txt.
- Полные compose logs за окно теста. Не вызывайте docker compose down до сохранения логов.
- Скриншот Task Manager или другой журнал внешней нагрузки хоста.
- Короткий вывод: что проверялось, валиден ли прогон, что сравнивать с предыдущими результатами нельзя.

## 14 Типовые ошибки

| Симптом | Вероятная причина | Что сделать |
| --- | --- | --- |
| docker недоступен в Ubuntu | WSL Integration выключена | Включить Ubuntu в Docker Desktop Resources WSL Integration и перезапустить |
| No such image mpp service test | Образ не собран или неверный tag | Повторить раздел 5 и проверить 16 image ids |
| docker-kafka-1 не найден | Изменено Compose project name | Из infra/docker выполнить export COMPOSE_PROJECT_NAME=docker |
| UnknownTopicOrPartition | Сервисы стартовали до топиков | Сохранить логи, сбросить свежий стенд, create_topics и verify_topics до приложений |
| verify_topics показывает mismatch | Старый Kafka том или ручное создание | Удалить тестовые volumes и создать топики штатным скриптом |
| x509 unknown authority | TLS inspecting proxy | Настроить proxy или повторить сборку с опциональным BuildKit secret |
| operator readiness FAIL | Нет маршрута VPN, неверные SMSC креды или bind отказан | nc до host port, проверить .env и логи |
| Все delivery unknown | operator route не засеян | Перезапустить стенд и первым поднять operator service |
| broken pipe после Redis restart | Сервисы не восстанавливают cached connection | Пересоздать зависимые сервисы, не рестартовать Redis во время серии |
| No partition of relation | Не применены миграции или проблема lifecycle partition | Проверить цикл миграций и lifecycle-writer logs |
| Kafka или Java OOM | Недостаточно RAM или внешняя нагрузка | Сохранить inspect, увеличить ресурсы или снизить scope; прогон invalid |
| Latency скачет между одинаковыми прогонами | Хост занят Defender или другой работой | Не делать вывод по одной точке; 3-5 прогонов и запись host load |
| После down логи пусты | Контейнеры уже уничтожены | Всегда сохранять docker compose logs до down |

## 15 Остановка и очистка

### 15.1 Остановка с сохранением данных

```bash
docker compose stop
```

Используйте этот вариант, если нужно сохранить PostgreSQL, Kafka и ClickHouse для расследования.

### 15.2 Удаление контейнеров с сохранением volumes

```bash
docker compose down --remove-orphans
```

Volumes остаются. Следующий запуск не является полностью чистым.

### 15.3 Полная очистка тестового стенда

**Внимание:** удаляет данные стенда без возможности восстановления из Docker volumes.

```bash
docker compose down -v --remove-orphans
```

Перед полной очисткой скопируйте RUN_DIR и логи в место хранения результатов.

## 16 Финальный чек лист

### До поездки или передачи машины

- [ ] Передан точный commit или архив текущего рабочего дерева.
- [ ] Локальные изменения перечислены и сверены.
- [ ] SMSC настройки переданы отдельно через защищённый канал.
- [ ] Получены loadgen и SMSC simulator либо подтверждён доступ к тестовому SMSC.
- [ ] Известны разрешённые номера, TPS и тестовое окно.

### На Windows до запуска

- [ ] WSL version 2, Docker Desktop работает в Linux containers режиме.
- [ ] Код хранится в ~/work/mpp, не в /mnt/c.
- [ ] Docker видит нужные CPU, RAM и свободный диск.
- [ ] git commit и git status записаны.
- [ ] TCP доступ до SMSC подтверждён.

### Перед нагрузкой

- [ ] 16 образов собраны и их ids сохранены.
- [ ] Инфраструктура healthy.
- [ ] Миграции применены без ошибок.
- [ ] verify_topics сообщает OK для 31 топика.
- [ ] operator service запущен первым и operator_route существует.
- [ ] Все readiness проверки успешны.
- [ ] Одиночный smoke тест дошёл до terminal и exec ключ удалён.
- [ ] На хосте тихое окно, внешняя нагрузка записана.

### После нагрузки

- [ ] exec ключей 0.
- [ ] Множество принятых message id совпадает с терминальными.
- [ ] Kafka lag сошёл.
- [ ] OOM=false и нет необъяснённых рестартов.
- [ ] Метрики посчитаны только по steady state окну.
- [ ] RUN_DIR, логи, image ids и сведения о хосте сохранены.
- [ ] Прогон помечен VALID или INVALID с конкретной причиной.

## Источники

- [Microsoft Learn — установка WSL](https://learn.microsoft.com/en-us/windows/wsl/install)

- [Docker Docs — установка Docker Desktop на Windows](https://docs.docker.com/desktop/setup/install/windows-install/)

- [Docker Docs — лучшие практики WSL 2](https://docs.docker.com/desktop/features/wsl/best-practices/)

- [Docker Docs — работа с Docker через WSL 2](https://docs.docker.com/desktop/features/wsl/use-wsl/)

### Файлы репозитория использованные для инструкции

- Docker стенд: infra/docker/docker-compose.yml и infra/docker/.env.example
- Kafka топики: infra/docker/create_topics.sh и infra/docker/verify_topics.sh
- migrations/V001-V034
- PLATFORM_STATE_FOR_REVIEW.md, LATENCY_INVESTIGATION.md и LATENCY_INVESTIGATION_1500TPS.md
- Dockerfile и README сервисов горячего контура
