#!/bin/bash
# Проверяет, что ФАКТИЧЕСКИЕ partition counts в брокере совпадают с тем, что
# объявлено в create_topics.sh. Расхождение — ошибка, а не предупреждение.
#
# ПОЧЕМУ этот скрипт существует. Нагрузочный прогон был признан
# недействительным после того, как выяснилось: stage.completed работал на
# ОДНОЙ партиции вместо 26. Причин было две, и обе тихие — включённый
# auto-create создавал топик дефолтным при первом обращении, а ручное
# предсоздание делалось не этим скриптом, а самодельной командой с
# захардкоженным --partitions 1. Ни то, ни другое ничего не сообщало: в
# логах стенда всё выглядело нормально, и расхождение с документацией
# прожило до внешнего ревью.
#
# Запуск: bash infra/docker/verify_topics.sh   (перед каждым замером)
set -uo pipefail

KAFKA_CONTAINER="docker-kafka-1"
BOOTSTRAP="localhost:9092"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ожидания берём из единственного источника правды — самого create_topics.sh,
# чтобы список не разъехался с ним при следующей правке.
expected=$(grep -E '^create_topic ' "$SCRIPT_DIR/create_topics.sh" | awk '{print $2, $3}')

actual=$(docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "$BOOTSTRAP" --describe 2>/dev/null \
  | awk '/^Topic: /{for(i=1;i<=NF;i++){if($i=="Topic:")t=$(i+1); if($i=="PartitionCount:")p=$(i+1)} print t, p}' \
  | sort -u)

fail=0
missing=0
while read -r name want; do
  [ -z "$name" ] && continue
  got=$(echo "$actual" | awk -v n="$name" '$1==n{print $2}')
  if [ -z "$got" ]; then
    echo "ОТСУТСТВУЕТ  $name (ожидалось partitions=$want)"
    missing=$((missing+1)); fail=1
  elif [ "$got" != "$want" ]; then
    echo "РАСХОЖДЕНИЕ  $name: partitions=$got, ожидалось $want"
    fail=1
  fi
done <<< "$expected"

total=$(echo "$expected" | grep -c .)
if [ "$fail" -eq 0 ]; then
  echo "OK: все $total топиков совпадают с create_topics.sh по числу партиций"
else
  echo "ПРОВАЛ: топология Kafka не соответствует create_topics.sh (отсутствует: $missing)."
  echo "Замер на такой конфигурации недействителен — исправьте до прогона."
fi
exit $fail
