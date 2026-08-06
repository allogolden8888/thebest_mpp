-- V023__number_range_operator_id_uz_suffix.sql
-- Найдено при работе над development_plan.md 5.5 (реальные connection-профили
-- операторов): routing.number_range (V011) сеялась с operator_id БЕЗ суффикса
-- ('beeline'/'ucell'/'uzmobile'), тогда как config_schemas/examples/{operator,
-- routing_table,number_range}.valid.json, а также дефолтный OPERATOR_ID обоих
-- connector-сервисов (operator-smpp-session-manager, operator-http-gateway)
-- используют суффикс '_uz' ('beeline_uz' и т.д.) — 5 мест против 2.
--
-- routing-service резолвит route по ТОЧНОМУ совпадению operator_id
-- (RouteTableSnapshot::for_operator, exact HashMap key) — реальное сообщение,
-- destination-resolution-service которого резолвит в 'beeline' (V011/снапшот),
-- не нашло бы маршрут в routing_table.valid.json, где ключ 'beeline_uz'.
-- RoutingError::UnknownOperator на каждом сообщении, для всех трёх операторов.
--
-- Нормализуем на '_uz' (большинство соглашение) вместо переписывания 5 мест
-- под 2. destination-resolution-service/data/number_range_snapshot.json
-- (production-эквивалентный снапшот той же V011-данных) обновлён тем же
-- коммитом отдельным diff'ом — исправление здесь, не только в снапшоте,
-- потому что migrations/ — источник истины для этой таблицы
-- (data_infrastructure_spec.md §1).

UPDATE routing.number_range
    SET operator_id = operator_id || '_uz'
    WHERE operator_id IN ('beeline', 'ucell', 'uzmobile');
