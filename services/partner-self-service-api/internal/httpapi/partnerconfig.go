// Package httpapi — общий read-modify-write помощник поверх PARTNER
// config-документа (config_schemas/partner.schema.json), используется
// applications.go/senders.go (Applications+Senders) и webhook.go
// (notification_callback_url живёт ВНУТРИ каждого application — то же
// поддерево документа). Вынесено в отдельный файл, чтобы оба потребителя
// не заводили собственные копии структур и не расходились.
//
// Найдено при реализации (Фаза 3 плана): ни одна RPC ConfigService не
// возвращала payload_json до коммита "platform-contracts:
// ConfigVersionResponse.payload_json" в этой же сессии — без него
// read-modify-write был структурно невозможен ни для одного клиента.
//
// entity_id для CONFIG_ENTITY_TYPE_PARTNER — сам partner_id (конвенция,
// не проверяется платформой ни для одного entity_type, см.
// config-cache-projector/internal/projector/projector.go за тем же
// наблюдением) — здесь строго соблюдается: entity_id и payload.PartnerID
// всегда равны claims.PartnerID вызывающего, партнёр не может прочитать
// или изменить чужой конфиг подменой ID в теле запроса.
package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"
)

const configRPCTimeout = 5 * time.Second

type applicationAuth struct {
	Type          string `json:"type"`
	CredentialRef string `json:"credential_ref"`
}

type application struct {
	ApplicationID           string          `json:"application_id"`
	DisplayName             string          `json:"display_name"`
	Auth                    applicationAuth `json:"auth"`
	IPAllowlist             []string        `json:"ip_allowlist"`
	RateLimitTPS            int             `json:"rate_limit_tps"`
	AllowedChannels         []string        `json:"allowed_channels"`
	NotificationCallbackURL string          `json:"notification_callback_url,omitempty"`
}

type sender struct {
	SenderID string `json:"sender_id"`
	Type     string `json:"type"`
	Status   string `json:"status"`
}

type partnerConfig struct {
	PartnerID    string        `json:"partner_id"`
	Version      int           `json:"version"`
	Status       string        `json:"status"`
	Applications []application `json:"applications"`
	Senders      []sender      `json:"senders,omitempty"`
}

// getPartnerConfig — GetActiveVersion(PARTNER, partnerID) + unmarshal.
// partnerID здесь — ВСЕГДА claims.PartnerID вызывающего, не значение из
// URL/тела запроса. Второе возвращаемое значение — resp.GetVersion(), версия,
// которую назначил ConfigService этой ревизии (НЕ путать с cfg.Version —
// см. doc-комментарий putPartnerConfig ниже); вызывающий обязан прокинуть
// её обратно в putPartnerConfig как expectedVersion (BACKOFFICE_ROADMAP.md,
// Production Readiness Review, P1 "Конкурентные изменения") — иначе
// read-modify-write здесь структурно уязвим к потере конкурентного
// изменения (два запроса читают одну и ту же версию, оба пишут поверх неё,
// один результат молча исчезает).
func getPartnerConfig(ctx context.Context, client grpcv1.ConfigServiceClient, partnerID string) (partnerConfig, int64, error) {
	ctx, cancel := context.WithTimeout(ctx, configRPCTimeout)
	defer cancel()

	resp, err := client.GetActiveVersion(ctx, &grpcv1.GetActiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   partnerID,
	})
	if err != nil {
		return partnerConfig{}, 0, fmt.Errorf("не удалось прочитать текущий конфиг партнёра: %w", err)
	}

	var cfg partnerConfig
	if err := json.Unmarshal(resp.GetPayloadJson(), &cfg); err != nil {
		return partnerConfig{}, 0, fmt.Errorf("не удалось разобрать текущий конфиг партнёра: %w", err)
	}
	return cfg, resp.GetVersion(), nil
}

// putPartnerConfig — CreateVersion(PARTNER, cfg.PartnerID, ...). version
// внутри payload — самостоятельный документ-ревизионный счётчик
// (config_schemas/partner.schema.json требует его, но семантически НЕ
// связан с версией, которую отдельно назначает сам ConfigService при
// записи — ничем в этом контракте не проверяется, см.
// configuration-service/internal/validate/semantic.go, там нет проверки
// этого поля); инкрементируется здесь просто для читаемости истории самими
// партнёрами, не как источник истины.
//
// expectedVersion — версия, которую вызывающий хендлер прочитал через
// getPartnerConfig НЕПОСРЕДСТВЕННО перед тем, как изменить cfg (0, если
// вызывающий её не отслеживает — тогда CreateVersion работает как раньше,
// без CAS-проверки). ConfigService атомарно сверяет её с реальной текущей
// версией и возвращает codes.Aborted при расхождении (кто-то другой успел
// записать между чтением и этим вызовом) — писатель, добавивший это поле
// последним, тогда не рискует молча затереть изменение первого писателя.
// Ошибка в этом случае возвращается КАК ЕСТЬ (не оборачивается текстом) —
// вызывающий хендлер обязан пропустить её через writePutConfigError, чтобы
// status.Code(err) всё ещё видел codes.Aborted через errors.As (fmt.Errorf
// с %w это не ломает, но лишний текст в сообщении не помогает клиенту
// понять, что нужно перечитать и повторить).
func putPartnerConfig(ctx context.Context, client grpcv1.ConfigServiceClient, cfg partnerConfig, requestedBy string, expectedVersion int64) error {
	cfg.Version++

	payload, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("не удалось сериализовать новый конфиг партнёра: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, configRPCTimeout)
	defer cancel()

	_, err = client.CreateVersion(ctx, &grpcv1.CreateVersionRequest{
		EntityType:      commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:        cfg.PartnerID,
		PayloadJson:     payload,
		RequestedBy:     requestedBy,
		ExpectedVersion: expectedVersion,
	})
	if err != nil {
		return fmt.Errorf("не удалось сохранить новый конфиг партнёра: %w", err)
	}
	return nil
}

// writePutConfigError — маппинг ошибки putPartnerConfig в HTTP-ответ,
// общий для applications.go/senders-хендлеров в том же файле и webhook.go
// (все три — read-modify-write поверх одного и того же PARTNER-документа
// через getPartnerConfig/putPartnerConfig выше). codes.Aborted — единственный
// отличимый случай CAS-конфликта (configuration-service/internal/grpcserver/
// server.go storeErrToStatus, store.ErrVersionConflict): expected_version,
// прочитанный этим запросом, разошёлся с реальной текущей версией к моменту
// записи, потому что другой запрос успел записать в промежутке. Это не
// сбой нижестоящего сервиса (остаётся 502 Bad Gateway, тот же смысл, что
// writeGRPCError в credentials.go) — вызывающему достаточно перечитать
// конфиг (GET) и повторить попытку, поэтому 409 Conflict, а не 502: клиент
// сам может это исправить простым повтором, в отличие от "сбой
// ConfigService" по любой другой причине.
func writePutConfigError(w http.ResponseWriter, err error) {
	if status.Code(err) == codes.Aborted {
		http.Error(w, "конфликт версий: конфиг партнёра был изменён другим запросом с момента последнего чтения — перечитайте конфиг (GET) и повторите изменение", http.StatusConflict)
		return
	}
	http.Error(w, fmt.Sprintf("не удалось сохранить конфиг партнёра: %v", err), http.StatusBadGateway)
}
