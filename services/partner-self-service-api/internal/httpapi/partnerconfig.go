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
	"time"

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
// URL/тела запроса.
func getPartnerConfig(ctx context.Context, client grpcv1.ConfigServiceClient, partnerID string) (partnerConfig, error) {
	ctx, cancel := context.WithTimeout(ctx, configRPCTimeout)
	defer cancel()

	resp, err := client.GetActiveVersion(ctx, &grpcv1.GetActiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   partnerID,
	})
	if err != nil {
		return partnerConfig{}, fmt.Errorf("не удалось прочитать текущий конфиг партнёра: %w", err)
	}

	var cfg partnerConfig
	if err := json.Unmarshal(resp.GetPayloadJson(), &cfg); err != nil {
		return partnerConfig{}, fmt.Errorf("не удалось разобрать текущий конфиг партнёра: %w", err)
	}
	return cfg, nil
}

// putPartnerConfig — CreateVersion(PARTNER, cfg.PartnerID, ...). version
// внутри payload — самостоятельный документ-ревизионный счётчик
// (config_schemas/partner.schema.json требует его, но семантически НЕ
// связан с версией, которую отдельно назначает сам ConfigService при
// записи — ничем в этом контракте не проверяется, см.
// configuration-service/internal/validate/semantic.go, там нет проверки
// этого поля); инкрементируется здесь просто для читаемости истории самими
// партнёрами, не как источник истины.
func putPartnerConfig(ctx context.Context, client grpcv1.ConfigServiceClient, cfg partnerConfig, requestedBy string) error {
	cfg.Version++

	payload, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("не удалось сериализовать новый конфиг партнёра: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, configRPCTimeout)
	defer cancel()

	_, err = client.CreateVersion(ctx, &grpcv1.CreateVersionRequest{
		EntityType:  commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:    cfg.PartnerID,
		PayloadJson: payload,
		RequestedBy: requestedBy,
	})
	if err != nil {
		return fmt.Errorf("не удалось сохранить новый конфиг партнёра: %w", err)
	}
	return nil
}
