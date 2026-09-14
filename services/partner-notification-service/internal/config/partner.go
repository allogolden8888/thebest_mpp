// Package config — resolve_delivery_channel (service_internal_methods.md
// §7.1): партнёрский снапшот, `config_schemas/partner.schema.json`.
//
// BACKOFFICE_ROADMAP.md "Production Readiness Review" P0 #4: раньше
// снапшот грузился РОВНО ОДИН РАЗ при старте из статического файла
// (PARTNER_CONFIG_PATH) и никогда не обновлялся — партнёр, созданный/
// изменённый через partner-self-service-api после старта процесса, был
// структурно невидим этому сервису до рестарта. Теперь (см.
// redis_source.go/Store) снапшот либо грузится один раз из статического
// файла (PARTNER_CONFIG_PATH, явно заданный — local dev без Configuration
// Redis), либо строится bootstrap-чтением из Configuration Redis + живьём
// обновляется через config.changes-консьюмер
// (internal/kafkaio/configchanges.go) — тот же
// config:current:partner:{id}/config:version:partner:{id}:{version},
// который уже пишет config-cache-projector, реально работающий в проде.
package config

import (
	"fmt"
)

type AuthConfig struct {
	Type          string `json:"type"`
	CredentialRef string `json:"credential_ref"`
}

type Application struct {
	ApplicationID           string     `json:"application_id"`
	DisplayName             string     `json:"display_name"`
	Auth                    AuthConfig `json:"auth"`
	IPAllowlist             []string   `json:"ip_allowlist"`
	RateLimitTPS            int        `json:"rate_limit_tps"`
	AllowedChannels         []string   `json:"allowed_channels"`
	NotificationCallbackURL string     `json:"notification_callback_url"`
}

type Partner struct {
	PartnerID    string        `json:"partner_id"`
	Version      int           `json:"version"`
	Status       string        `json:"status"`
	Applications []Application `json:"applications"`
}

func (p Partner) IsActive() bool {
	return p.Status == "active"
}

// IsArchived — партнёр, снятый с обслуживания целиком (не "suspended", тот
// временный/обратимый — partner_id остаётся в живом снапшоте, чтобы запросы
// от него получали осмысленный отказ, а не "неизвестный партнёр"). Только
// archived означает "удалить из живого состояния", см.
// internal/kafkaio/configchanges.go::HandleConfigChangeRecord.
func (p Partner) IsArchived() bool {
	return p.Status == "archived"
}

// Snapshot — по всем партнёрам, keyed по partner_id (в этом срезе строится
// из одного файла с одним партнёром, тот же паттерн упрощения, что
// partner-rest-receiver).
type Snapshot struct {
	partners map[string]Partner
}

func NewSnapshot(partners []Partner) Snapshot {
	m := make(map[string]Partner, len(partners))
	for _, p := range partners {
		m[p.PartnerID] = p
	}
	return Snapshot{partners: m}
}

// Len — number of partners currently in the snapshot (bootstrap logging).
func (s Snapshot) Len() int {
	return len(s.partners)
}

func (s Snapshot) Application(partnerID, applicationID string) (Partner, Application, bool) {
	partner, ok := s.partners[partnerID]
	if !ok {
		return Partner{}, Application{}, false
	}
	for _, app := range partner.Applications {
		if app.ApplicationID == applicationID {
			return partner, app, true
		}
	}
	return Partner{}, Application{}, false
}

// FirstApplication — реальная находка, не идеальное решение: `msgctx:{message_id}`
// (data_infrastructure_spec.md §284) несёт `partner_id`, но НЕ `application_id`
// — на момент, когда message.lifecycle долетает сюда, мы не знаем, каким
// application_id сообщение было отправлено. resolve_delivery_channel
// поэтому в этом срезе работает на уровне партнёра, беря первое
// сконфигурированное приложение — корректно для тестового партнёра
// (одинаковый auth.type/канал у всех его application), но не
// генерализовано для партнёра с разнородными приложениями. Правильный
// фикс — добавить application_id в msgctx (Pipeline Engine, уже
// закоммичен) либо в MessageLifecycleEvent (Message State Resolver, уже
// закоммичен) — не сделано здесь, см. README.
func (s Snapshot) FirstApplication(partnerID string) (Partner, Application, bool) {
	partner, ok := s.partners[partnerID]
	if !ok || len(partner.Applications) == 0 {
		return Partner{}, Application{}, false
	}
	return partner, partner.Applications[0], true
}

// withPartner/withoutPartner — copy-on-write, backing Store's atomic swap
// (store.go): Snapshot is treated as immutable once published, so every
// live update builds a new map instead of mutating the one readers may
// currently be iterating/looking up concurrently.

func (s Snapshot) withPartner(p Partner) Snapshot {
	m := make(map[string]Partner, len(s.partners)+1)
	for k, v := range s.partners {
		m[k] = v
	}
	m[p.PartnerID] = p
	return Snapshot{partners: m}
}

func (s Snapshot) withoutPartner(partnerID string) Snapshot {
	if _, ok := s.partners[partnerID]; !ok {
		return s
	}
	m := make(map[string]Partner, len(s.partners))
	for k, v := range s.partners {
		if k != partnerID {
			m[k] = v
		}
	}
	return Snapshot{partners: m}
}

type Channel int

const (
	ChannelUnspecified Channel = iota
	ChannelSMPP
	ChannelREST
)

// ResolveDeliveryChannel — найдено при реализации: partner.schema.json
// раньше не имело поля, куда слать REST-уведомление о статусе (только
// как партнёр аутентифицируется на приёме, не куда пушить статус
// обратно) — добавлено поле `notification_callback_url` (см.
// config_schemas/partner.schema.json, коммит перед этим сервисом).
// auth.type=SMPP_BIND -> уведомление идёт через deliver_sm на тот же
// бинд (system_id = application_id, см. registry.go), иначе нужен явный
// callback URL.
func ResolveDeliveryChannel(app Application) (Channel, error) {
	if app.Auth.Type == "SMPP_BIND" {
		return ChannelSMPP, nil
	}
	if app.NotificationCallbackURL != "" {
		return ChannelREST, nil
	}
	return ChannelUnspecified, fmt.Errorf("application %s: auth.type=%s без notification_callback_url — некуда доставить уведомление", app.ApplicationID, app.Auth.Type)
}
