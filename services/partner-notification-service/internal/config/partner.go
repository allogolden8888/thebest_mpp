// Package config — resolve_delivery_channel (service_internal_methods.md
// §7.1): партнёрский снапшот, `config_schemas/partner.schema.json`. В
// проде — из config.changes (entity_type=PARTNER); здесь, как и у
// partner-rest-receiver, снапшот грузится один раз из статического файла.
package config

import "fmt"

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
