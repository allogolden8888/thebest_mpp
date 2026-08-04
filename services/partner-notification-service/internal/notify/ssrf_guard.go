package notify

import (
	"fmt"
	"net"
	"net/url"
	"syscall"
	"time"
)

// Закрывает HIGH находку кодревью (PART 2, partner-notification-service #1):
// `notification_callback_url` (config_schemas/partner.schema.json) приходил
// в `RestClient.SendCallback` без единой проверки — ни scheme, ни
// DNS/IP-класса, ни metadata-эндпоинта (`format: uri` в schema разрешает
// буквально что угодно, включая `http://169.254.169.254/...` или
// `https://internal-service.mpp.svc:8080/admin`). Значение приходит из
// конфигурации партнёра (в проде — `config.changes`, здесь — статический
// файл), не напрямую из недоверенного partner-facing запроса, но
// компрометация онбординг-процесса/конфига — реальный, не гипотетический
// вектор, и требовать защиту только от "недоверенного HTTP-запроса" здесь
// было бы слишком узко: тот же класс риска (SSRF из кластера наружу
// невозможен, а вот ИЗНУТРИ кластера — в другой internal-сервис — вполне).
//
// checkScheme — дешёвая синхронная проверка ДО попытки соединения (отвергает
// весь неверный scheme класс, `file://`/`gopher://`/plain `http://`, без
// сетевого запроса вообще).
//
// ssrfSafeDialer — дорогая, но структурно надёжная часть: Control-хук
// `net.Dialer` вызывается Go runtime для КАЖДОГО фактического TCP dial'а
// (включая редиректы на новый хост и повторные DNS-резолвы), с уже
// резолвленным IP — проверка на этом уровне не обходится DNS rebinding
// (смена A-записи между разовой pre-check валидацией URL и реальным
// подключением), в отличие от проверки только на уровне hostname/URL.
// SSRFRejectedError — отдельный тип, не просто fmt.Errorf: RestClient.SendCallback
// должен отличить "guard отверг адрес" (OutcomePermanentFailure — повтор
// того же forbidden URL никогда не поможет) от обычной сетевой ошибки
// (OutcomeRetryable — таймаут/connection refused у настоящего партнёрского
// endpoint'а). net/http оборачивает ошибку из Control-хука через
// *net.OpError -> *url.Error, оба реализуют Unwrap(), так что
// errors.As на вызывающей стороне находит этот тип сквозь обёртки.
type SSRFRejectedError struct {
	Address string
	Reason  string
}

func (e *SSRFRejectedError) Error() string {
	return fmt.Sprintf("ssrf guard: %s: %s", e.Address, e.Reason)
}

func checkScheme(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("ssrf guard: невалидный URL: %w", err)
	}
	if u.Scheme != "https" {
		return &SSRFRejectedError{Address: rawURL, Reason: fmt.Sprintf("scheme %q запрещён, разрешён только https", u.Scheme)}
	}
	return nil
}

func ssrfSafeDialer() *net.Dialer {
	return &net.Dialer{
		Timeout: 10 * time.Second,
		Control: func(network, address string, c syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return fmt.Errorf("ssrf guard: не удалось разобрать address %q: %w", address, err)
			}
			ip := net.ParseIP(host)
			if ip == nil {
				return &SSRFRejectedError{Address: address, Reason: "не резолвится в IP на момент dial"}
			}
			if !isPubliclyRoutable(ip) {
				return &SSRFRejectedError{Address: address, Reason: "приватный/loopback/link-local/metadata-класс адрес запрещён для partner callback"}
			}
			return nil
		},
	}
}

// isPubliclyRoutable — включает cloud metadata endpoint (169.254.169.254 и
// аналоги у GCP/Azure — все в 169.254.0.0/16, покрыто IsLinkLocalUnicast),
// RFC1918 (IsPrivate), loopback (127.0.0.0/8, ::1), IPv6 unique-local
// (fc00::/7, тоже IsPrivate в стандартной библиотеке), 0.0.0.0/::.
func isPubliclyRoutable(ip net.IP) bool {
	return !(ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast())
}
