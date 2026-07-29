package httpio

import (
	"fmt"
	"net"
	"net/url"
	"syscall"
	"time"
)

// CODE_REVIEW.md MEDIUM finding: endpointURL (сейчас — статический env var
// per-instance, в будущем — per-operator значение из config.changes,
// см. README) отправлялся в POST без единой проверки scheme/хоста. Не
// эксплуатируемо сегодня (endpoint_url задаётся деплоем, не приходит из
// недоверенного запроса), но должно быть закрыто ДО того, как
// config.changes-driven обновление endpoint'а появится — иначе
// компрометация config-пайплайна становится SSRF в произвольный
// internal-сервис кластера. Тот же приём, что
// services/partner-notification-service/internal/notify/ssrf_guard.go в
// main-ветке для notification_callback_url — тот же класс риска
// (endpoint приходит из конфигурации, не напрямую из недоверенного
// запроса, но компрометация конфиг-пайплайна — реальный вектор).
//
// checkScheme — дешёвая pre-check без сетевого запроса, отвергает
// file://gopher:// и т.п. ssrfSafeDialer.Control — вызывается для КАЖДОГО
// фактического TCP dial с уже резолвленным IP, поэтому не обходится DNS
// rebinding (в отличие от проверки только по hostname до резолва).
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
	if u.Scheme != "http" && u.Scheme != "https" {
		return &SSRFRejectedError{Address: rawURL, Reason: fmt.Sprintf("scheme %q запрещён, разрешены только http/https", u.Scheme)}
	}
	return nil
}

func ssrfSafeDialer(timeout time.Duration) *net.Dialer {
	return &net.Dialer{
		Timeout: timeout,
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
				return &SSRFRejectedError{Address: address, Reason: "приватный/loopback/link-local/metadata-класс адрес запрещён для operator submit endpoint"}
			}
			return nil
		},
	}
}

// isPubliclyRoutable — включает cloud metadata endpoint (169.254.169.254 и
// аналоги — все в 169.254.0.0/16, покрыто IsLinkLocalUnicast), RFC1918
// (IsPrivate), loopback, IPv6 unique-local (тоже IsPrivate в stdlib),
// 0.0.0.0/::.
func isPubliclyRoutable(ip net.IP) bool {
	return !(ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast())
}
