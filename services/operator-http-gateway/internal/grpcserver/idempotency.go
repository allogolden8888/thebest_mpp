package grpcserver

import (
	"math/rand/v2"
	"sync"
	"time"
)

// segmentIdempotency — CODE_REVIEW.md HIGH finding #4: раньше, если
// сегмент 1 многосегментного SMS был реально принят оператором
// (необратимо — уже ушёл на handset), а сегмент 2 отклонён, Submit
// возвращал ошибку на весь вызов; вызывающий (Delivery Service) не мог
// отличить "ничего не отправлено" от "часть отправлена" и ретраил ВЕСЬ
// SubmitRequest — сегмент 1 уходил абоненту повторно.
//
// Инстанс-адресован (тот же принцип, что и весь этот gRPC-сервер —
// см. package doc в server.go): ретрай того же message_id гарантированно
// приходит на тот же под, поэтому in-process карта — не распределённый
// кэш — корректно решает проблему без внешней инфраструктуры (Redis).
// segment_id -> smsc_message_id уже принятого сегмента; при повторном
// Submit с тем же message_id уже принятые сегменты не отправляются
// оператору повторно, только оставшиеся.
type segmentIdempotency struct {
	mu   sync.Mutex
	msgs map[string]map[int32]string // message_id -> segment_id -> smsc_message_id
	seen map[string]time.Time        // message_id -> последний доступ, для TTL-вытеснения
	ttl  time.Duration
}

func newSegmentIdempotency(ttl time.Duration) *segmentIdempotency {
	return &segmentIdempotency{
		msgs: make(map[string]map[int32]string),
		seen: make(map[string]time.Time),
		ttl:  ttl,
	}
}

func (s *segmentIdempotency) get(messageID string, segmentID int32) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	segs, ok := s.msgs[messageID]
	if !ok {
		return "", false
	}
	s.seen[messageID] = time.Now()
	smscID, ok := segs[segmentID]
	return smscID, ok
}

func (s *segmentIdempotency) record(messageID string, segmentID int32, smscMessageID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	segs, ok := s.msgs[messageID]
	if !ok {
		segs = make(map[int32]string)
		s.msgs[messageID] = segs
	}
	segs[segmentID] = smscMessageID
	s.seen[messageID] = time.Now()

	// Вероятностный sweep вместо фонового тикера — не нужен отдельный
	// goroutine/lifecycle (Close на shutdown, утечка в тестах, создающих
	// много *Server через New()); при разумном TPS достаточно, чтобы карта
	// не росла неограниченно за счёт ретраев, которые так и не пришли.
	if rand.IntN(2000) == 0 {
		s.sweepLocked(time.Now())
	}
}

// forget — вызывается, когда все сегменты сообщения успешно приняты и
// опубликованы: держать запись дальше бессмысленно, тот же message_id
// повторно не придёт в норме (Delivery Service не ретраит успешные
// Submit).
func (s *segmentIdempotency) forget(messageID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.msgs, messageID)
	delete(s.seen, messageID)
}

// sweepLocked — вытесняет записи о частично отправленных сообщениях
// старше ttl (ретраи, которые так и не пришли — не даём карте расти
// неограниченно). Вызывающий должен держать s.mu.
func (s *segmentIdempotency) sweepLocked(now time.Time) {
	for id, last := range s.seen {
		if now.Sub(last) > s.ttl {
			delete(s.msgs, id)
			delete(s.seen, id)
		}
	}
}
