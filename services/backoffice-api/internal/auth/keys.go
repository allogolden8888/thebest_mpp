// Package auth — keys.go: JWKS/kid key rotation (BACKOFFICE_ROADMAP.md P0
// "секреты" — "статический public key нужно заменить на JWKS/kid rotation").
// До этого файла backoffice-api несло РОВНО один RSA keypair, зашитый в
// JWT_PUBLIC_KEY_PEM/JWT_PRIVATE_KEY_PEM — замена ключа требовала мгновенной
// инвалидации всех выпущенных токенов (env var меняется -> старый public key
// исчезает -> ВСЕ токены, подписанные старым private key, включая ещё живые
// в пределах 8h TTL, отваливаются одновременно, без окна перекрытия).
//
// KeyID — детерминированный отпечаток публичного ключа (SHA-256 от PKIX/SPKI
// DER, первые 16 base64url-символов), а не отдельная env var/секрет на
// каждый ключ. Оператору не нужно САМОМУ придумывать/синхронизировать `kid`
// между issuer и validator — kid одного и того же ключа всегда один и тот же
// на обеих сторонах автоматически, потому что вычисляется из самого
// ключевого материала. Та же идея, что HTTP ETag/git blob hash — identity
// через содержимое, не через отдельно поддерживаемый реестр имён.
package auth

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"fmt"
	"math/big"
)

// KeyID — см. package doc. Паникует только если pub нельзя маршалить как
// PKIX (rsa.PublicKey всегда можно — MarshalPKIXPublicKey не может упасть на
// валидном *rsa.PublicKey), поэтому ошибка здесь была бы признаком
// повреждённого ключа, а не штатным случаем, который стоит прокидывать
// наверх через error во всех вызывающих.
func KeyID(pub *rsa.PublicKey) string {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		panic(fmt.Sprintf("auth: MarshalPKIXPublicKey не удался для валидного *rsa.PublicKey: %v", err))
	}
	sum := sha256.Sum256(der)
	return base64.RawURLEncoding.EncodeToString(sum[:])[:16]
}

// JWK — одна запись JWKS-ответа (RFC 7517), только поля, которые нужны для
// RSA sig-ключа — не полный JWK-словарь (нет ни EC/oct полей, ни x5c/x5t).
type JWK struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// JWKSet — верхний уровень JWKS-документа (RFC 7517 §5).
type JWKSet struct {
	Keys []JWK `json:"keys"`
}

// jwkFromPublicKey — base64url (без padding) big-endian байтов модуля/
// экспоненты, ровно та кодировка, которую RFC 7518 §6.3.1 требует для полей
// `n`/`e`. Написано напрямую поверх math/big + encoding/base64 стандартной
// библиотеки, не через отдельную JWK-библиотеку — в остальном репозитории
// (services/*/go.mod) нет НИ ОДНОЙ зависимости с "jwk"/"jose" в имени (см.
// README credential-issuer-service за тем же принципом минимализма: не
// тянуть SDK ради нескольких строк кодирования).
func jwkFromPublicKey(kid string, pub *rsa.PublicKey) JWK {
	return JWK{
		Kty: "RSA",
		Kid: kid,
		Use: "sig",
		Alg: "RS256",
		N:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
		E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
	}
}
