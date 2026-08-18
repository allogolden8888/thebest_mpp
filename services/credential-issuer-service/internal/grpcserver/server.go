// Package grpcserver реализует mpp.grpc.v1.CredentialIssuerService
// (platform-contracts/grpc/credentials.proto). Порядок операций в
// RotateCredential — lookup credential_ref (Postgres) -> сгенерировать
// секрет -> записать в Vault -> ТОЛЬКО ПОТОМ зафиксировать в Postgres
// (issued_secrets + rotation_audit). Если Vault-запись падает, Postgres
// вообще не трогается — не бывает состояния "Postgres думает, что секрет
// выпущен, а Vault его не содержит" (в отличие от обратного порядка, где
// пришлось бы откатывать Postgres-транзакцию по ошибке ВНЕШНЕЙ системы).
package grpcserver

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/credential-issuer-service/internal/store"
	"mpp/credential-issuer-service/internal/vault"
)

// Store — минимальный интерфейс, который нужен серверу от store.Postgres
// (фейк в тестах без реального Postgres).
type Store interface {
	LookupCredentialRef(ctx context.Context, partnerID, applicationID string) (string, error)
	RecordRotation(ctx context.Context, partnerID, applicationID, credentialRef, issuedBy string) (store.IssuedSecret, error)
	ListIssuedSecrets(ctx context.Context, partnerID string) ([]store.IssuedSecret, error)
}

// VaultWriter — минимальный интерфейс от vault.Client (фейк в тестах без
// реального Vault).
type VaultWriter interface {
	WriteKV2Field(ctx context.Context, kvPath, property, value string) error
}

// SecretGenerator — генерация plaintext секрета, параметризовано ради
// детерминированных тестов (реальная реализация — crypto/rand ниже).
type SecretGenerator func() (string, error)

// secretByteLength — 32 байта случайности перед base64 (256 бит) — тот же
// порядок величины, что типичные API-key генераторы, с большим запасом
// против brute force при разумной длине передаваемой строки.
const secretByteLength = 32

func RandomSecretGenerator() (string, error) {
	buf := make([]byte, secretByteLength)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("генерация случайного секрета: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

type Server struct {
	grpcv1.UnimplementedCredentialIssuerServiceServer

	store           Store
	vaultClient     VaultWriter
	secretGenerator SecretGenerator
}

func New(s Store, vaultClient VaultWriter, secretGenerator SecretGenerator) *Server {
	if secretGenerator == nil {
		secretGenerator = RandomSecretGenerator
	}
	return &Server{store: s, vaultClient: vaultClient, secretGenerator: secretGenerator}
}

func (s *Server) RotateCredential(ctx context.Context, req *grpcv1.RotateCredentialRequest) (*grpcv1.RotateCredentialResponse, error) {
	if req.GetPartnerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "partner_id обязателен")
	}
	if req.GetApplicationId() == "" {
		return nil, status.Error(codes.InvalidArgument, "application_id обязателен")
	}
	if req.GetIssuedBy() == "" {
		return nil, status.Error(codes.InvalidArgument, "issued_by обязателен для аудита")
	}

	credentialRef, err := s.store.LookupCredentialRef(ctx, req.GetPartnerId(), req.GetApplicationId())
	if err != nil {
		switch {
		case errors.Is(err, store.ErrPartnerNotFound), errors.Is(err, store.ErrApplicationNotFound):
			return nil, status.Error(codes.NotFound, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	kvPath, property, err := vault.ParseCredentialRef(credentialRef)
	if err != nil {
		// credential_ref в PARTNER-конфиге в неожиданной форме — конфигурационная
		// ошибка выше по стеку (configuration-service должен был бы отклонить
		// такой payload при валидации схемы), не наша, но не молчим об этом.
		return nil, status.Errorf(codes.FailedPrecondition, "credential_ref %q не в ожидаемой форме vault://path/property: %v", credentialRef, err)
	}

	plaintext, err := s.secretGenerator()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	if err := s.vaultClient.WriteKV2Field(ctx, kvPath, property, plaintext); err != nil {
		return nil, status.Errorf(codes.Unavailable, "запись в Vault не удалась: %v", err)
	}

	issued, err := s.store.RecordRotation(ctx, req.GetPartnerId(), req.GetApplicationId(), credentialRef, req.GetIssuedBy())
	if err != nil {
		// Секрет уже записан в Vault (новое значение по факту действует), но
		// Postgres-запись метаданных не удалась — не откатываем Vault
		// (предыдущее значение всё равно потеряно бы для читателей нового
		// секрета), явно возвращаем Internal, чтобы вызывающий знал, что
		// state рассинхронизирован и стоит повторить/проверить вручную.
		return nil, status.Errorf(codes.Internal, "секрет записан в Vault, но не удалось зафиксировать в Postgres: %v", err)
	}

	return &grpcv1.RotateCredentialResponse{
		CredentialRef:   credentialRef,
		SecretVersion:   issued.SecretVersion,
		PlaintextSecret: plaintext,
		IssuedAt:        timestamppb.New(issued.IssuedAt),
	}, nil
}

func (s *Server) ListIssuedSecrets(ctx context.Context, req *grpcv1.ListIssuedSecretsRequest) (*grpcv1.ListIssuedSecretsResponse, error) {
	if req.GetPartnerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "partner_id обязателен")
	}

	secrets, err := s.store.ListIssuedSecrets(ctx, req.GetPartnerId())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	resp := &grpcv1.ListIssuedSecretsResponse{Secrets: make([]*grpcv1.IssuedSecretSummary, 0, len(secrets))}
	for _, sec := range secrets {
		resp.Secrets = append(resp.Secrets, &grpcv1.IssuedSecretSummary{
			Id:            sec.ID,
			PartnerId:     sec.PartnerID,
			ApplicationId: sec.ApplicationID,
			CredentialRef: sec.CredentialRef,
			SecretVersion: sec.SecretVersion,
			Status:        sec.Status,
			IssuedAt:      timestamppb.New(sec.IssuedAt),
			IssuedBy:      sec.IssuedBy,
		})
	}
	return resp, nil
}
