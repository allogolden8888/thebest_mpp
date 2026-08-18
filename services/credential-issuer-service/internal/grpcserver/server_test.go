package grpcserver

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	grpcv1 "mpp/platformcontracts/grpc/v1"

	"mpp/credential-issuer-service/internal/store"
)

type fakeStore struct {
	credentialRef  string
	lookupErr      error
	recordErr      error
	lastPartnerID  string
	lastApp        string
	lastCredRef    string
	lastIssuedBy   string
	listSecrets    []store.IssuedSecret
	listErr        error
}

func (f *fakeStore) LookupCredentialRef(_ context.Context, partnerID, applicationID string) (string, error) {
	f.lastPartnerID, f.lastApp = partnerID, applicationID
	if f.lookupErr != nil {
		return "", f.lookupErr
	}
	return f.credentialRef, nil
}

func (f *fakeStore) RecordRotation(_ context.Context, partnerID, applicationID, credentialRef, issuedBy string) (store.IssuedSecret, error) {
	f.lastCredRef, f.lastIssuedBy = credentialRef, issuedBy
	if f.recordErr != nil {
		return store.IssuedSecret{}, f.recordErr
	}
	return store.IssuedSecret{
		ID: 1, PartnerID: partnerID, ApplicationID: applicationID, CredentialRef: credentialRef,
		SecretVersion: 1, Status: "active", IssuedAt: time.Unix(0, 0), IssuedBy: issuedBy,
	}, nil
}

func (f *fakeStore) ListIssuedSecrets(_ context.Context, _ string) ([]store.IssuedSecret, error) {
	return f.listSecrets, f.listErr
}

type fakeVault struct {
	writeErr    error
	lastKVPath  string
	lastProp    string
	lastValue   string
	writeCalled bool
}

func (f *fakeVault) WriteKV2Field(_ context.Context, kvPath, property, value string) error {
	f.writeCalled = true
	f.lastKVPath, f.lastProp, f.lastValue = kvPath, property, value
	return f.writeErr
}

func fixedSecretGenerator() (string, error) { return "fixed-plaintext-secret", nil }

func grpcCode(t *testing.T, err error) codes.Code {
	t.Helper()
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("ожидали gRPC status error, получили %v", err)
	}
	return st.Code()
}

func TestRotateCredentialRequiresFields(t *testing.T) {
	s := New(&fakeStore{}, &fakeVault{}, fixedSecretGenerator)

	cases := []*grpcv1.RotateCredentialRequest{
		{ApplicationId: "app", IssuedBy: "admin"},
		{PartnerId: "p", IssuedBy: "admin"},
		{PartnerId: "p", ApplicationId: "app"},
	}
	for _, req := range cases {
		if _, err := s.RotateCredential(context.Background(), req); grpcCode(t, err) != codes.InvalidArgument {
			t.Errorf("запрос %+v должен давать InvalidArgument, получили %v", req, err)
		}
	}
}

func TestRotateCredentialHappyPathWritesVaultThenPostgres(t *testing.T) {
	fs := &fakeStore{credentialRef: "vault://partners/click_uz/main/api_key"}
	fv := &fakeVault{}
	s := New(fs, fv, fixedSecretGenerator)

	resp, err := s.RotateCredential(context.Background(), &grpcv1.RotateCredentialRequest{
		PartnerId: "click_uz", ApplicationId: "main", IssuedBy: "admin-1",
	})
	if err != nil {
		t.Fatalf("RotateCredential: %v", err)
	}
	if resp.GetPlaintextSecret() != "fixed-plaintext-secret" {
		t.Errorf("неожиданный plaintext: %q", resp.GetPlaintextSecret())
	}
	if resp.GetCredentialRef() != "vault://partners/click_uz/main/api_key" {
		t.Errorf("неожиданный credential_ref: %q", resp.GetCredentialRef())
	}
	if !fv.writeCalled {
		t.Fatal("ожидали вызов WriteKV2Field")
	}
	if fv.lastKVPath != "partners/click_uz/main" || fv.lastProp != "api_key" {
		t.Errorf("неверный KV path/property: path=%q prop=%q", fv.lastKVPath, fv.lastProp)
	}
	if fv.lastValue != "fixed-plaintext-secret" {
		t.Errorf("Vault получил не тот plaintext: %q", fv.lastValue)
	}
	if fs.lastCredRef != "vault://partners/click_uz/main/api_key" || fs.lastIssuedBy != "admin-1" {
		t.Errorf("RecordRotation получил неверные аргументы: credRef=%q issuedBy=%q", fs.lastCredRef, fs.lastIssuedBy)
	}
}

func TestRotateCredentialLookupNotFoundMapsToNotFound(t *testing.T) {
	cases := []error{store.ErrPartnerNotFound, store.ErrApplicationNotFound}
	for _, wantErr := range cases {
		fs := &fakeStore{lookupErr: wantErr}
		s := New(fs, &fakeVault{}, fixedSecretGenerator)

		_, err := s.RotateCredential(context.Background(), &grpcv1.RotateCredentialRequest{
			PartnerId: "p", ApplicationId: "app", IssuedBy: "admin",
		})
		if grpcCode(t, err) != codes.NotFound {
			t.Errorf("ожидали NotFound для %v, получили %v", wantErr, err)
		}
	}
}

func TestRotateCredentialMalformedCredentialRefRejected(t *testing.T) {
	fs := &fakeStore{credentialRef: "not-a-vault-ref"}
	fv := &fakeVault{}
	s := New(fs, fv, fixedSecretGenerator)

	_, err := s.RotateCredential(context.Background(), &grpcv1.RotateCredentialRequest{
		PartnerId: "p", ApplicationId: "app", IssuedBy: "admin",
	})
	if grpcCode(t, err) != codes.FailedPrecondition {
		t.Errorf("ожидали FailedPrecondition, получили %v", err)
	}
	if fv.writeCalled {
		t.Error("Vault не должен был вызываться при некорректном credential_ref")
	}
}

func TestRotateCredentialVaultFailureDoesNotTouchPostgres(t *testing.T) {
	fs := &fakeStore{credentialRef: "vault://partners/p/app/api_key"}
	fv := &fakeVault{writeErr: errors.New("vault unavailable")}
	s := New(fs, fv, fixedSecretGenerator)

	_, err := s.RotateCredential(context.Background(), &grpcv1.RotateCredentialRequest{
		PartnerId: "p", ApplicationId: "app", IssuedBy: "admin",
	})
	if grpcCode(t, err) != codes.Unavailable {
		t.Errorf("ожидали Unavailable, получили %v", err)
	}
	if fs.lastCredRef != "" {
		t.Error("RecordRotation не должен был вызываться, если запись в Vault упала — иначе Postgres думал бы, что секрет выпущен, когда Vault его не содержит")
	}
}

func TestRotateCredentialPostgresFailureAfterSuccessfulVaultWriteReturnsInternal(t *testing.T) {
	fs := &fakeStore{credentialRef: "vault://partners/p/app/api_key", recordErr: errors.New("db down")}
	fv := &fakeVault{}
	s := New(fs, fv, fixedSecretGenerator)

	_, err := s.RotateCredential(context.Background(), &grpcv1.RotateCredentialRequest{
		PartnerId: "p", ApplicationId: "app", IssuedBy: "admin",
	})
	if grpcCode(t, err) != codes.Internal {
		t.Errorf("ожидали Internal, получили %v", err)
	}
	if !fv.writeCalled {
		t.Error("Vault ДОЛЖЕН был уже быть записан к этому моменту (Vault-запись идёт до Postgres)")
	}
}

func TestListIssuedSecretsRequiresPartnerID(t *testing.T) {
	s := New(&fakeStore{}, &fakeVault{}, fixedSecretGenerator)
	if _, err := s.ListIssuedSecrets(context.Background(), &grpcv1.ListIssuedSecretsRequest{}); grpcCode(t, err) != codes.InvalidArgument {
		t.Error("пустой partner_id должен давать InvalidArgument")
	}
}

func TestListIssuedSecretsTranslatesToProto(t *testing.T) {
	fs := &fakeStore{listSecrets: []store.IssuedSecret{
		{ID: 1, PartnerID: "p", ApplicationID: "app", CredentialRef: "vault://partners/p/app/api_key", SecretVersion: 2, Status: "active", IssuedAt: time.Unix(0, 0), IssuedBy: "admin"},
	}}
	s := New(fs, &fakeVault{}, fixedSecretGenerator)

	resp, err := s.ListIssuedSecrets(context.Background(), &grpcv1.ListIssuedSecretsRequest{PartnerId: "p"})
	if err != nil {
		t.Fatalf("ListIssuedSecrets: %v", err)
	}
	if len(resp.GetSecrets()) != 1 || resp.GetSecrets()[0].GetSecretVersion() != 2 {
		t.Errorf("неожиданный ответ: %+v", resp)
	}
}

func TestRandomSecretGeneratorProducesDistinctNonEmptyValues(t *testing.T) {
	a, err := RandomSecretGenerator()
	if err != nil {
		t.Fatalf("RandomSecretGenerator: %v", err)
	}
	b, err := RandomSecretGenerator()
	if err != nil {
		t.Fatalf("RandomSecretGenerator: %v", err)
	}
	if a == "" || b == "" {
		t.Fatal("секрет не должен быть пустым")
	}
	if a == b {
		t.Error("два вызова подряд не должны совпасть — иначе генератор не случаен")
	}
}
