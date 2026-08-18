package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "mpp/platformcontracts/common/v1"
	grpcv1 "mpp/platformcontracts/grpc/v1"
)

const configRPCTimeout = 5 * time.Second

// billingTariff — config_schemas/billing_tariff.schema.json.
type billingTariff struct {
	Currency         string                `json:"currency"`
	PricePerSegment  map[string]int64      `json:"price_per_segment"`
	DefaultCategory  string                `json:"default_category"`
	RecurringCharges *recurringChargesSpec `json:"recurring_charges,omitempty"`
}

type recurringChargesSpec struct {
	AlphanameMonthlyFee *int64              `json:"alphaname_monthly_fee,omitempty"`
	ServiceSmsPackage   *servicePackageSpec `json:"service_sms_package,omitempty"`
}

type servicePackageSpec struct {
	Segments int64 `json:"segments"`
	Price    int64 `json:"price"`
}

// getPartnerTariff — GetActiveVersion(BILLING_TARIFF, partnerID). Найдено
// при закрытии Фазы 5a: до неё ни один BILLING_TARIFF config version не
// публиковался ни для одного партнёра (billing-service читал единственный
// статический дефолт-файл) — NotFound здесь штатный, реальный результат,
// не ошибка вызывающей стороны. found=false означает "для этого партнёра
// применяется платформенный дефолт billing-service, конкретные цифры
// которого этому сервису недоступны" (дефолт зашит в образ billing-service,
// не проходит через configuration-service).
func getPartnerTariff(ctx context.Context, client grpcv1.ConfigServiceClient, partnerID string) (billingTariff, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, configRPCTimeout)
	defer cancel()

	resp, err := client.GetActiveVersion(ctx, &grpcv1.GetActiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_BILLING_TARIFF,
		EntityId:   partnerID,
	})
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return billingTariff{}, false, nil
		}
		return billingTariff{}, false, fmt.Errorf("не удалось прочитать тариф партнёра: %w", err)
	}

	var tariff billingTariff
	if err := json.Unmarshal(resp.GetPayloadJson(), &tariff); err != nil {
		return billingTariff{}, false, fmt.Errorf("не удалось разобрать тариф партнёра: %w", err)
	}
	return tariff, true, nil
}

type partnerSender struct {
	SenderID string `json:"sender_id"`
	Type     string `json:"type"`
	Status   string `json:"status"`
}

type partnerDocument struct {
	PartnerID string          `json:"partner_id"`
	Senders   []partnerSender `json:"senders"`
}

// getActiveSenders — GetActiveVersion(PARTNER, partnerID), фильтрует
// status=active (только active senders несут alphaname_monthly_fee —
// archived не тарифицируется, config_schemas/billing_tariff.schema.json
// doc-комментарий на recurring_charges).
func getActiveSenders(ctx context.Context, client grpcv1.ConfigServiceClient, partnerID string) ([]partnerSender, error) {
	ctx, cancel := context.WithTimeout(ctx, configRPCTimeout)
	defer cancel()

	resp, err := client.GetActiveVersion(ctx, &grpcv1.GetActiveVersionRequest{
		EntityType: commonv1.ConfigEntityType_CONFIG_ENTITY_TYPE_PARTNER,
		EntityId:   partnerID,
	})
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать конфиг партнёра: %w", err)
	}

	var doc partnerDocument
	if err := json.Unmarshal(resp.GetPayloadJson(), &doc); err != nil {
		return nil, fmt.Errorf("не удалось разобрать конфиг партнёра: %w", err)
	}

	var active []partnerSender
	for _, s := range doc.Senders {
		if s.Status == "active" {
			active = append(active, s)
		}
	}
	return active, nil
}
