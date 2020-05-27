package actions

import (
	"net/http"

	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/resourceadapter"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/support/render/problem"
)

// EffectsQuery query struct for effects end-points
type EffectsQuery struct {
	AccountID   string `schema:"account_id" valid:"accountID,optional"`
	OperationID int64  `schema:"op_id" valid:"-"`
	TxHash      string `schema:"tx_id" valid:"-"`
	LedgerID    int32  `schema:"ledger_id" valid:"ledgerID,optional"`
}

type GetEffectsHandler struct{}

func (handler GetEffectsHandler) GetResourcePage(w HeaderWriter, r *http.Request) ([]hal.Pageable, error) {
	pq, err := GetPageQuery(r)
	if err != nil {
		return nil, err
	}

	err = ValidateCursorWithinHistory(pq)
	if err != nil {
		return nil, err
	}

	qp := EffectsQuery{}
	err = GetParams(&qp, r)
	if err != nil {
		return nil, err
	}

	historyQ, err := HistoryQFromRequest(r)
	if err != nil {
		return nil, err
	}

	records, err := loadEffectRecords(historyQ, qp.AccountID, qp.OperationID, qp.TxHash, qp.LedgerID, pq)
	if err != nil {
		return nil, errors.Wrap(err, "loading transaction records")
	}

	ledgers, err := loadEffectLedgers(historyQ, records)
	if err != nil {
		return nil, errors.Wrap(err, "loading ledgers")
	}

	var result []hal.Pageable
	for _, record := range records {
		effect, err := resourceadapter.NewEffect(r.Context(), record, ledgers[record.LedgerSequence()])
		if err != nil {
			return nil, errors.Wrap(err, "could not create effect")
		}
		result = append(result, effect)
	}

	return result, nil
}

func loadEffectRecords(hq *history.Q, accountID string, operationID int64, transactionHash string, ledgerID int32,
	pq db2.PageQuery) ([]history.Effect, error) {

	count, err := CountNonEmpty(
		accountID,
		operationID,
		transactionHash,
		ledgerID,
	)

	if err != nil {
		return nil, errors.Wrap(err, "error in CountNonEmpty")
	}

	if count > 1 {
		return nil, problem.BadRequest
	}

	effects := hq.Effects()

	switch {
	case accountID != "":
		effects.ForAccount(accountID)
	case ledgerID > 0:
		effects.ForLedger(ledgerID)
	case operationID > 0:
		effects.ForOperation(operationID)
	case transactionHash != "":
		effects.ForTransaction(transactionHash)
	}
	var result []history.Effect
	err = effects.Page(pq).Select(&result)
	return result, err
}

func loadEffectLedgers(hq *history.Q, effects []history.Effect) (map[int32]history.Ledger, error) {
	ledgers := &history.LedgerCache{}

	for _, e := range effects {
		ledgers.Queue(e.LedgerSequence())
	}

	if err := ledgers.Load(hq); err != nil {
		return nil, err
	}
	return ledgers.Records, nil
}
