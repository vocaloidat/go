package actions

import (
	"context"

	"github.com/stellar/go/services/horizon/internal/db2"
	"github.com/stellar/go/services/horizon/internal/db2/history"
	"github.com/stellar/go/services/horizon/internal/render/sse"
	"github.com/stellar/go/services/horizon/internal/resourceadapter"
	"github.com/stellar/go/support/errors"
	"github.com/stellar/go/support/render/hal"
	"github.com/stellar/go/support/render/problem"
)

// StreamEffects streams effect records of an account/operation/transaction/ledger
// identified by accountID/operationID/transactionHash/ledgerID based on pq.
func StreamEffects(ctx context.Context, s *sse.Stream, hq *history.Q,
	accountID string, operationID int64, transactionHash string, ledgerID int32, pq db2.PageQuery) error {
	allRecords, err := loadEffectRecords(hq, accountID, operationID, transactionHash, ledgerID, pq)
	if err != nil {
		return errors.Wrap(err, "loading transaction records")
	}

	ledgers, err := loadEffectLedgers(hq, allRecords)
	if err != nil {
		return errors.Wrap(err, "loading ledgers")
	}

	s.SetLimit(int(pq.Limit))
	records := allRecords[s.SentCount():]
	for _, record := range records {
		res, err := resourceadapter.NewEffect(ctx, record, ledgers[record.LedgerSequence()])
		if err != nil {
			return errors.Wrap(err, "could not create effect")
		}
		s.Send(sse.Event{ID: res.PagingToken(), Data: res})
	}

	return nil
}

// EffectsPage returns a page containing the transaction records of an
// account/operation/transaction/ledger identified by identified by accountID/operationID/transactionHash/ledgerID
// into a page, based on pq.
func EffectsPage(ctx context.Context, hq *history.Q,
	accountID string, operationID int64, transactionHash string, ledgerID int32, pq db2.PageQuery) (hal.Page, error) {
	records, err := loadEffectRecords(hq, accountID, operationID, transactionHash, ledgerID, pq)
	if err != nil {
		return hal.Page{}, errors.Wrap(err, "loading transaction records")
	}

	page := hal.Page{
		Cursor: pq.Cursor,
		Order:  pq.Order,
		Limit:  pq.Limit,
	}

	ledgers, err := loadEffectLedgers(hq, records)
	if err != nil {
		return hal.Page{}, errors.Wrap(err, "loading ledgers")
	}

	for _, record := range records {
		res, err := resourceadapter.NewEffect(ctx, record, ledgers[record.LedgerSequence()])
		if err != nil {
			return hal.Page{}, errors.Wrap(err, "could not create effect")
		}
		page.Add(res)
	}

	page.FullURL = FullURL(ctx)
	page.PopulateLinks()
	return page, nil
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
