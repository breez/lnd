//go:build chanreservedynamic
// +build chanreservedynamic

package lnd

import (
	"strconv"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/lightningnetwork/lnd/lncfg"
	"go.starlark.net/starlark"
)

func requiredRemoteChanReserve(chainCfg *lncfg.Chain,
	chanAmt, defaultReserve btcutil.Amount) (reserve btcutil.Amount, err error) {
	reserve = defaultReserve
	if len(chainCfg.ChanReserveScript) == 0 {
		return
	}

	var value starlark.Value
	value, err = starlark.Eval(
		&starlark.Thread{},
		"",
		chainCfg.ChanReserveScript,
		starlark.StringDict{
			"chanAmt": starlark.MakeInt64(int64(chanAmt)),
		},
	)
	if err != nil {
		if evalErr, ok := err.(*starlark.EvalError); ok {
			srvrLog.Debugf("error in starlark.Eval: %v", evalErr.Backtrace())
		}
		srvrLog.Debugf("error in starlark.Eval error: %v", err)
		return
	}

	if value.Type() == "int" {
		var i int64
		i, err = strconv.ParseInt(value.String(), 10, 64)
		if err == nil {
			reserve = btcutil.Amount(i)
			return
		}
	}

	return
}
