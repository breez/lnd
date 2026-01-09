//go:build integration && submarineswaprpc

package itest

import "github.com/lightningnetwork/lnd/lntest"

// submarineSwapTestCases defines the test cases for submarine swap
// functionality. These tests require the submarineswaprpc build tag.
var submarineSwapTestCases = []*lntest.TestCase{
	{
		Name:     "happy path",
		TestFunc: testSubmarineSwapHappyPath,
	},
	{
		Name:     "fee estimation",
		TestFunc: testSubmarineSwapFeeEstimation,
	},
	{
		Name:     "error cases",
		TestFunc: testSubmarineSwapErrorCases,
	},
	{
		Name:     "upgrade from v0.18.5",
		TestFunc: testSubmarineSwapUpgrade,
	},
}

func init() {
	// Register submarine swap tests with prefix.
	allTestCases = appendPrefixed(
		"submarine swap", allTestCases, submarineSwapTestCases,
	)
}
