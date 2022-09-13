//go:build submarineswaprpc
// +build submarineswaprpc

package submarineswaprpc

import (
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/lightningnetwork/lnd/lnwallet"
	"github.com/lightningnetwork/lnd/lnwallet/chainfee"
	"github.com/lightningnetwork/lnd/macaroons"
)

// Config is the primary configuration struct for the submarine swapper RPC server.
// It contains all the items required for the server to carry out its duties.
// The fields with struct tags are meant to be parsed as normal configuration
// options, while if able to be populated, the latter fields MUST also be
// specified.
type Config struct {
	// SubmarineSwapMacPath is the path for the submarine swapper macaroon. If
	// unspecified then we assume that the macaroon will be found under the
	// network directory, named DefaultSubmarineSwapMacFilename.
	SubmarineSwapMacPath string `long:"submarineswapmacaroonpath" description:"Path to the submarine swap macaroon"`

	// NetworkDir is the main network directory wherein the submarine swapper
	// RPC server will find the macaroon named
	// DefaultSubmarineSwapMacFilename.
	NetworkDir string

	// ActiveNetParams are the network parameters of the primary network
	// that the submarine swapper is operating on. This is necessary so we can
	// ensure that we receive payment requests that send to destinations on our
	// network.
	ActiveNetParams *chaincfg.Params

	// MacService is the main macaroon service that we'll use to handle
	// authentication for the submarine swapper RPC server.
	MacService *macaroons.Service

	FeeEstimator chainfee.Estimator

	Wallet *lnwallet.LightningWallet
}
