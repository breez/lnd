// +build breezbackuprpc

package breezbackuprpc

import (
	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcwallet/walletdb"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/lightningnetwork/lnd/macaroons"
)

// Config is the primary configuration struct for the backup RPC server. It
// contains all the items required for the RPC server to carry out its duties.
// The fields with struct tags are meant to be parsed as normal configuration
// options, while if able to be populated, the latter fields MUST also be
// specified.
type Config struct {
	// BreezBackuperMacPath is the path for the breez backuper macaroon. If
	// unspecified then we assume that the macaroon will be found under the
	// network directory, named DefaultBreezBackuperMacFilename.
	BreezBackuperMacPath string `long:"breezbackupmacaroonpath" description:"Path to the breez backup macaroon"`

	// NetworkDir is the main network directory wherein the breez backuper
	// RPC server will find the macaroon named
	// DefaultBreezBackuperMacFilename.
	NetworkDir string

	// ActiveNetParams are the network parameters of the primary network
	// that the breez backuper is operating on. This is necessary so we can
	// ensure that we receive payment requests that send to destinations on our
	// network.
	ActiveNetParams *chaincfg.Params

	// MacService is the main macaroon service that we'll use to handle
	// authentication for the breez backuper RPC server.
	MacService *macaroons.Service

	ChannelDB *channeldb.DB
	WalletDB  walletdb.DB
}
