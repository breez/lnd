// +build backuprpc

package backuprpc

import (	
	"github.com/lightningnetwork/lnd/macaroons"
	"github.com/lightningnetwork/lnd/backupnotifier"
)

// Config is the primary configuration struct for the backup RPC server.
// It contains all the items required for the server to carry out its duties.
// The fields with struct tags are meant to be parsed as normal configuration
// options, while if able to be populated, the latter fields MUST also be
// specified.
type Config struct {
	// BackupMacPath is the path for the backup macaroon. If
	// unspecified then we assume that the macaroon will be found under the
	// network directory, named DefaultBackupMacFilename.
	BackupMacPath string `long:"backupmacaroonpath" description:"Path to the backup macaroon"`

	// NetworkDir is the main network directory wherein the backup
	// RPC server will find the macaroon named
	// DefaultBackupMacFilename.
	NetworkDir string

	// MacService is the main macaroon service that we'll use to handle
	// authentication for the backup RPC server.
	MacService *macaroons.Service
	
	// BackupNotifier is the struct responsible for notifying backup events
	BackupNotifier *backupnotifier.BackupNotifier
}
