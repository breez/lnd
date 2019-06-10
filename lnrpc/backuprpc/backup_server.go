// +build backuprpc

package backuprpc

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/macaroons"
	"google.golang.org/grpc"
	"gopkg.in/macaroon-bakery.v2/bakery"
)

const (
	// subServerName is the name of the RPC sub-server. We'll use this name
	// to register ourselves, and we also require that the main
	// SubServerConfigDispatcher instance recognize this as the name of the
	// config file that we need.
	subServerName = "BackupRPC"
)

var (
	// macaroonOps are the set of capabilities that our minted macaroon (if
	// it doesn't already exist) will have.
	macaroonOps = []bakery.Op{
		{
			Entity: "onchain",
			Action: "read",
		},
	}

	// macPermissions maps RPC calls to the permissions they require.
	macPermissions = map[string][]bakery.Op{
		"/backuprpc.Backup/SubscribeBackupEvents": {{
			Entity: "onchain",
			Action: "read",
		}},
	}

	// DefaultBackupMacFilename is the default name of the backup
	// macaroon that we expect to find via a file handle within the
	// main configuration file in this package.
	DefaultBackupMacFilename = "backupevents.macaroon"
)

// fileExists reports whether the named file or directory exists.
func fileExists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// Server is a sub-server of the main RPC server: the backup RPC. This
// RPC sub-server allows external callers to access the supscribe for
// domains.
type Server struct {
	started uint32
	stopped uint32

	cfg Config

	quit chan struct{}
}

// New returns a new instance of the backuprpc Backup sub-server. We also
// return the set of permissions for the macaroons that we may create within
// this method. If the macaroons we need aren't found in the filepath, then
// we'll create them on start up. If we're unable to locate, or create the
// macaroons we need, then we'll return with an error.
func New(cfg *Config) (*Server, lnrpc.MacaroonPerms, error) {
	// If the path of the backup macaroon wasn't generated, then
	// we'll assume that it's found at the default network directory.
	if cfg.BackupMacPath == "" {
		cfg.BackupMacPath = filepath.Join(
			cfg.NetworkDir, DefaultBackupMacFilename,
		)
	}

	// Now that we know the full path of the backup macaroon, we can
	// check to see if we need to create it or not.
	macFilePath := cfg.BackupMacPath
	if cfg.MacService != nil && !fileExists(macFilePath) {
		log.Infof("Baking macaroons for backup RPC Server at: %v",
			macFilePath)

		// At this point, we know that the backkup macaroon
		// doesn't yet, exist, so we need to create it with the help of
		// the main macaroon service.
		backupMac, err := cfg.MacService.NewMacaroon(
			context.Background(), macaroons.DefaultRootKeyID,
			macaroonOps...,
		)
		if err != nil {
			return nil, nil, err
		}
		backupMacBytes, err := backupMac.M().MarshalBinary()
		if err != nil {
			return nil, nil, err
		}
		err = ioutil.WriteFile(macFilePath, backupMacBytes, 0644)
		if err != nil {
			os.Remove(macFilePath)
			return nil, nil, err
		}
	}

	return &Server{
		cfg:  *cfg,
		quit: make(chan struct{}),
	}, macPermissions, nil
}

// Compile-time checks to ensure that Server fully implements the
// BackupServer gRPC service and lnrpc.SubServer interface.
var _ BackupServer = (*Server)(nil)
var _ lnrpc.SubServer = (*Server)(nil)

// Start launches any helper goroutines required for the server to function.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Start() error {
	if !atomic.CompareAndSwapUint32(&s.started, 0, 1) {
		return nil
	}

	return nil
}

// Stop signals any active goroutines for a graceful closure.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Stop() error {
	if !atomic.CompareAndSwapUint32(&s.stopped, 0, 1) {
		return nil
	}

	close(s.quit)

	return nil
}

// Name returns a unique string representation of the sub-server. This can be
// used to identify the sub-server and also de-duplicate them.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) Name() string {
	return subServerName
}

// RegisterWithRootServer will be called by the root gRPC server to direct a RPC
// sub-server to register itself with the main gRPC root server. Until this is
// called, each sub-server won't be able to have requests routed towards it.
//
// NOTE: This is part of the lnrpc.SubServer interface.
func (s *Server) RegisterWithRootServer(grpcServer *grpc.Server) error {
	// We make sure that we register it with the main gRPC server to ensure
	// all our methods are routed properly.
	RegisterBackupServer(grpcServer, s)
	log.Infof("Backup RPC server successfully register with root " +
		"gRPC server")
	return nil
}

func (s *Server) RegisterWithRestServer(ctx context.Context,
	mux *runtime.ServeMux, dest string, opts []grpc.DialOption) error {

	return nil
}

// SubscribeBackupEvents returns a uni-directional stream (server -> client)
// for notifying the client of points in time where backup is needed.
func (r *Server) SubscribeBackupEvents(req *BackupEventSubscription,
	updateStream Backup_SubscribeBackupEventsServer) error {

	backupEventSub, err := r.cfg.BackupNotifier.SubscribeBackupEvents()
	if err != nil {
		return err
	}

	// Ensure that the resources for the client is cleaned up once either
	// the server, or client exits.
	defer backupEventSub.Cancel()

	for {
		select {
		// A new backup event was sent
		case _ = <-backupEventSub.Updates():
			if err := updateStream.Send(&BackupEventUpdate{}); err != nil {
				return err
			}
		case <-r.quit:
			return nil
		}
	}
}
