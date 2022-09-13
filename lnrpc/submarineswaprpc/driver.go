//go:build submarineswaprpc
// +build submarineswaprpc

package submarineswaprpc

import (
	"fmt"

	"github.com/lightningnetwork/lnd/lnrpc"
)

// createNewSubServer is a helper method that will create the new submarine
// swapper sub server given the main config dispatcher method. If we're unable
// to find the config that is meant for us in the config dispatcher, then
// we'll exit with an error.
func createNewSubServer(configRegistry lnrpc.SubServerConfigDispatcher) (
	*Server, lnrpc.MacaroonPerms, error) {

	// We'll attempt to look up the config that we expect, according to our
	// subServerName name. If we can't find this, then we'll exit with an
	// error, as we're unable to properly initialize ourselves without this
	// config.
	submarineSwapperServerConf, ok := configRegistry.FetchConfig(subServerName)
	if !ok {
		return nil, nil, fmt.Errorf("unable to find config for "+
			"subserver type %s", subServerName)
	}

	// Now that we've found an object mapping to our service name, we'll
	// ensure that it's the type we need.
	config, ok := submarineSwapperServerConf.(*Config)
	if !ok {
		return nil, nil, fmt.Errorf("wrong type of config for "+
			"subserver %s, expected %T got %T", subServerName,
			&Config{}, submarineSwapperServerConf)
	}

	// Before we try to make the new submarine swapper service instance, we'll
	// perform some sanity checks on the arguments to ensure that they're
	// usable.
	switch {
	// If the macaroon service is set (we should use macaroons), then
	// ensure that we know where to look for them, or create them if not
	// found.
	case config.MacService != nil && config.NetworkDir == "":
		return nil, nil, fmt.Errorf("NetworkDir must be set to create " +
			"submarineswaprpc")
	case config.FeeEstimator == nil:
		return nil, nil, fmt.Errorf("FeeEstimator must be set to " +
			"create submarineswaprpc")
	case config.Wallet == nil:
		return nil, nil, fmt.Errorf("Wallet must be set to " +
			"create submarineswaprpc")
	}

	return New(config)
}

func init() {
	subServer := &lnrpc.SubServerDriver{
		SubServerName: subServerName,
		NewGrpcHandler: func() lnrpc.GrpcHandler {
			return &ServerShell{}
		},
	}

	// If the build tag is active, then we'll register ourselves as a
	// sub-RPC server within the global lnrpc package namespace.
	if err := lnrpc.RegisterSubServer(subServer); err != nil {
		panic(fmt.Sprintf("failed to register subserver driver %s: %v",
			subServerName, err))
	}
}
