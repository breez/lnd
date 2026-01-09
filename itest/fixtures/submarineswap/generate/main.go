//go:build ignore

// This tool generates submarine swap database fixtures for upgrade testing.
// It starts btcd and lnd-v0.18.5 to create a swap, then exports the database.
//
// Usage:
//
//	go run main.go -lnd-binary=./lnd-v0.18.5 -output=../
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/submarineswaprpc"
	"github.com/lightningnetwork/lnd/lntest/node"
	"github.com/lightningnetwork/lnd/macaroons"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/macaroon.v2"
)

var (
	lndBinary = flag.String("lnd-binary", "./lnd-v0.18.5", "Path to lnd v0.18.5 binary")
	outputDir = flag.String("output", ".", "Output directory for fixtures")
)

// FixtureMetadata matches the format expected by the upgrade test.
type FixtureMetadata struct {
	Version string `json:"version"`
	State   string `json:"state"`
	Swap    struct {
		Hash          string `json:"hash"`
		Preimage      string `json:"preimage"`
		ClientPubkey  string `json:"client_pubkey"`
		SwapperPubkey string `json:"swapper_pubkey"`
		Address       string `json:"address"`
		LockHeight    int64  `json:"lock_height"`
		AmountSats    int64  `json:"amount_sats"`
	} `json:"swap"`
	Wallet struct {
		Password string `json:"password"`
	} `json:"wallet"`
	BlockHeight int64 `json:"block_height"`
}

// LndProcess manages an LND process.
type LndProcess struct {
	binary    string
	dataDir   string
	rpcPort   int
	restPort  int
	p2pPort   int
	cmd       *exec.Cmd
	conn      *grpc.ClientConn
	lightning lnrpc.LightningClient
	subswap   submarineswaprpc.SubmarineSwapperClient
	unlocker  lnrpc.WalletUnlockerClient
}

func main() {
	flag.Parse()

	log.Println("Starting submarine swap fixture generator...")

	// Verify lnd binary exists
	if _, err := os.Stat(*lndBinary); os.IsNotExist(err) {
		log.Fatalf("LND binary not found at %s", *lndBinary)
	}

	// Create absolute path for lnd binary
	lndPath, err := filepath.Abs(*lndBinary)
	if err != nil {
		log.Fatalf("Failed to get absolute path: %v", err)
	}

	// Start btcd miner
	log.Println("Starting btcd miner...")
	miner, err := setupBtcdMiner()
	if err != nil {
		log.Fatalf("Failed to setup btcd miner: %v", err)
	}
	defer func() {
		log.Println("Shutting down btcd miner...")
		if err := miner.TearDown(); err != nil {
			log.Printf("Warning: miner teardown error: %v", err)
		}
	}()

	// Mine initial blocks
	log.Println("Mining initial blocks...")
	if _, err := miner.Client.Generate(101); err != nil {
		log.Fatalf("Failed to mine initial blocks: %v", err)
	}

	// Start LND (with --noseedbackup, wallet is auto-created)
	log.Println("Starting LND v0.18.5...")
	lnd, err := startLnd(lndPath, miner)
	if err != nil {
		log.Fatalf("Failed to start LND: %v", err)
	}

	// Wait for LND to be ready
	log.Println("Waiting for LND to sync...")
	if err := lnd.waitForSync(); err != nil {
		log.Fatalf("Failed waiting for sync: %v", err)
	}

	// Create submarine swap
	log.Println("Creating submarine swap...")
	metadata, err := createSwap(lnd)
	if err != nil {
		log.Fatalf("Failed to create swap: %v", err)
	}

	// Get block height
	info, err := lnd.lightning.GetInfo(context.Background(), &lnrpc.GetInfoRequest{})
	if err != nil {
		log.Fatalf("Failed to get info: %v", err)
	}
	metadata.BlockHeight = int64(info.BlockHeight)

	// Stop LND gracefully to flush DB
	log.Println("Stopping LND...")
	if err := lnd.stop(); err != nil {
		log.Fatalf("Failed to stop LND: %v", err)
	}

	// Export fixture
	log.Println("Exporting fixture...")
	if err := exportFixture(lnd.dataDir, metadata, *outputDir); err != nil {
		log.Fatalf("Failed to export fixture: %v", err)
	}

	log.Println("Fixture generated successfully!")
	log.Printf("Output: %s/v0.18.5_initialized/\n", *outputDir)
}

func setupBtcdMiner() (*rpctest.Harness, error) {
	btcdBinary := node.GetBtcdBinary()
	handler := &rpcclient.NotificationHandlers{}
	args := []string{
		"--rejectnonstd",
		"--txindex",
		"--debuglevel=info",
		"--trickleinterval=100ms",
	}

	miner, err := rpctest.New(&chaincfg.RegressionNetParams, handler, args, btcdBinary)
	if err != nil {
		return nil, fmt.Errorf("failed to create miner: %w", err)
	}

	if err := miner.SetUp(true, 0); err != nil {
		return nil, fmt.Errorf("failed to setup miner: %w", err)
	}

	return miner, nil
}

func startLnd(binary string, miner *rpctest.Harness) (*LndProcess, error) {
	// Create temp data directory
	dataDir, err := os.MkdirTemp("", "lnd-fixture-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	// Use fixed ports for simplicity
	rpcPort := 10009
	restPort := 8080
	p2pPort := 9735

	lnd := &LndProcess{
		binary:   binary,
		dataDir:  dataDir,
		rpcPort:  rpcPort,
		restPort: restPort,
		p2pPort:  p2pPort,
	}

	// Build command arguments
	// Note: miner.RPCConfig().Host already includes host:port
	args := []string{
		"--bitcoin.active",
		"--bitcoin.regtest",
		"--bitcoin.node=btcd",
		fmt.Sprintf("--btcd.rpchost=%s", miner.RPCConfig().Host),
		fmt.Sprintf("--btcd.rpcuser=%s", miner.RPCConfig().User),
		fmt.Sprintf("--btcd.rpcpass=%s", miner.RPCConfig().Pass),
		"--btcd.rawrpccert=" + hex.EncodeToString(miner.RPCConfig().Certificates),
		fmt.Sprintf("--lnddir=%s", dataDir),
		fmt.Sprintf("--rpclisten=127.0.0.1:%d", rpcPort),
		fmt.Sprintf("--restlisten=127.0.0.1:%d", restPort),
		fmt.Sprintf("--listen=127.0.0.1:%d", p2pPort),
		"--noseedbackup",
		"--norest",
		"--debuglevel=info",
		"--accept-keysend",
		"--trickledelay=50",
	}

	lnd.cmd = exec.Command(binary, args...)
	lnd.cmd.Stdout = os.Stdout
	lnd.cmd.Stderr = os.Stderr

	if err := lnd.cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start lnd: %w", err)
	}

	// Wait for TLS cert to be created
	tlsCertPath := filepath.Join(dataDir, "tls.cert")
	if err := waitForFile(tlsCertPath, 30*time.Second); err != nil {
		lnd.cmd.Process.Kill()
		return nil, fmt.Errorf("tls cert not created: %w", err)
	}

	// With --noseedbackup, LND auto-creates wallet. Wait for macaroon.
	macPath := filepath.Join(dataDir, "data", "chain", "bitcoin", "regtest", "admin.macaroon")
	if err := waitForFile(macPath, 60*time.Second); err != nil {
		lnd.cmd.Process.Kill()
		return nil, fmt.Errorf("macaroon not created: %w", err)
	}

	// Connect with macaroon
	if err := lnd.connectWithMacaroon(); err != nil {
		lnd.cmd.Process.Kill()
		return nil, fmt.Errorf("failed to connect: %w", err)
	}

	return lnd, nil
}

func (l *LndProcess) connectUnlocker() error {
	tlsCertPath := filepath.Join(l.dataDir, "tls.cert")
	creds, err := credentials.NewClientTLSFromFile(tlsCertPath, "")
	if err != nil {
		return fmt.Errorf("failed to load TLS cert: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithBlock(),
	}

	addr := fmt.Sprintf("127.0.0.1:%d", l.rpcPort)
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	l.conn = conn
	l.unlocker = lnrpc.NewWalletUnlockerClient(conn)
	return nil
}

func (l *LndProcess) createWallet() error {
	// Wait for RPC to be ready (retry GenSeed until it works)
	var seedResp *lnrpc.GenSeedResponse
	var err error

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for RPC to be ready: %w", err)
		default:
		}

		seedResp, err = l.unlocker.GenSeed(ctx, &lnrpc.GenSeedRequest{})
		if err == nil {
			break
		}

		// RPC not ready yet, wait and retry
		time.Sleep(500 * time.Millisecond)
	}

	// Initialize wallet
	password := []byte("testpassword12345678")
	_, err = l.unlocker.InitWallet(ctx, &lnrpc.InitWalletRequest{
		WalletPassword:     password,
		CipherSeedMnemonic: seedResp.CipherSeedMnemonic,
	})
	if err != nil {
		return fmt.Errorf("failed to init wallet: %w", err)
	}

	// Close unlocker connection
	l.conn.Close()

	// Wait for macaroon to be created
	macPath := filepath.Join(l.dataDir, "data", "chain", "bitcoin", "regtest", "admin.macaroon")
	if err := waitForFile(macPath, 30*time.Second); err != nil {
		return fmt.Errorf("macaroon not created: %w", err)
	}

	// Reconnect with macaroon
	if err := l.connectWithMacaroon(); err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}

	return nil
}

func (l *LndProcess) connectWithMacaroon() error {
	tlsCertPath := filepath.Join(l.dataDir, "tls.cert")
	macPath := filepath.Join(l.dataDir, "data", "chain", "bitcoin", "regtest", "admin.macaroon")

	// Load TLS cert
	tlsCert, err := os.ReadFile(tlsCertPath)
	if err != nil {
		return fmt.Errorf("failed to read TLS cert: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(tlsCert) {
		return fmt.Errorf("failed to append cert")
	}

	tlsConfig := &tls.Config{
		RootCAs: certPool,
	}
	tlsCreds := credentials.NewTLS(tlsConfig)

	// Load macaroon
	macBytes, err := os.ReadFile(macPath)
	if err != nil {
		return fmt.Errorf("failed to read macaroon: %w", err)
	}

	mac := &macaroon.Macaroon{}
	if err := mac.UnmarshalBinary(macBytes); err != nil {
		return fmt.Errorf("failed to unmarshal macaroon: %w", err)
	}

	macCred, err := macaroons.NewMacaroonCredential(mac)
	if err != nil {
		return fmt.Errorf("failed to create macaroon cred: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithPerRPCCredentials(macCred),
		grpc.WithBlock(),
	}

	addr := fmt.Sprintf("127.0.0.1:%d", l.rpcPort)
	conn, err := grpc.DialContext(ctx, addr, opts...)
	if err != nil {
		return fmt.Errorf("failed to dial: %w", err)
	}

	l.conn = conn
	l.lightning = lnrpc.NewLightningClient(conn)
	l.subswap = submarineswaprpc.NewSubmarineSwapperClient(conn)
	return nil
}

func (l *LndProcess) waitForSync() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for sync")
		default:
		}

		info, err := l.lightning.GetInfo(ctx, &lnrpc.GetInfoRequest{})
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if info.SyncedToChain {
			return nil
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (l *LndProcess) stop() error {
	if l.conn != nil {
		// Try graceful stop first
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, _ = l.lightning.StopDaemon(ctx, &lnrpc.StopRequest{})
		l.conn.Close()
	}

	// Wait for process to exit
	done := make(chan error, 1)
	go func() {
		done <- l.cmd.Wait()
	}()

	select {
	case <-done:
		return nil
	case <-time.After(10 * time.Second):
		l.cmd.Process.Kill()
		return nil
	}
}

func createSwap(lnd *LndProcess) (*FixtureMetadata, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Client init - generates preimage, hash, keypair
	clientResp, err := lnd.subswap.SubSwapClientInit(ctx, &submarineswaprpc.SubSwapClientInitRequest{})
	if err != nil {
		return nil, fmt.Errorf("SubSwapClientInit failed: %w", err)
	}

	// Service init - registers swap, returns address
	serviceResp, err := lnd.subswap.SubSwapServiceInit(ctx, &submarineswaprpc.SubSwapServiceInitRequest{
		Hash:       clientResp.Hash,
		Pubkey:     clientResp.Pubkey,
		LockHeight: 288,
	})
	if err != nil {
		return nil, fmt.Errorf("SubSwapServiceInit failed: %w", err)
	}

	metadata := &FixtureMetadata{
		Version: "v0.18.5",
		State:   "initialized",
	}
	metadata.Swap.Hash = hex.EncodeToString(clientResp.Hash)
	metadata.Swap.Preimage = hex.EncodeToString(clientResp.Preimage)
	metadata.Swap.ClientPubkey = hex.EncodeToString(clientResp.Pubkey)
	metadata.Swap.SwapperPubkey = hex.EncodeToString(serviceResp.Pubkey)
	metadata.Swap.Address = serviceResp.Address
	metadata.Swap.LockHeight = serviceResp.LockHeight
	metadata.Swap.AmountSats = 0
	metadata.Wallet.Password = "testpassword12345678"

	return metadata, nil
}

func exportFixture(lndDataDir string, metadata *FixtureMetadata, outputDir string) error {
	// Create fixture directory
	fixtureDir := filepath.Join(outputDir, "v0.18.5_initialized")
	dbDir := filepath.Join(fixtureDir, "lnd", "data", "graph", "regtest")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create fixture dir: %w", err)
	}

	// Copy channel.db (renamed to .backup to avoid .gitignore *.db pattern)
	srcDB := filepath.Join(lndDataDir, "data", "graph", "regtest", "channel.db")
	dstDB := filepath.Join(dbDir, "channel.db.backup")
	if err := copyFile(srcDB, dstDB); err != nil {
		return fmt.Errorf("failed to copy channel.db: %w", err)
	}

	// Write metadata.json
	metadataPath := filepath.Join(fixtureDir, "metadata.json")
	metadataJSON, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}
	if err := os.WriteFile(metadataPath, metadataJSON, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

func copyFile(src, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return dstFile.Sync()
}

func waitForFile(path string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for %s", path)
		default:
		}

		if _, err := os.Stat(path); err == nil {
			return nil
		}

		time.Sleep(100 * time.Millisecond)
	}
}
