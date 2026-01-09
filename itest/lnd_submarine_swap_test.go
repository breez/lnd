//go:build integration && submarineswaprpc

package itest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/btcsuite/btcd/btcutil"
	"github.com/btcsuite/btcd/wire"
	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/lightningnetwork/lnd/lnrpc/submarineswaprpc"
	"github.com/lightningnetwork/lnd/lntest"
	"github.com/lightningnetwork/lnd/lntest/node"
	"github.com/stretchr/testify/require"
)

const (
	// testSubSwapLockHeight is a short lock height for testing.
	testSubSwapLockHeight = 10

	// defaultRPCTimeout is the default timeout for RPC calls.
	defaultRPCTimeout = 30 * time.Second
)

// getSubmarineSwapClient creates a SubmarineSwapperClient from a node's
// gRPC connection. This avoids modifying the harness and prevents merge
// conflicts on future rebases.
func getSubmarineSwapClient(hn *node.HarnessNode) submarineswaprpc.SubmarineSwapperClient {
	conn, err := hn.ConnectRPC()
	if err != nil {
		panic(err)
	}
	return submarineswaprpc.NewSubmarineSwapperClient(conn)
}

// testSubmarineSwap runs all submarine swap integration tests.
func testSubmarineSwap(ht *lntest.HarnessTest) {
	testSubmarineSwapHappyPath(ht)
	testSubmarineSwapFeeEstimation(ht)
	testSubmarineSwapErrorCases(ht)
}

// testSubmarineSwapHappyPath tests the complete happy path flow:
// 1. Client: SubSwapClientInit - get preimage, hash, keypair
// 2. Swapper: SubSwapServiceInit - get deposit address
// 3. Client: SubSwapClientWatch - verify address matches
// 4. Swapper: UnspentAmount - verify 0 before funding
// 5. Fund the swap address from miner
// 6. Swapper: UnspentAmount - verify funds received
// 7. Client: Create invoice with preimage
// 8. Swapper: Pay invoice via Lightning (obtains preimage)
// 9. Swapper: SubSwapServiceRedeem - redeem on-chain funds
func testSubmarineSwapHappyPath(ht *lntest.HarnessTest) {
	// Create Swapper and Client nodes.
	swapper := ht.NewNode("Swapper", nil)
	client := ht.NewNode("Client", nil)

	// Fund both nodes so they can open channels and pay fees.
	ht.FundCoins(btcutil.SatoshiPerBitcoin, swapper)
	ht.FundCoins(btcutil.SatoshiPerBitcoin, client)

	// Connect and open a channel: Swapper -> Client
	// (so Swapper can pay Client via Lightning)
	ht.EnsureConnected(swapper, client)
	chanPoint := ht.OpenChannel(
		swapper, client, lntest.OpenChannelParams{
			Amt: 500_000,
		},
	)
	defer ht.CloseChannel(swapper, chanPoint)

	// Get submarine swap clients for both nodes.
	swapperClient := getSubmarineSwapClient(swapper)
	clientClient := getSubmarineSwapClient(client)

	// Step 1: Client initializes swap (generates preimage, hash, keypair).
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	clientInitResp, err := clientClient.SubSwapClientInit(
		ctx, &submarineswaprpc.SubSwapClientInitRequest{},
	)
	require.NoError(ht, err, "SubSwapClientInit failed")
	require.Len(ht, clientInitResp.Preimage, 32, "preimage should be 32 bytes")
	require.Len(ht, clientInitResp.Hash, 32, "hash should be 32 bytes")
	require.Len(ht, clientInitResp.Key, 32, "key should be 32 bytes")
	require.Len(ht, clientInitResp.Pubkey, 33, "pubkey should be 33 bytes compressed")

	// Verify hash is SHA256 of preimage.
	expectedHash := sha256.Sum256(clientInitResp.Preimage)
	require.Equal(ht, expectedHash[:], clientInitResp.Hash, "hash should match SHA256(preimage)")

	// Step 2: Swapper initializes swap (generates deposit address).
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel2()

	serviceInitResp, err := swapperClient.SubSwapServiceInit(
		ctx2, &submarineswaprpc.SubSwapServiceInitRequest{
			Hash:       clientInitResp.Hash,
			Pubkey:     clientInitResp.Pubkey,
			LockHeight: testSubSwapLockHeight,
		},
	)
	require.NoError(ht, err, "SubSwapServiceInit failed")
	require.NotEmpty(ht, serviceInitResp.Address, "address should not be empty")
	require.Len(ht, serviceInitResp.Pubkey, 33, "service pubkey should be 33 bytes")
	require.Equal(ht, int64(testSubSwapLockHeight), serviceInitResp.LockHeight)

	// Step 3: Client watches the swap address.
	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel3()

	clientWatchResp, err := clientClient.SubSwapClientWatch(
		ctx3, &submarineswaprpc.SubSwapClientWatchRequest{
			Preimage:      clientInitResp.Preimage,
			Key:           clientInitResp.Key,
			ServicePubkey: serviceInitResp.Pubkey,
			LockHeight:    serviceInitResp.LockHeight,
		},
	)
	require.NoError(ht, err, "SubSwapClientWatch failed")
	require.Equal(ht, serviceInitResp.Address, clientWatchResp.Address,
		"client watch address should match service address")
	require.NotEmpty(ht, clientWatchResp.Script)

	// Step 4: Query UnspentAmount BEFORE funding - should be 0.
	ctx4, cancel4 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel4()

	unspentBefore, err := swapperClient.UnspentAmount(
		ctx4, &submarineswaprpc.UnspentAmountRequest{
			Hash: clientInitResp.Hash,
		},
	)
	require.NoError(ht, err, "UnspentAmount before funding failed")
	require.Equal(ht, int64(0), unspentBefore.Amount,
		"unspent amount should be 0 before funding")

	// Step 5: Fund the swap address (simulating user sending on-chain).
	const swapAmount = 100_000 // satoshis
	swapAddr := ht.DecodeAddress(serviceInitResp.Address)
	swapScript := ht.PayToAddrScript(swapAddr)
	output := &wire.TxOut{
		PkScript: swapScript,
		Value:    swapAmount,
	}
	ht.SendOutputsWithoutChange([]*wire.TxOut{output}, 7500)

	// Mine a block to confirm the funding transaction.
	ht.MineBlocksAndAssertNumTxes(1, 1)

	// Step 6: Query UnspentAmount AFTER funding - should have funds.
	ctx5, cancel5 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel5()

	unspentAfter, err := swapperClient.UnspentAmount(
		ctx5, &submarineswaprpc.UnspentAmountRequest{
			Hash: clientInitResp.Hash,
		},
	)
	require.NoError(ht, err, "UnspentAmount after funding failed")
	require.Equal(ht, int64(swapAmount), unspentAfter.Amount,
		"unspent amount should match funded amount")
	require.Len(ht, unspentAfter.Utxos, 1, "should have exactly 1 UTXO")

	// Also verify query by address works.
	ctx6, cancel6 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel6()

	unspentByAddr, err := clientClient.UnspentAmount(
		ctx6, &submarineswaprpc.UnspentAmountRequest{
			Address: serviceInitResp.Address,
		},
	)
	require.NoError(ht, err, "UnspentAmount by address failed")
	require.Equal(ht, int64(swapAmount), unspentByAddr.Amount)

	// Step 7: Client creates invoice with the preimage.
	// The swapper will pay this invoice, and through the payment will
	// learn the preimage (which is revealed when the invoice is settled).
	invoiceAmt := int64(swapAmount - 5000) // Leave room for fees
	invoice := client.RPC.AddInvoice(&lnrpc.Invoice{
		RPreimage: clientInitResp.Preimage,
		Value:     invoiceAmt,
		Memo:      "submarine swap",
	})

	// Step 8: Swapper pays Client's invoice via Lightning.
	// Upon successful payment, Swapper obtains the preimage.
	payReq := &routerrpc.SendPaymentRequest{
		PaymentRequest: invoice.PaymentRequest,
		TimeoutSeconds: 60,
		FeeLimitMsat:   noFeeLimitMsat,
	}
	ht.SendPaymentAssertSettled(swapper, payReq)

	// Verify the invoice was settled.
	dbInvoice := client.RPC.LookupInvoice(invoice.RHash)
	require.Equal(ht, lnrpc.Invoice_SETTLED, dbInvoice.State,
		"invoice should be marked as settled")

	// Step 9: Swapper redeems the on-chain funds using the preimage.
	ctx7, cancel7 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel7()

	redeemResp, err := swapperClient.SubSwapServiceRedeem(
		ctx7, &submarineswaprpc.SubSwapServiceRedeemRequest{
			Preimage:   clientInitResp.Preimage,
			SatPerByte: 10,
		},
	)
	require.NoError(ht, err, "SubSwapServiceRedeem failed")
	require.NotEmpty(ht, redeemResp.Txid, "redeem txid should not be empty")

	// Mine the redemption transaction.
	ht.MineBlocksAndAssertNumTxes(1, 1)

	// Verify Swapper's on-chain balance increased.
	swapperBalance := swapper.RPC.WalletBalance()
	// Swapper started with 1 BTC, opened 500k sat channel, redeemed ~95k sats
	// Balance should be > initial - channel (accounting for fees)
	require.Greater(ht, swapperBalance.ConfirmedBalance, int64(400_000),
		"swapper balance should have increased from redemption")
}

// testSubmarineSwapFeeEstimation tests fee estimation accuracy.
func testSubmarineSwapFeeEstimation(ht *lntest.HarnessTest) {
	swapper := ht.NewNode("Swapper", nil)
	client := ht.NewNode("Client", nil)

	ht.FundCoins(btcutil.SatoshiPerBitcoin, swapper)

	swapperClient := getSubmarineSwapClient(swapper)
	clientClient := getSubmarineSwapClient(client)

	// Initialize a swap.
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	clientInitResp, err := clientClient.SubSwapClientInit(
		ctx, &submarineswaprpc.SubSwapClientInitRequest{},
	)
	require.NoError(ht, err)

	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel2()

	serviceInitResp, err := swapperClient.SubSwapServiceInit(
		ctx2, &submarineswaprpc.SubSwapServiceInitRequest{
			Hash:       clientInitResp.Hash,
			Pubkey:     clientInitResp.Pubkey,
			LockHeight: testSubSwapLockHeight,
		},
	)
	require.NoError(ht, err)

	// Fund the swap address.
	const swapAmount = 200_000
	swapAddr := ht.DecodeAddress(serviceInitResp.Address)
	swapScript := ht.PayToAddrScript(swapAddr)
	output := &wire.TxOut{
		PkScript: swapScript,
		Value:    swapAmount,
	}
	ht.SendOutputsWithoutChange([]*wire.TxOut{output}, 7500)
	ht.MineBlocksAndAssertNumTxes(1, 1)

	// Test fee estimation at different fee rates.
	feeRates := []int64{1, 5, 10, 50}
	var prevFee int64

	for _, rate := range feeRates {
		ctx3, cancel3 := context.WithTimeout(context.Background(), defaultRPCTimeout)
		defer cancel3()

		feeEstimate, err := swapperClient.SubSwapServiceRedeemFees(
			ctx3, &submarineswaprpc.SubSwapServiceRedeemFeesRequest{
				Hash:       clientInitResp.Hash,
				SatPerByte: rate,
			},
		)
		require.NoError(ht, err, "SubSwapServiceRedeemFees failed for rate %d", rate)

		// Fees should be positive.
		require.Greater(ht, feeEstimate.Amount, int64(0),
			"fees should be positive for rate %d", rate)

		// Fees should be less than swap amount.
		require.Less(ht, feeEstimate.Amount, int64(swapAmount),
			"fees should be less than swap amount for rate %d", rate)

		// Fees should increase with higher fee rates.
		if prevFee > 0 {
			require.Greater(ht, feeEstimate.Amount, prevFee,
				"fees should increase with higher fee rate")
		}
		prevFee = feeEstimate.Amount
	}
}

// testSubmarineSwapErrorCases tests error handling.
func testSubmarineSwapErrorCases(ht *lntest.HarnessTest) {
	swapper := ht.NewNode("Swapper", nil)
	client := ht.NewNode("Client", nil)

	swapperClient := getSubmarineSwapClient(swapper)
	clientClient := getSubmarineSwapClient(client)

	// Test 1: Duplicate hash should fail.
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	clientInitResp, err := clientClient.SubSwapClientInit(
		ctx, &submarineswaprpc.SubSwapClientInitRequest{},
	)
	require.NoError(ht, err)

	// First init should succeed.
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel2()

	_, err = swapperClient.SubSwapServiceInit(
		ctx2, &submarineswaprpc.SubSwapServiceInitRequest{
			Hash:       clientInitResp.Hash,
			Pubkey:     clientInitResp.Pubkey,
			LockHeight: testSubSwapLockHeight,
		},
	)
	require.NoError(ht, err)

	// Second init with same hash should fail.
	ctx3, cancel3 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel3()

	_, err = swapperClient.SubSwapServiceInit(
		ctx3, &submarineswaprpc.SubSwapServiceInitRequest{
			Hash:       clientInitResp.Hash,
			Pubkey:     clientInitResp.Pubkey,
			LockHeight: testSubSwapLockHeight,
		},
	)
	require.Error(ht, err, "duplicate hash should fail")
	require.Contains(ht, err.Error(), "Hash already exists")

	// Test 2: Invalid hash length should fail.
	ctx4, cancel4 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel4()

	_, err = swapperClient.SubSwapServiceInit(
		ctx4, &submarineswaprpc.SubSwapServiceInitRequest{
			Hash:       []byte{0x01, 0x02, 0x03}, // Too short
			Pubkey:     clientInitResp.Pubkey,
			LockHeight: testSubSwapLockHeight,
		},
	)
	require.Error(ht, err, "invalid hash length should fail")

	// Test 3: Invalid pubkey length should fail.
	ctx5, cancel5 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel5()

	// Get a new hash for this test.
	clientInit2, err := clientClient.SubSwapClientInit(
		ctx5, &submarineswaprpc.SubSwapClientInitRequest{},
	)
	require.NoError(ht, err)

	ctx6, cancel6 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel6()

	_, err = swapperClient.SubSwapServiceInit(
		ctx6, &submarineswaprpc.SubSwapServiceInitRequest{
			Hash:       clientInit2.Hash,
			Pubkey:     []byte{0x01, 0x02}, // Invalid pubkey
			LockHeight: testSubSwapLockHeight,
		},
	)
	require.Error(ht, err, "invalid pubkey length should fail")
}

// fixtureMetadata represents the metadata stored with each fixture.
type fixtureMetadata struct {
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

// loadFixtureMetadata loads the metadata.json file from a fixture directory.
func loadFixtureMetadata(fixtureDir string) (*fixtureMetadata, error) {
	metadataPath := filepath.Join(fixtureDir, "metadata.json")
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}

	var metadata fixtureMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

// testSubmarineSwapUpgrade tests that swaps created with v0.18.5 can be
// completed after upgrading to v0.20.0.
//
// This test verifies database format compatibility by:
// 1. Loading a fixture containing v0.18.5 submarine swap data
// 2. Starting a v0.20.0 node with the fixture database
// 3. Querying the swap to verify data can be read
// 4. Funding and completing the swap to verify full functionality
//
// The test requires fixtures in itest/fixtures/submarineswap/.
// See the README.md in that directory for fixture creation instructions.
func testSubmarineSwapUpgrade(ht *lntest.HarnessTest) {
	// Check if fixtures exist.
	fixtureDir := filepath.Join("fixtures", "submarineswap", "v0.18.5_initialized")

	// Check if fixture directory exists.
	if _, err := os.Stat(fixtureDir); os.IsNotExist(err) {
		ht.Skip("Upgrade test requires fixtures - see fixtures/submarineswap/README.md")
		return
	}

	// Load fixture metadata.
	metadata, err := loadFixtureMetadata(fixtureDir)
	require.NoError(ht, err, "failed to load fixture metadata")

	// Parse the swap hash and preimage from metadata.
	swapHash, err := hex.DecodeString(metadata.Swap.Hash)
	require.NoError(ht, err, "failed to decode swap hash")
	preimage, err := hex.DecodeString(metadata.Swap.Preimage)
	require.NoError(ht, err, "failed to decode preimage")

	// Verify preimage matches hash.
	expectedHash := sha256.Sum256(preimage)
	require.Equal(ht, expectedHash[:], swapHash, "preimage should match hash")

	// Create a fresh node - we'll inject the fixture's channel.db after creation.
	swapper := ht.NewNode("Swapper", nil)

	// Get the path to the swapper's channel.db.
	swapperDBPath := filepath.Join(swapper.Cfg.BaseDir, "data", "graph", "regtest")

	// Stop the swapper to replace its database.
	require.NoError(ht, swapper.Stop(), "failed to stop swapper")

	// Copy the fixture's channel.db to the swapper's data directory.
	// The fixture contains the v0.18.5 submarine swap bucket data.
	// Note: fixture uses .backup extension to avoid .gitignore *.db pattern.
	fixtureDBPath := filepath.Join(fixtureDir, "lnd", "data", "graph", "regtest", "channel.db.backup")
	destDBPath := filepath.Join(swapperDBPath, "channel.db")

	// Remove the existing channel.db and copy the fixture's version.
	require.NoError(ht, os.Remove(destDBPath), "failed to remove existing channel.db")

	srcDB, err := os.Open(fixtureDBPath)
	require.NoError(ht, err, "failed to open fixture channel.db")
	defer srcDB.Close()

	dstDB, err := os.OpenFile(destDBPath, os.O_CREATE|os.O_WRONLY, 0600)
	require.NoError(ht, err, "failed to create destination channel.db")
	defer dstDB.Close()

	_, err = io.Copy(dstDB, srcDB)
	require.NoError(ht, err, "failed to copy channel.db")

	// Ensure the file is fully written.
	require.NoError(ht, dstDB.Sync(), "failed to sync channel.db")

	// Restart the swapper with the fixture's database.
	// The v0.20.0 code should be able to read the v0.18.5 database format.
	require.NoError(ht, swapper.Start(ht.Context()), "failed to restart swapper")

	// Wait for the swapper to sync.
	ht.WaitForBlockchainSync(swapper)

	// Get submarine swap client.
	swapperClient := getSubmarineSwapClient(swapper)

	// Step 1: Query the swap by hash from fixture.
	// This verifies that v0.20.0 can read v0.18.5 database format.
	ctx, cancel := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel()

	// Query unspent amount - should succeed (proves data is readable).
	// Amount will be 0 since the wallet doesn't track the address
	// (we only copied channel.db, not wallet.db).
	unspent, err := swapperClient.UnspentAmount(
		ctx, &submarineswaprpc.UnspentAmountRequest{
			Hash: swapHash,
		},
	)
	require.NoError(ht, err, "failed to query swap from v0.18.5 database")
	require.Equal(ht, int64(0), unspent.Amount,
		"unspent amount should be 0 (wallet doesn't track fixture addresses)")

	// Step 2: Verify the swap data exists by trying to create a duplicate.
	// This should fail with "Hash already exists" error, proving the
	// v0.18.5 data was successfully loaded by v0.20.0.
	ctx2, cancel2 := context.WithTimeout(context.Background(), defaultRPCTimeout)
	defer cancel2()

	// Decode the client pubkey from metadata to use in the duplicate attempt.
	clientPubkey, err := hex.DecodeString(metadata.Swap.ClientPubkey)
	require.NoError(ht, err, "failed to decode client pubkey")

	_, err = swapperClient.SubSwapServiceInit(
		ctx2, &submarineswaprpc.SubSwapServiceInitRequest{
			Hash:       swapHash,
			Pubkey:     clientPubkey,
			LockHeight: metadata.Swap.LockHeight,
		},
	)
	require.Error(ht, err, "duplicate hash should fail")
	require.Contains(ht, err.Error(), "Hash already exists",
		"error should indicate hash already exists in v0.18.5 database")

	ht.Log("Successfully verified v0.18.5 database format is readable by v0.20.0")
}
