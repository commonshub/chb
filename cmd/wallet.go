package cmd

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/decred/dcrd/dcrec/secp256k1/v4"
	"golang.org/x/crypto/sha3"
)

// keccak256 computes the Keccak-256 hash of the input (same as Solidity's keccak256).
func keccak256(data []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil)
}

// Default CitizenWallet CardManager on Celo
const (
	defaultCardManagerAddress = "0xBA861e2DABd8316cf11Ae7CdA101d110CF581f28"
	defaultInstanceID         = "cw-discord-1"
	defaultCeloRPC            = "https://forno.celo.org"
	defaultGnosisRPC          = "https://rpc.gnosischain.com"
	safeProxyFactoryAddress   = "0x4e1dcf7ad4e460cfd30791ccc4f9c8a4f820ec67"
	safeSingletonAddress      = "0x41675c099f32341bf84bfc5382af534df5c7461a"
	fallbackHandlerAddress    = "0xfd0732dc9e303f09fcef3a7388ad10a83459ec99"
	walletCacheVersion        = 3
)

var (
	walletPositiveCacheTTL = 30 * 24 * time.Hour
	walletNegativeCacheTTL = 24 * time.Hour
)

type walletResolutionCache struct {
	Version   int                              `json:"version"`
	UpdatedAt string                           `json:"updatedAt"`
	Entries   map[string]walletResolutionEntry `json:"entries"`
}

type walletResolutionEntry struct {
	Address   *string `json:"address,omitempty"`
	CheckedAt string  `json:"checkedAt"`
}

type walletResolverScope struct {
	Resolver           string
	TokenKey           string
	RPCURL             string
	CardManagerAddress string
	InstanceID         string
	OwnersKey          string
}

// resolveWalletAddress resolves a Discord user ID to a wallet address
// using the CitizenWallet CardManager contract.
func resolveWalletAddress(discordUserID string, settings *Settings) (string, error) {
	manager := contributionTokenWalletManager(settings)
	if manager == "opencollective" {
		return resolveOpenCollectiveAddress(discordUserID)
	}

	rpcURL := defaultCeloRPC
	cardManagerAddress := defaultCardManagerAddress
	instanceID := defaultInstanceID

	if settings != nil && settings.ContributionToken != nil {
		if settings.ContributionToken.CardManagerAddress != "" {
			cardManagerAddress = settings.ContributionToken.CardManagerAddress
		}
		if settings.ContributionToken.CardManagerInstanceID != "" {
			instanceID = settings.ContributionToken.CardManagerInstanceID
		}
		if settings.ContributionToken.RpcUrl != "" {
			rpcURL = settings.ContributionToken.RpcUrl
		} else if settings.ContributionToken.CardManagerAddress != "" {
			rpcURL = defaultRPCForChain(settings.ContributionToken.Chain)
		}
	}

	return resolveCardManagerAddress(discordUserID, cardManagerAddress, instanceID, rpcURL)
}

func contributionWalletResolverScope(settings *Settings) walletResolverScope {
	scope := walletResolverScope{
		Resolver:           contributionTokenWalletManager(settings),
		TokenKey:           "unknown-token",
		RPCURL:             defaultCeloRPC,
		CardManagerAddress: strings.ToLower(defaultCardManagerAddress),
		InstanceID:         defaultInstanceID,
	}

	if settings != nil && settings.ContributionToken != nil {
		ct := settings.ContributionToken
		if ct.RpcUrl != "" {
			scope.RPCURL = ct.RpcUrl
		}
		switch {
		case ct.Address != "" && ct.ChainID != 0:
			scope.TokenKey = fmt.Sprintf("%s:%d:%s", strings.ToLower(ct.Symbol), ct.ChainID, strings.ToLower(ct.Address))
		case ct.Address != "":
			scope.TokenKey = strings.ToLower(ct.Address)
		case ct.Symbol != "":
			scope.TokenKey = strings.ToLower(ct.Symbol)
		}
	}

	if scope.Resolver == "opencollective" {
		scope.CardManagerAddress = ""
		scope.InstanceID = ""
		scope.RPCURL = defaultRPCForChain("")
		if owners, err := getOpenCollectiveSafeOwners(); err == nil && len(owners) > 0 {
			scope.OwnersKey = strings.Join(owners, ",")
		}
		return scope
	}

	if settings != nil && settings.ContributionToken != nil {
		ct := settings.ContributionToken
		if ct.CardManagerAddress != "" {
			scope.CardManagerAddress = strings.ToLower(ct.CardManagerAddress)
		}
		if ct.CardManagerInstanceID != "" {
			scope.InstanceID = ct.CardManagerInstanceID
		}
	}

	return scope
}

func (s walletResolverScope) cacheKey(discordUserID string) string {
	return strings.Join([]string{
		s.Resolver,
		s.TokenKey,
		s.OwnersKey,
		s.CardManagerAddress,
		s.InstanceID,
		discordUserID,
	}, "|")
}

func contributionTokenWalletManager(settings *Settings) string {
	if settings != nil && settings.ContributionToken != nil {
		if settings.ContributionToken.WalletManager != "" {
			return strings.ToLower(settings.ContributionToken.WalletManager)
		}
		if settings.ContributionToken.CardManagerAddress != "" || settings.ContributionToken.CardManagerInstanceID != "" {
			return "citizenwallet"
		}
		if strings.EqualFold(settings.ContributionToken.Chain, "celo") {
			return "citizenwallet"
		}
	}
	return "opencollective"
}

func defaultRPCForChain(chain string) string {
	switch strings.ToLower(chain) {
	case "celo":
		return defaultCeloRPC
	case "gnosis":
		return defaultGnosisRPC
	default:
		return defaultCeloRPC
	}
}

// resolveCardManagerAddress calls the CardManager contract's getCardAddress function.
// Equivalent to: contract.getCardAddress(keccak256(instanceId), keccak256(userId))
func resolveCardManagerAddress(userID, cardManagerAddr, instanceID, rpcURL string) (string, error) {
	hashedInstanceID := keccak256([]byte(instanceID))
	hashedUserID := keccak256([]byte(userID))

	// ABI-encode the call: getCardAddress(bytes32, bytes32)
	// Function selector: keccak256("getCardAddress(bytes32,bytes32)")[:4]
	selector := keccak256([]byte("getCardAddress(bytes32,bytes32)"))[:4]

	// Construct calldata: selector + padded hashedInstanceID + padded hashedUserID
	calldata := make([]byte, 0, 4+32+32)
	calldata = append(calldata, selector...)
	calldata = append(calldata, padTo32(hashedInstanceID)...)
	calldata = append(calldata, padTo32(hashedUserID)...)

	// JSON-RPC eth_call
	payload := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_call",
		"params": []interface{}{
			map[string]string{
				"to":   cardManagerAddr,
				"data": "0x" + hex.EncodeToString(calldata),
			},
			"latest",
		},
	}

	body, _ := json.Marshal(payload)
	resp, err := http.Post(rpcURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("RPC request failed: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Result string `json:"result"`
		Error  *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("RPC decode failed: %w", err)
	}
	if result.Error != nil {
		return "", fmt.Errorf("RPC error: %s", result.Error.Message)
	}

	// Result is a hex-encoded 32-byte value (padded address)
	// Take the last 20 bytes as the address
	resultHex := strings.TrimPrefix(result.Result, "0x")
	if len(resultHex) < 40 {
		return "", fmt.Errorf("unexpected result length: %s", result.Result)
	}

	addr := "0x" + resultHex[len(resultHex)-40:]

	// Check for zero address (user has no wallet)
	if addr == "0x0000000000000000000000000000000000000000" {
		return "", nil
	}

	return strings.ToLower(addr), nil
}

func padTo32(b []byte) []byte {
	if len(b) >= 32 {
		return b[:32]
	}
	padded := make([]byte, 32)
	copy(padded[32-len(b):], b)
	return padded
}

// resolveDiscordToWalletMap resolves all Discord user IDs to wallet addresses
// using the CardManager contract. Returns a map of discordID → walletAddress.
func resolveDiscordToWalletMap(discordUserIDs []string, settings *Settings, cache *walletResolutionCache) (map[string]string, bool) {
	result := map[string]string{}
	if cache == nil {
		cache = &walletResolutionCache{Version: walletCacheVersion, Entries: map[string]walletResolutionEntry{}}
	}
	if cache.Entries == nil {
		cache.Entries = map[string]walletResolutionEntry{}
	}

	now := time.Now().UTC()
	seen := map[string]bool{}
	dirty := false
	scope := contributionWalletResolverScope(settings)

	for _, id := range discordUserIDs {
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		cacheKey := scope.cacheKey(id)

		if entry, ok := cache.Entries[cacheKey]; ok {
			if checkedAt, err := time.Parse(time.RFC3339, entry.CheckedAt); err == nil {
				ttl := walletNegativeCacheTTL
				if entry.Address != nil && *entry.Address != "" {
					ttl = walletPositiveCacheTTL
				}
				if now.Sub(checkedAt) < ttl {
					if entry.Address != nil && *entry.Address != "" {
						result[id] = *entry.Address
					}
					continue
				}
			}
		}

		addr, err := resolveWalletAddress(id, settings)
		if err != nil {
			if entry, ok := cache.Entries[cacheKey]; ok && entry.Address != nil && *entry.Address != "" {
				result[id] = *entry.Address
			}
			continue
		}

		entry := walletResolutionEntry{CheckedAt: now.Format(time.RFC3339)}
		if addr != "" {
			addr = strings.ToLower(addr)
			entry.Address = &addr
			result[id] = addr
		}
		cache.Entries[cacheKey] = entry
		dirty = true
	}

	if dirty {
		cache.Version = walletCacheVersion
		cache.UpdatedAt = now.Format(time.RFC3339)
	}

	return result, dirty
}

func resolveOpenCollectiveAddress(discordUserID string) (string, error) {
	owners, err := getOpenCollectiveSafeOwners()
	if err != nil {
		return "", err
	}
	return predictOpenCollectiveSafeAddress(discordUserID, owners)
}

func getOpenCollectiveSafeOwners() ([]string, error) {
	primary := strings.ToLower(strings.TrimSpace(os.Getenv("SAFE_OWNER_ADDRESS")))
	backup := strings.ToLower(strings.TrimSpace(os.Getenv("SAFE_BACKUP_OWNER_ADDRESS")))
	if primary != "" && backup != "" {
		return normalizeOwnerAddresses([]string{primary, backup})
	}

	privateKey := strings.TrimSpace(os.Getenv("PRIVATE_KEY"))
	if privateKey == "" {
		return nil, fmt.Errorf("PRIVATE_KEY or SAFE_OWNER_ADDRESS not set")
	}
	backupKey := strings.TrimSpace(os.Getenv("BACKUP_PRIVATE_KEY"))
	if backupKey == "" {
		return nil, fmt.Errorf("BACKUP_PRIVATE_KEY or SAFE_BACKUP_OWNER_ADDRESS not set")
	}

	primaryAddr, err := privateKeyToAddress(privateKey)
	if err != nil {
		return nil, err
	}
	backupAddr, err := privateKeyToAddress(backupKey)
	if err != nil {
		return nil, err
	}
	return normalizeOwnerAddresses([]string{primaryAddr, backupAddr})
}

func normalizeOwnerAddresses(owners []string) ([]string, error) {
	if len(owners) == 0 {
		return nil, fmt.Errorf("no safe owners configured")
	}
	normalized := make([]string, 0, len(owners))
	for _, owner := range owners {
		addr := strings.ToLower(strings.TrimSpace(owner))
		if !isHexAddress(addr) {
			return nil, fmt.Errorf("invalid owner address: %s", owner)
		}
		normalized = append(normalized, addr)
	}
	sort.Strings(normalized)
	return normalized, nil
}

func privateKeyToAddress(privateKey string) (string, error) {
	keyHex := strings.TrimPrefix(strings.TrimSpace(privateKey), "0x")
	keyBytes, err := hex.DecodeString(keyHex)
	if err != nil {
		return "", fmt.Errorf("invalid private key: %w", err)
	}
	privKey := secp256k1.PrivKeyFromBytes(keyBytes)
	pubKey := privKey.PubKey().SerializeUncompressed()
	hash := keccak256(pubKey[1:])
	return "0x" + hex.EncodeToString(hash[12:]), nil
}

func predictOpenCollectiveSafeAddress(discordUserID string, owners []string) (string, error) {
	owners, err := normalizeOwnerAddresses(owners)
	if err != nil {
		return "", err
	}

	initializer, err := encodeSafeSetup(owners, 1)
	if err != nil {
		return "", err
	}
	initializerHash := keccak256(initializer)
	saltNonce := keccak256([]byte(discordUserID))
	saltInput := make([]byte, 0, 64)
	saltInput = append(saltInput, initializerHash...)
	saltInput = append(saltInput, saltNonce...)
	salt := keccak256(saltInput)

	bytecode, err := safeDeploymentBytecode()
	if err != nil {
		return "", err
	}
	bytecodeHash := keccak256(bytecode)

	factoryBytes, _ := hex.DecodeString(strings.TrimPrefix(safeProxyFactoryAddress, "0x"))
	create2Input := make([]byte, 0, 1+20+32+32)
	create2Input = append(create2Input, 0xff)
	create2Input = append(create2Input, factoryBytes...)
	create2Input = append(create2Input, salt...)
	create2Input = append(create2Input, bytecodeHash...)

	hash := keccak256(create2Input)
	return "0x" + hex.EncodeToString(hash[12:]), nil
}

func encodeSafeSetup(owners []string, threshold int) ([]byte, error) {
	if threshold <= 0 {
		return nil, fmt.Errorf("invalid threshold: %d", threshold)
	}

	selector, err := hex.DecodeString("b63e800d")
	if err != nil {
		return nil, err
	}

	ownersEncoded, err := encodeAddressArray(owners)
	if err != nil {
		return nil, err
	}
	dataEncoded := encodeBytes(nil)

	headSize := 8 * 32
	ownersOffset := encodeUint256(big.NewInt(int64(headSize)))
	dataOffset := encodeUint256(big.NewInt(int64(headSize + len(ownersEncoded))))

	var payload []byte
	payload = append(payload, ownersOffset...)
	payload = append(payload, encodeUint256(big.NewInt(int64(threshold)))...)
	payload = append(payload, encodeAddress(commonHexToAddress(""))...)
	payload = append(payload, dataOffset...)
	payload = append(payload, encodeAddress(commonHexToAddress(fallbackHandlerAddress))...)
	payload = append(payload, encodeAddress(commonHexToAddress(""))...)
	payload = append(payload, encodeUint256(big.NewInt(0))...)
	payload = append(payload, encodeAddress(commonHexToAddress(""))...)
	payload = append(payload, ownersEncoded...)
	payload = append(payload, dataEncoded...)

	return append(selector, payload...), nil
}

func encodeAddressArray(owners []string) ([]byte, error) {
	var out []byte
	out = append(out, encodeUint256(big.NewInt(int64(len(owners))))...)
	for _, owner := range owners {
		if !isHexAddress(owner) {
			return nil, fmt.Errorf("invalid owner address: %s", owner)
		}
		out = append(out, encodeAddress(commonHexToAddress(owner))...)
	}
	return out, nil
}

func encodeBytes(data []byte) []byte {
	length := encodeUint256(big.NewInt(int64(len(data))))
	padded := rightPadTo32(data)
	return append(length, padded...)
}

func encodeAddress(addr []byte) []byte {
	out := make([]byte, 32)
	copy(out[12:], addr)
	return out
}

func encodeUint256(n *big.Int) []byte {
	out := make([]byte, 32)
	if n == nil {
		return out
	}
	b := n.Bytes()
	copy(out[32-len(b):], b)
	return out
}

func rightPadTo32(data []byte) []byte {
	if len(data)%32 == 0 {
		return data
	}
	out := make([]byte, len(data)+(32-len(data)%32))
	copy(out, data)
	return out
}

func safeDeploymentBytecode() ([]byte, error) {
	prefix, err := hex.DecodeString(strings.TrimPrefix("0x608060405234801561001057600080fd5b506040516101e63803806101e68339818101604052602081101561003357600080fd5b8101908080519060200190929190505050600073ffffffffffffffffffffffffffffffffffffffff168173ffffffffffffffffffffffffffffffffffffffff1614156100ca576040517f08c379a00000000000000000000000000000000000000000000000000000000081526004018080602001828103825260248152602001806101c26024913960400191505060405180910390fd5b806000806101000a81548173ffffffffffffffffffffffffffffffffffffffff021916908373ffffffffffffffffffffffffffffffffffffffff1602179055505060ab806101196000396000f3fe", "0x"))
	if err != nil {
		return nil, err
	}
	singleton := encodeUint256(new(big.Int).SetBytes(commonHexToAddress(safeSingletonAddress)))
	return append(prefix, singleton...), nil
}

func commonHexToAddress(addr string) []byte {
	if addr == "" {
		return make([]byte, 20)
	}
	addr = strings.TrimPrefix(strings.ToLower(addr), "0x")
	raw, _ := hex.DecodeString(addr)
	if len(raw) >= 20 {
		return raw[len(raw)-20:]
	}
	out := make([]byte, 20)
	copy(out[20-len(raw):], raw)
	return out
}

func isHexAddress(addr string) bool {
	addr = strings.TrimPrefix(strings.ToLower(strings.TrimSpace(addr)), "0x")
	if len(addr) != 40 {
		return false
	}
	_, err := hex.DecodeString(addr)
	return err == nil
}

func walletResolutionCachePath(dataDir string) string {
	return filepath.Join(dataDir, "latest", stewardsDirName, "cache", "discord-wallets.json")
}

func loadWalletResolutionCache(dataDir string) *walletResolutionCache {
	cache := &walletResolutionCache{
		Version: walletCacheVersion,
		Entries: map[string]walletResolutionEntry{},
	}

	data, err := os.ReadFile(walletResolutionCachePath(dataDir))
	if err != nil {
		return cache
	}
	if json.Unmarshal(data, cache) != nil || cache.Entries == nil {
		cache.Entries = map[string]walletResolutionEntry{}
	}
	return cache
}

func saveWalletResolutionCache(dataDir string, cache *walletResolutionCache) error {
	if cache == nil {
		return nil
	}
	if cache.Entries == nil {
		cache.Entries = map[string]walletResolutionEntry{}
	}
	cache.Version = walletCacheVersion
	if cache.UpdatedAt == "" {
		cache.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}

	path := walletResolutionCachePath(dataDir)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cache, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}
