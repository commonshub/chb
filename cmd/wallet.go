package cmd

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

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
)

// resolveWalletAddress resolves a Discord user ID to a wallet address
// using the CitizenWallet CardManager contract.
func resolveWalletAddress(discordUserID string, settings *Settings) (string, error) {
	rpcURL := defaultCeloRPC
	if settings != nil && settings.ContributionToken != nil && settings.ContributionToken.RpcUrl != "" {
		rpcURL = settings.ContributionToken.RpcUrl
	}

	return resolveCardManagerAddress(discordUserID, defaultCardManagerAddress, defaultInstanceID, rpcURL)
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
func resolveDiscordToWalletMap(discordUserIDs []string, settings *Settings) map[string]string {
	result := map[string]string{}
	for _, id := range discordUserIDs {
		addr, err := resolveWalletAddress(id, settings)
		if err != nil {
			continue
		}
		if addr != "" {
			result[id] = addr
		}
	}
	return result
}
