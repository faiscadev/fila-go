package fila

import (
	"context"
	"time"

	"github.com/faisca/fila-go/fibp"
)

// ApiKeyInfo holds information about a created API key.
type ApiKeyInfo struct {
	KeyID        string
	Key          string // Plaintext key, returned only on creation.
	IsSuperadmin bool
}

// CreateApiKey creates a new API key.
// expiresAt is the expiration time; zero means no expiration.
func (c *Client) CreateApiKey(ctx context.Context, name string, expiresAt time.Time, isSuperadmin bool) (*ApiKeyInfo, error) {
	var expiresMs uint64
	if !expiresAt.IsZero() {
		expiresMs = uint64(expiresAt.UnixMilli())
	}
	body, err := fibp.EncodeCreateApiKey(name, expiresMs, isSuperadmin)
	if err != nil {
		return nil, err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeCreateApiKey, body)
	if err != nil {
		return nil, err
	}
	resp, err := fibp.DecodeCreateApiKeyResult(respBody)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return nil, errorCodeToErr(resp.ErrorCode, "", nil)
	}
	return &ApiKeyInfo{
		KeyID:        resp.KeyID,
		Key:          resp.Key,
		IsSuperadmin: resp.IsSuperadmin,
	}, nil
}

// RevokeApiKey revokes an API key by its ID.
func (c *Client) RevokeApiKey(ctx context.Context, keyID string) error {
	body, err := fibp.EncodeRevokeApiKey(keyID)
	if err != nil {
		return err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeRevokeApiKey, body)
	if err != nil {
		return err
	}
	resp, err := fibp.DecodeRevokeApiKeyResult(respBody)
	if err != nil {
		return err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return errorCodeToErr(resp.ErrorCode, "", nil)
	}
	return nil
}

// ApiKeyListEntry holds info about an API key in a list operation.
type ApiKeyListEntry struct {
	KeyID        string
	Name         string
	CreatedAt    time.Time
	ExpiresAt    time.Time // Zero if no expiration.
	IsSuperadmin bool
}

// ListApiKeys lists all API keys.
func (c *Client) ListApiKeys(ctx context.Context) ([]ApiKeyListEntry, error) {
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeListApiKeys, nil)
	if err != nil {
		return nil, err
	}
	resp, err := fibp.DecodeListApiKeysResult(respBody)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return nil, errorCodeToErr(resp.ErrorCode, "", nil)
	}
	entries := make([]ApiKeyListEntry, len(resp.Keys))
	for i, k := range resp.Keys {
		entries[i] = ApiKeyListEntry{
			KeyID:        k.KeyID,
			Name:         k.Name,
			CreatedAt:    time.UnixMilli(int64(k.CreatedAtMs)),
			IsSuperadmin: k.IsSuperadmin,
		}
		if k.ExpiresAtMs > 0 {
			entries[i].ExpiresAt = time.UnixMilli(int64(k.ExpiresAtMs))
		}
	}
	return entries, nil
}

// AclPermission represents a single ACL permission entry.
type AclPermission struct {
	Kind    string // "produce", "consume", or "admin"
	Pattern string // Queue name pattern (e.g., "*", "orders.*")
}

// SetAcl sets ACL permissions for an API key.
func (c *Client) SetAcl(ctx context.Context, keyID string, permissions []AclPermission) error {
	fibpPerms := make([]fibp.AclPermission, len(permissions))
	for i, p := range permissions {
		fibpPerms[i] = fibp.AclPermission{Kind: p.Kind, Pattern: p.Pattern}
	}
	body, err := fibp.EncodeSetAcl(keyID, fibpPerms)
	if err != nil {
		return err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeSetAcl, body)
	if err != nil {
		return err
	}
	resp, err := fibp.DecodeSetAclResult(respBody)
	if err != nil {
		return err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return errorCodeToErr(resp.ErrorCode, "", nil)
	}
	return nil
}

// AclInfo holds ACL information for an API key.
type AclInfo struct {
	KeyID        string
	IsSuperadmin bool
	Permissions  []AclPermission
}

// GetAcl gets ACL permissions for an API key.
func (c *Client) GetAcl(ctx context.Context, keyID string) (*AclInfo, error) {
	body, err := fibp.EncodeGetAcl(keyID)
	if err != nil {
		return nil, err
	}
	_, respBody, err := c.conn.request(ctx, fibp.OpcodeGetAcl, body)
	if err != nil {
		return nil, err
	}
	resp, err := fibp.DecodeGetAclResult(respBody)
	if err != nil {
		return nil, err
	}
	if resp.ErrorCode != fibp.ErrorOk {
		return nil, errorCodeToErr(resp.ErrorCode, "", nil)
	}
	perms := make([]AclPermission, len(resp.Permissions))
	for i, p := range resp.Permissions {
		perms[i] = AclPermission{Kind: p.Kind, Pattern: p.Pattern}
	}
	return &AclInfo{
		KeyID:        resp.KeyID,
		IsSuperadmin: resp.IsSuperadmin,
		Permissions:  perms,
	}, nil
}
