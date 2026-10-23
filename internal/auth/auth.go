// Package auth verifies the JWT of every API call (section 6.7). The token
// carries the owner reference and the permissions; the platform never trusts an
// owner reference from a request body or query parameter.
//
// The spike uses HS256 with a shared secret so that tests and the harness can
// mint tokens without an identity provider. Production uses JWKS against any
// standards-based OIDC issuer.
package auth

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Scopes (permissions) of section 6.7.
const (
	ScopePublish    = "publish"
	ScopeSubscribe  = "subscribe"
	ScopeStatusRead = "status:read"
	ScopeReplay     = "replay"
)

// Claims is the authorised identity of one API call.
type Claims struct {
	Owner  string
	Scopes []string
}

// HasScope reports whether the claims carry the permission.
func (c *Claims) HasScope(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope || s == "*" {
			return true
		}
	}
	return false
}

// Validator verifies bearer tokens.
type Validator struct {
	key      func(*jwt.Token) (any, error)
	issuer   string
	audience string
}

// NewHMAC builds a validator for HS256 tokens signed with a shared secret.
func NewHMAC(secret, issuer, audience string) (*Validator, error) {
	if secret == "" {
		return nil, errors.New("auth: empty JWT secret")
	}
	return &Validator{
		key:      func(*jwt.Token) (any, error) { return []byte(secret), nil },
		issuer:   issuer,
		audience: audience,
	}, nil
}

// Mint issues a development token signed with the shared secret. It stands in
// for the identity provider of section 6.7 during the spike.
func Mint(secret, issuer, audience, owner string, scopes []string, ttl time.Duration) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"iss":   issuer,
		"aud":   audience,
		"sub":   owner,
		"owner": owner,
		"scope": strings.Join(scopes, " "),
		"iat":   now.Unix(),
		"nbf":   now.Unix(),
		"exp":   now.Add(ttl).Unix(),
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return t.SignedString([]byte(secret))
}

// Parse verifies a bearer token and extracts the claims.
func (v *Validator) Parse(token string) (*Claims, error) {
	parsed, err := jwt.Parse(token, v.key,
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(v.issuer),
		jwt.WithAudience(v.audience),
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("auth: invalid token: %w", err)
	}
	if !parsed.Valid {
		return nil, errors.New("auth: invalid token")
	}
	mc, ok := parsed.Claims.(jwt.MapClaims)
	if !ok {
		return nil, errors.New("auth: unexpected claim type")
	}
	c := &Claims{}
	if owner, ok := mc["owner"].(string); ok && owner != "" {
		c.Owner = owner
	} else if sub, ok := mc["sub"].(string); ok {
		c.Owner = sub
	}
	if c.Owner == "" {
		return nil, errors.New("auth: token carries no owner reference")
	}
	switch scope := mc["scope"].(type) {
	case string:
		c.Scopes = strings.Fields(scope)
	case []any:
		for _, s := range scope {
			if str, ok := s.(string); ok {
				c.Scopes = append(c.Scopes, str)
			}
		}
	}
	return c, nil
}

// Authorize extracts the bearer token from the request and checks the scope.
// It writes the error response itself and returns false on failure.
func (v *Validator) Authorize(w http.ResponseWriter, r *http.Request, scope string) (*Claims, bool) {
	h := r.Header.Get("Authorization")
	if h == "" || !strings.HasPrefix(h, "Bearer ") {
		writeError(w, http.StatusUnauthorized, "missing bearer token")
		return nil, false
	}
	claims, err := v.Parse(strings.TrimPrefix(h, "Bearer "))
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return nil, false
	}
	if !claims.HasScope(scope) {
		writeError(w, http.StatusForbidden, fmt.Sprintf("missing scope %q", scope))
		return nil, false
	}
	return claims, true
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	fmt.Fprintf(w, `{"error":%q}`, msg)
}
