// Mint mints a development JWT so that tests, the harness and curl invocations
// can call the APIs without an identity provider (section 6.7: token issuance
// is outside the platform).
//
//	mint -owner team-a -scopes publish,subscribe,status:read -ttl 24h
package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"pubsub/internal/auth"
	"pubsub/internal/config"
)

func main() {
	owner := flag.String("owner", "", "owner reference to put into the token (required)")
	scopes := flag.String("scopes", auth.ScopePublish+","+auth.ScopeSubscribe+","+auth.ScopeStatusRead, "comma separated permissions")
	ttl := flag.Duration("ttl", 24*time.Hour, "token lifetime")
	flag.Parse()
	if *owner == "" {
		fmt.Fprintln(os.Stderr, "mint: -owner is required")
		flag.Usage()
		os.Exit(2)
	}
	token, err := auth.Mint(
		config.MustStr("JWT_SECRET"),
		config.Str("JWT_ISSUER", "dev"),
		config.Str("JWT_AUDIENCE", "pubsub"),
		*owner,
		strings.Split(*scopes, ","),
		*ttl,
	)
	if err != nil {
		fmt.Fprintln(os.Stderr, "mint:", err)
		os.Exit(1)
	}
	fmt.Println(token)
}
