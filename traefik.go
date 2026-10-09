package main

import (
	"fmt"
	"sort"
	"strings"
)

// Traefik dynamic configuration served to its HTTP provider. One router per group, all
// pointing at woc's proxy port; woc then picks the container by Host.
type traefikDynamic struct {
	HTTP traefikHTTP `json:"http"`
}

type traefikHTTP struct {
	Routers  map[string]traefikRouter  `json:"routers"`
	Services map[string]traefikService `json:"services"`
}

type traefikRouter struct {
	Rule        string            `json:"rule"`
	EntryPoints []string          `json:"entryPoints"`
	Service     string            `json:"service"`
	Middlewares []string          `json:"middlewares,omitempty"`
	TLS         map[string]string `json:"tls"`
}

type traefikService struct {
	LoadBalancer struct {
		Servers        []map[string]string `json:"servers"`
		PassHostHeader bool                `json:"passHostHeader"`
	} `json:"loadBalancer"`
}

func (e *engine) traefikConfig(cfg config) traefikDynamic {
	var svc traefikService
	svc.LoadBalancer.Servers = []map[string]string{{"url": cfg.selfURL}}
	svc.LoadBalancer.PassHostHeader = true
	out := traefikDynamic{HTTP: traefikHTTP{
		Routers:  map[string]traefikRouter{},
		Services: map[string]traefikService{"woc": svc},
	}}
	e.mu.RLock()
	defer e.mu.RUnlock()
	byGroup := map[*group][]string{}
	for h, r := range e.routes {
		byGroup[r.group] = append(byGroup[r.group], h)
	}
	for g, hosts := range byGroup {
		sort.Strings(hosts)
		rules := make([]string, len(hosts))
		for i, h := range hosts {
			rules[i] = fmt.Sprintf("Host(`%s`)", h)
		}
		out.HTTP.Routers["woc-"+routerName(g.name)] = traefikRouter{
			Rule:        strings.Join(rules, " || "),
			EntryPoints: []string{cfg.entryPoint},
			Service:     "woc",
			Middlewares: g.middlewares,
			TLS:         map[string]string{"certResolver": cfg.certResolver},
		}
	}
	return out
}

// routerName keeps what Traefik accepts in a router name.
func routerName(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, s)
}
