package main

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"html"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// "We're waking up the server", by language tag. Lookup goes full tag first (pt-pt, zh-tw),
// then the base language; anything else gets English.
var wakingUp = map[string]string{
	"en":      "We’re waking up the server",
	"es":      "Estamos despertando el servidor",
	"ca":      "Estem despertant el servidor",
	"eu":      "Zerbitzaria esnatzen ari gara",
	"gl":      "Estamos espertando o servidor",
	"pt":      "Estamos acordando o servidor",
	"pt-pt":   "Estamos a acordar o servidor",
	"it":      "Stiamo risvegliando il server",
	"fr":      "Nous réveillons le serveur",
	"de":      "Wir wecken den Server auf",
	"nl":      "We maken de server wakker",
	"pl":      "Wybudzamy serwer",
	"cs":      "Probouzíme server",
	"sk":      "Prebúdzame server",
	"ro":      "Trezim serverul",
	"hu":      "Felébresztjük a szervert",
	"sv":      "Vi väcker servern",
	"da":      "Vi vækker serveren",
	"nb":      "Vi vekker serveren",
	"fi":      "Herätämme palvelinta",
	"el":      "Ξυπνάμε τον διακομιστή",
	"tr":      "Sunucuyu uyandırıyoruz",
	"ru":      "Пробуждаем сервер",
	"uk":      "Пробуджуємо сервер",
	"ar":      "نوقظ الخادم الآن",
	"he":      "אנחנו מעירים את השרת",
	"hi":      "हम सर्वर को जगा रहे हैं",
	"ja":      "サーバーを起動しています",
	"zh":      "正在唤醒服务器",
	"zh-hant": "正在喚醒伺服器",
	"ko":      "서버를 깨우는 중입니다",
	"id":      "Kami sedang membangunkan server",
	"vi":      "Đang đánh thức máy chủ",
	"th":      "กำลังปลุกเซิร์ฟเวอร์",
}

// Tags that name the same text under another key.
var langAlias = map[string]string{
	"no": "nb", "nn": "nb",
	"zh-tw": "zh-hant", "zh-hk": "zh-hant", "zh-mo": "zh-hant",
	"zh-cn": "zh", "zh-sg": "zh", "zh-hans": "zh",
	"iw": "he", "in": "id",
}

var rtl = map[string]bool{"ar": true, "he": true}

// pickLang returns the best supported key for an Accept-Language header ("en" if none).
func pickLang(accept string) string {
	type cand struct {
		tag string
		q   float64
		i   int
	}
	var cs []cand
	for i, part := range strings.Split(accept, ",") {
		tag, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		tag = strings.ToLower(strings.TrimSpace(tag))
		if tag == "" || tag == "*" {
			continue
		}
		q := 1.0
		if v, ok := strings.CutPrefix(strings.TrimSpace(params), "q="); ok {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				q = f
			}
		}
		if q > 0 {
			cs = append(cs, cand{tag, q, i})
		}
	}
	sort.SliceStable(cs, func(a, b int) bool { return cs[a].q > cs[b].q })
	resolve := func(t string) (string, bool) {
		if a, ok := langAlias[t]; ok {
			t = a
		}
		_, ok := wakingUp[t]
		return t, ok
	}
	for _, c := range cs {
		// zh-Hant-TW → zh-hant; pt-PT → pt-pt; es-419 → es.
		parts := strings.Split(c.tag, "-")
		for n := len(parts); n >= 1; n-- {
			if k, ok := resolve(strings.Join(parts[:n], "-")); ok {
				return k
			}
		}
	}
	return "en"
}

// uuidv7 is time-ordered: the first 48 bits are Unix milliseconds, so ids sort by when the
// request arrived and line up with the logs.
// waitID is the request id shown on the waiting page. The page reloads every 2 s while the app
// wakes, and each reload is a new request; a short-lived cookie keeps one id for the whole wait,
// so the id a visitor reports matches one "waiting page" line in the log.
func waitID(w http.ResponseWriter, r *http.Request, now time.Time) (id string, isNew bool) {
	if c, err := r.Cookie(waitCookie); err == nil && validUUID(c.Value) {
		return c.Value, false
	}
	id = uuidv7(now)
	http.SetCookie(w, &http.Cookie{Name: waitCookie, Value: id, Path: "/", MaxAge: 60,
		HttpOnly: true, Secure: r.Header.Get("X-Forwarded-Proto") == "https", SameSite: http.SameSiteLaxMode})
	return id, true
}

const waitCookie = "woc_rid"

func validUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
				return false
			}
		}
	}
	return true
}

func uuidv7(now time.Time) string {
	var b [16]byte
	_, _ = rand.Read(b[6:])
	binary.BigEndian.PutUint64(b[:8], uint64(now.UnixMilli())<<16|uint64(binary.BigEndian.Uint16(b[6:8])))
	b[6] = b[6]&0x0f | 0x70 // version 7
	b[8] = b[8]&0x3f | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// writeStartingPage answers 503 + Retry-After while an app wakes, and 200 for the preview.
func writeStartingPage(w http.ResponseWriter, r *http.Request, status int, brand, node, reqID string) {
	lang := pickLang(r.Header.Get("Accept-Language"))
	title := html.EscapeString(wakingUp[lang])
	dir := "ltr"
	if rtl[lang] {
		dir = "rtl"
	}
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Content-Language", lang)
	h.Set("Cache-Control", "no-store")
	if status == http.StatusServiceUnavailable {
		h.Set("Retry-After", "2")
	}
	h.Set("Vary", "Accept-Language")
	h.Set("X-Request-Id", reqID)
	w.WriteHeader(status)
	// While an app wakes, the page reloads itself until the app answers. The preview doesn't.
	refresh := ""
	if status == http.StatusServiceUnavailable {
		refresh = `<meta http-equiv="refresh" content="2">`
	}
	fmt.Fprintf(w, startingPage, lang, dir, refresh, title, title, html.EscapeString(brand), html.EscapeString(node), html.EscapeString(reqID))
}

// Waiting page in the spirit of Vercel's checkpoint: one centred line and a monospace footer
// with the node and the request id. It reloads itself every 2 s until the app answers.
const startingPage = `<!doctype html>
<html lang="%s" dir="%s">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
%s
<meta name="robots" content="noindex">
<title>%s</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Geist:wght@500&family=Geist+Mono:wght@400&display=swap" rel="stylesheet">
<style>
:root{color-scheme:light dark;--bg:#fff;--fg:#171717;--muted:#666}
@media (prefers-color-scheme:dark){:root{--bg:#0a0a0a;--fg:#ededed;--muted:#a1a1a1}}
*{box-sizing:border-box}
html,body{height:100%%;margin:0}
body{display:flex;flex-direction:column;background:var(--bg);color:var(--fg);
  font-family:Geist,system-ui,-apple-system,"Segoe UI",sans-serif;-webkit-font-smoothing:antialiased}
main{flex:1;display:grid;place-items:center;padding:24px 16px}
h1{margin:0;font-size:1.5rem;font-weight:500;letter-spacing:-.015em;line-height:1.3;text-align:center;text-wrap:balance}
.dots{display:inline-flex;gap:.18em;margin-inline-start:.2em}
.dots i{width:.18em;height:.18em;border-radius:50%%;background:currentColor;opacity:.25;animation:p 1.2s infinite ease-in-out}
.dots i:nth-child(2){animation-delay:.2s}.dots i:nth-child(3){animation-delay:.4s}
@keyframes p{0%%,80%%,100%%{opacity:.25}40%%{opacity:1}}
@media (prefers-reduced-motion:reduce){.dots i{animation:none;opacity:.6}}
footer{padding:0 16px 28px;text-align:center;color:var(--muted);
  font:400 .8125rem/1.6 "Geist Mono",ui-monospace,"SF Mono",Menlo,monospace;overflow-wrap:anywhere}
footer .sep{margin:0 .6em;opacity:.6}
</style>
</head>
<body>
<main><h1>%s<span class="dots" aria-hidden="true"><i></i><i></i><i></i></span></h1></main>
<footer dir="ltr">%s<span class="sep">|</span>%s::%s</footer>
</body>
</html>`
