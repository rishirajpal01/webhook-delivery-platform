// Command webhookctl calls the webhook-delivery-platform API from the
// terminal. It is a thin layer over the Go SDK (package webhooks).
//
//	webhookctl apps create acme
//	webhookctl endpoints create app_… https://acme.example/hooks -events order.paid
//	webhookctl send app_… order.paid '{"order_id":"981"}'
//	webhookctl deliveries app_… msg_… -wait 1m
//
// Output is JSON; -o takes a Go template instead, such as -o '{{.id}}'.
// Run webhookctl -h for every command.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
	"time"

	"github.com/rishirajpal01/webhook-delivery-platform/webhooks"
)

const usage = `Usage: webhookctl [-api URL] [-key KEY] [-o TEMPLATE] COMMAND ARGS [FLAGS]

Commands:
  apps create NAME [-idempotency-key K]
  apps get APP
  endpoints create APP URL [-events a,b] [-secret whsec_…] [-max-in-flight N] [-rate-limit N] [-idempotency-key K]
  endpoints list APP [-limit N] [-cursor C]
  endpoints get APP EP
  endpoints update APP EP [-url U] [-events a,b] [-status active|paused] [-max-in-flight N] [-rate-limit N]
  endpoints delete APP EP
  endpoints enable APP EP
  endpoints rotate-secret APP EP [-secret whsec_…]
  endpoints replay APP EP -since TIME   (RFC 3339, or a duration such as 2h meaning that long ago)
  send APP EVENT_TYPE PAYLOAD_JSON [-idempotency-key K]
  messages APP [-limit N] [-cursor C]   (newest first)
  deliveries APP MSG [-limit N] [-cursor C] [-wait DURATION]
  attempts APP DEL [-limit N] [-cursor C]
  replay APP DEL

-wait polls until no delivery is pending or in flight, and exits 1 unless
every delivery succeeded.

Environment: WEBHOOKS_API (default http://localhost:8080), API_KEY.
`

func main() {
	os.Exit(run(context.Background(), os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

// errUsage means the command line was wrong; usage is printed.
var errUsage = errors.New("usage")

func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	global := flag.NewFlagSet("webhookctl", flag.ContinueOnError)
	global.SetOutput(stderr)
	global.Usage = func() { fmt.Fprint(stderr, usage) }
	apiURL := global.String("api", or(getenv("WEBHOOKS_API"), "http://localhost:8080"), "API base URL")
	key := global.String("key", getenv("API_KEY"), "API key")
	tmpl := global.String("o", "", "Go template for the output, such as '{{.id}}'")
	if err := global.Parse(args); err != nil {
		return 2
	}
	if global.NArg() == 0 {
		global.Usage()
		return 2
	}

	out := output{w: stdout}
	if *tmpl != "" {
		t, err := template.New("o").Option("missingkey=error").Parse(*tmpl + "\n")
		if err != nil {
			fmt.Fprintln(stderr, "webhookctl: -o:", err)
			return 2
		}
		out.tmpl = t
	}
	c := webhooks.NewClient(*apiURL, *key)

	code, err := dispatch(ctx, c, out, global.Args())
	if errors.Is(err, errUsage) {
		global.Usage()
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "webhookctl:", err)
		return 1
	}
	return code
}

// dispatch runs one command and returns its exit code.
func dispatch(ctx context.Context, c *webhooks.Client, out output, args []string) (int, error) {
	cmd, args := args[0], args[1:]
	if (cmd == "apps" || cmd == "endpoints") && len(args) > 0 {
		cmd, args = cmd+" "+args[0], args[1:]
	}
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var (
		idemKey     = fs.String("idempotency-key", "", "")
		events      = fs.String("events", "", "")
		secret      = fs.String("secret", "", "")
		maxInFlight = fs.Int("max-in-flight", 0, "")
		rateLimit   = fs.Int("rate-limit", 0, "")
		urlFlag     = fs.String("url", "", "")
		status      = fs.String("status", "", "")
		since       = fs.String("since", "", "")
		limit       = fs.Int("limit", 0, "")
		cursor      = fs.String("cursor", "", "")
		wait        = fs.Duration("wait", 0, "")
	)
	pos, err := parse(fs, args)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", cmd, err)
	}
	want := func(n int) error {
		if len(pos) != n {
			return errUsage
		}
		return nil
	}
	page := webhooks.ListOptions{Limit: *limit, Cursor: *cursor}

	switch cmd {
	case "apps create":
		if err := want(1); err != nil {
			return 0, err
		}
		return out.print(c.CreateApp(ctx, webhooks.CreateAppRequest{Name: pos[0], IdempotencyKey: *idemKey}))
	case "apps get":
		if err := want(1); err != nil {
			return 0, err
		}
		return out.print(c.GetApp(ctx, pos[0]))
	case "endpoints create":
		if err := want(2); err != nil {
			return 0, err
		}
		return out.print(c.CreateEndpoint(ctx, pos[0], webhooks.CreateEndpointRequest{
			URL: pos[1], EventTypes: split(*events), Secret: *secret,
			MaxInFlight: *maxInFlight, RateLimit: *rateLimit, IdempotencyKey: *idemKey,
		}))
	case "endpoints list":
		if err := want(1); err != nil {
			return 0, err
		}
		return out.print(c.ListEndpoints(ctx, pos[0], page))
	case "endpoints get":
		if err := want(2); err != nil {
			return 0, err
		}
		return out.print(c.GetEndpoint(ctx, pos[0], pos[1]))
	case "endpoints update":
		if err := want(2); err != nil {
			return 0, err
		}
		var req webhooks.UpdateEndpointRequest
		fs.Visit(func(f *flag.Flag) { // only the flags given change anything
			switch f.Name {
			case "url":
				req.URL = urlFlag
			case "events":
				req.EventTypes = webhooks.Ptr(split(*events))
			case "status":
				req.Status = status
			case "max-in-flight":
				req.MaxInFlight = maxInFlight
			case "rate-limit":
				req.RateLimit = rateLimit
			}
		})
		return out.print(c.UpdateEndpoint(ctx, pos[0], pos[1], req))
	case "endpoints delete":
		if err := want(2); err != nil {
			return 0, err
		}
		return 0, c.DeleteEndpoint(ctx, pos[0], pos[1])
	case "endpoints enable":
		if err := want(2); err != nil {
			return 0, err
		}
		return out.print(c.EnableEndpoint(ctx, pos[0], pos[1]))
	case "endpoints rotate-secret":
		if err := want(2); err != nil {
			return 0, err
		}
		return out.print(c.RotateSecret(ctx, pos[0], pos[1], webhooks.RotateSecretRequest{Secret: *secret}))
	case "endpoints replay":
		if err := want(2); err != nil {
			return 0, err
		}
		t, err := parseSince(*since, time.Now())
		if err != nil {
			return 0, err
		}
		n, err := c.ReplayEndpoint(ctx, pos[0], pos[1], t)
		return out.print(map[string]int{"replayed": n}, err)
	case "send":
		if err := want(3); err != nil {
			return 0, err
		}
		if !json.Valid([]byte(pos[2])) {
			return 0, errors.New("send: PAYLOAD_JSON is not valid JSON")
		}
		return out.print(c.SendMessage(ctx, pos[0], webhooks.SendMessageRequest{
			EventType: pos[1], Payload: json.RawMessage(pos[2]), IdempotencyKey: *idemKey,
		}))
	case "messages":
		if err := want(1); err != nil {
			return 0, err
		}
		return out.print(c.ListMessages(ctx, pos[0], page))
	case "deliveries":
		if err := want(2); err != nil {
			return 0, err
		}
		if *wait > 0 {
			return waitForDeliveries(ctx, c, out, pos[0], pos[1], *wait)
		}
		return out.print(c.ListDeliveries(ctx, pos[0], pos[1], page))
	case "attempts":
		if err := want(2); err != nil {
			return 0, err
		}
		return out.print(c.ListAttempts(ctx, pos[0], pos[1], page))
	case "replay":
		if err := want(2); err != nil {
			return 0, err
		}
		return out.print(c.ReplayDelivery(ctx, pos[0], pos[1]))
	}
	return 0, errUsage
}

// waitForDeliveries polls a message's deliveries until none is pending or
// in flight, prints them, and returns 0 only if every one succeeded.
func waitForDeliveries(ctx context.Context, c *webhooks.Client, out output, appID, msgID string, wait time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	for {
		ds, err := c.ListDeliveries(ctx, appID, msgID, webhooks.ListOptions{Limit: 100})
		if err != nil {
			return 0, err
		}
		done, ok := true, true
		for _, d := range ds.Data {
			done = done && (d.State == "succeeded" || d.State == "dead")
			ok = ok && d.State == "succeeded"
		}
		if done {
			if _, err := out.print(ds, nil); err != nil {
				return 0, err
			}
			if !ok {
				return 1, nil
			}
			return 0, nil
		}
		select {
		case <-ctx.Done():
			out.print(ds, nil)
			return 0, fmt.Errorf("deliveries still pending after %v", wait)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// parse reads flags that may come before, between or after the positional
// arguments, and returns the positional ones.
func parse(fs *flag.FlagSet, args []string) ([]string, error) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		if fs.NArg() == 0 {
			return pos, nil
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

// parseSince takes an RFC 3339 time, or a duration meaning that long before
// now.
func parseSince(s string, now time.Time) (time.Time, error) {
	if s == "" {
		return time.Time{}, errors.New("endpoints replay: -since is required")
	}
	if d, err := time.ParseDuration(s); err == nil {
		return now.Add(-d), nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, errors.New("endpoints replay: -since must be an RFC 3339 time or a duration such as 2h")
	}
	return t, nil
}

func split(s string) []string {
	if s == "" {
		return []string{}
	}
	return strings.Split(s, ",")
}

func or(v, fallback string) string {
	if v != "" {
		return v
	}
	return fallback
}

// output prints results as indented JSON, or through the -o template.
type output struct {
	w    io.Writer
	tmpl *template.Template
}

// print writes v unless err is set, and returns err for the caller to
// report.
func (o output) print(v any, err error) (int, error) {
	if err != nil {
		return 0, err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return 0, err
	}
	if o.tmpl == nil {
		_, err = fmt.Fprintf(o.w, "%s\n", b)
		return 0, err
	}
	var data any // the template sees the JSON field names, as in the API docs
	if err := json.Unmarshal(b, &data); err != nil {
		return 0, err
	}
	return 0, o.tmpl.Execute(o.w, data)
}
