// Command heartime operates a local Heartime ledger.
//
// Every command opens the ledger, performs one durable operation and exits, so
// each invocation is also a restart. Output is JSON on stdout.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"powerfarm.dev/heartime"
)

const usage = `usage: heartime [-db PATH] [-at RFC3339-UTC] COMMAND [ARGS]

commands:
  import FILE                               cache exact contract terms (operator admission, not Registry)
  evaluate                                  materialize everything due, then print the account
  account                                   what was due, emitted, may have executed, is unresolved, happens next
  status                                    full ledger for audit
  pending                                   deliveries not yet acknowledged
  attempt ID                                record a delivery attempt before sending
  ack ID                                    record transport acknowledgement (not verification)
  report ID OUTCOME SHA256                  verified | failed | uncertain | contained, bound to evidence
  renew CONTRACT GEN THROUGH REVIEW SHA256  accept a planning return that extends coverage
  pause CONTRACT GEN UNTIL REASON           defer ordinary work delivery
  resume CONTRACT GEN REASON                end a pause
  retire CONTRACT GEN REASON                end future work and planning evaluation
  serve ENDPOINT TOKEN_FILE                 evaluate and deliver continuously (system clock only)`

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, errUsage) {
			fmt.Fprintln(os.Stderr, usage)
			os.Exit(2)
		}
		os.Exit(1)
	}
}

var errUsage = errors.New("invalid command line")

func run(arguments []string) error {
	flags := flag.NewFlagSet("heartime", flag.ContinueOnError)
	database := flags.String("db", "heartime.db", "application-owned ledger path")
	at := flags.String("at", "", "operator-supplied UTC instant (RFC 3339); recorded as such in evidence")
	if err := flags.Parse(arguments); err != nil {
		return errUsage
	}
	args := flags.Args()
	if len(args) == 0 {
		return errUsage
	}
	now := time.Now().UTC().Unix()
	clockSource := heartime.ClockSystemUTC
	if *at != "" {
		instant, err := time.Parse(time.RFC3339, *at)
		if err != nil {
			return fmt.Errorf("%w: -at must be RFC 3339: %v", errUsage, err)
		}
		now, clockSource = instant.UTC().Unix(), heartime.ClockOperatorUTC
	}
	store, err := heartime.Open(*database)
	if err != nil {
		return err
	}
	defer store.Close()
	store.ClockSource = clockSource

	command, params := args[0], args[1:]
	expect := func(count int) error {
		if len(params) != count {
			return fmt.Errorf("%w: %s takes %d argument(s)", errUsage, command, count)
		}
		return nil
	}
	var result any
	switch command {
	case "import":
		if err := expect(1); err != nil {
			return err
		}
		raw, err := os.ReadFile(params[0])
		if err != nil {
			return err
		}
		contract, err := heartime.Parse(raw)
		if err != nil {
			return err
		}
		if err := store.Install(contract, now); err != nil {
			return err
		}
		result = map[string]any{"installed": contract.Metadata, "admission": "trusted-local-operator-cache; not Registry admission"}
	case "evaluate":
		if err := expect(0); err != nil {
			return err
		}
		if err := store.Evaluate(now); err != nil {
			return err
		}
		if result, err = store.Account(now); err != nil {
			return err
		}
	case "account":
		if err := expect(0); err != nil {
			return err
		}
		if result, err = store.Account(now); err != nil {
			return err
		}
	case "status":
		if err := expect(0); err != nil {
			return err
		}
		if result, err = store.Status(); err != nil {
			return err
		}
	case "pending":
		if err := expect(0); err != nil {
			return err
		}
		if result, err = store.Pending(); err != nil {
			return err
		}
	case "attempt":
		if err := expect(1); err != nil {
			return err
		}
		err = store.Attempt(params[0], now)
	case "ack":
		if err := expect(1); err != nil {
			return err
		}
		err = store.Acknowledge(params[0], now)
	case "report":
		if err := expect(3); err != nil {
			return err
		}
		err = store.Report(params[0], heartime.Outcome(params[1]), params[2], now)
	case "renew":
		if err := expect(5); err != nil {
			return err
		}
		contract, err := contractRef(params[0], params[1])
		if err != nil {
			return err
		}
		through, err := heartime.ParseUTC(params[2])
		if err != nil {
			return fmt.Errorf("%w: %v", errUsage, err)
		}
		review, err := heartime.ParseUTC(params[3])
		if err != nil {
			return fmt.Errorf("%w: %v", errUsage, err)
		}
		if err := store.Renew(contract, through, review, params[4], now); err != nil {
			return err
		}
	case "pause":
		if err := expect(4); err != nil {
			return err
		}
		contract, err := contractRef(params[0], params[1])
		if err != nil {
			return err
		}
		until, err := heartime.ParseUTC(params[2])
		if err != nil {
			return fmt.Errorf("%w: %v", errUsage, err)
		}
		if err := store.Pause(contract, until, now, params[3]); err != nil {
			return err
		}
	case "resume", "retire":
		if err := expect(3); err != nil {
			return err
		}
		contract, err := contractRef(params[0], params[1])
		if err != nil {
			return err
		}
		if command == "resume" {
			err = store.Resume(contract, now, params[2])
		} else {
			err = store.Retire(contract, now, params[2])
		}
		if err != nil {
			return err
		}
	case "serve":
		if err := expect(2); err != nil {
			return err
		}
		if *at != "" {
			return fmt.Errorf("%w: serve uses the system clock and refuses -at", errUsage)
		}
		secret, err := os.ReadFile(params[1])
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := store.Run(ctx, params[0], strings.TrimSpace(string(secret))); !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	default:
		return fmt.Errorf("%w: unknown command %q", errUsage, command)
	}
	if err != nil {
		return err
	}
	if result == nil {
		result = map[string]bool{"accepted": true}
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}

func contractRef(id, generation string) (heartime.Ref, error) {
	value, err := strconv.Atoi(generation)
	if err != nil || value < 1 {
		return heartime.Ref{}, fmt.Errorf("%w: generation must be a positive integer", errUsage)
	}
	return heartime.Ref{ID: id, Generation: value}, nil
}
