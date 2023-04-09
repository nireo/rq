package http

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/goccy/go-json"
	"github.com/nireo/rq/internal/broker"
	"github.com/nireo/rq/internal/store"
)

type Server struct {
	broker broker.Broker
}

type httpErr string

const (
	errInvalidTopicValue = httpErr("invalid topic value")
	errReadBody          = httpErr("error reading the request body")
	errPublish           = httpErr("error publishing to broker")
	errNextValue         = httpErr("error getting next value for consumer")
	errAck               = httpErr("error ACKing message")
	errNack              = httpErr("error NACKing message")
	errDecodingCmd       = httpErr("error decoding command")
	errRequestCancelled  = httpErr("request context cancelled")
	errPurge             = httpErr("failed to purge topic")
)

func (e httpErr) Error() string {
	return string(e)
}

type flushWriter struct {
	flusher http.Flusher
	writer  io.Writer
}

func newFlushWriter(w io.Writer) flushWriter {
	fw := flushWriter{writer: w}
	if f, ok := w.(http.Flusher); ok {
		fw.flusher = f
	}
	return fw
}

func (f flushWriter) Write(data []byte) (int, error) {
	n, err := f.writer.Write(data)
	if f.flusher != nil {
		f.flusher.Flush()
	}

	return n, err
}

func (s *Server) Publish(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "no topic provided", http.StatusBadRequest)
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error reading request body", http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	val := store.NewValue(b)
	if err := s.broker.Publish(topic, val); err != nil {
		http.Error(w, "error publishing topic", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (s *Server) Subscribe(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "no topic provided", http.StatusBadRequest)
		return
	}

	csm := s.broker.Subscribe()
	encoder, decoder := json.NewEncoder(newFlushWriter(w)), json.NewDecoder(r.Body)

	for {
		var cmd string
		if err := decoder.Decode(&cmd); isDisconnect(err) {
		}
	}
}

func isDisconnect(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "client disconnected") ||
		strings.Contains(err.Error(), "; CANCEL") ||
		errors.Is(err, io.EOF))
}
