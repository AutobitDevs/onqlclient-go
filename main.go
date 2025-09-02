package onqlclient

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	// EOM is the End-of-Message character used to delimit messages.
	EOM = 0x04
	// DELIMITER is the character used to separate fields in a message.
	DELIMITER = 0x1E
)

// Response holds the parsed data from a server response.
type Response struct {
	RequestID string
	Source    string
	Payload   string
}

// ONQLClient is an asynchronous, concurrent-safe Go client for the ONQL TCP server.
type ONQLClient struct {
	conn            net.Conn
	reader          *bufio.Reader
	pendingRequests map[string]chan *Response
	mu              sync.Mutex
	wg              sync.WaitGroup
	shutdown        chan struct{}
}

// New creates and connects a new ONQLClient.
// It establishes a TCP connection to the given host and port, and starts a
// background goroutine to handle incoming responses.
func New(host string, port int) (*ONQLClient, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
	}

	log.Printf("Successfully connected to server at %s", addr)

	client := &ONQLClient{
		conn:            conn,
		reader:          bufio.NewReader(conn),
		pendingRequests: make(map[string]chan *Response),
		shutdown:        make(chan struct{}),
	}

	// Start the background reader goroutine.
	client.wg.Add(1)
	go client.responseReaderLoop()

	return client, nil
}

// responseReaderLoop runs in a background goroutine, continuously reading data
// from the server, parsing responses, and forwarding them to the correct waiting request.
func (c *ONQLClient) responseReaderLoop() {
	defer c.wg.Done()
	for {
		select {
		case <-c.shutdown:
			// Close was called, so we exit the loop.
			c.failAllPendingRequests(errors.New("client connection closed"))
			return
		default:
			// Read until the End-of-Message (EOM) byte.
			fullResponseBytes, err := c.reader.ReadBytes(EOM)
			if err != nil {
				if err == io.EOF || errors.Is(err, net.ErrClosed) {
					log.Println("Connection closed by server.")
				} else {
					log.Printf("Error reading from connection: %v", err)
				}
				// Fail any pending requests because the connection is lost.
				c.failAllPendingRequests(errors.New("connection lost"))
				return
			}

			// Process the received message.
			c.processResponse(fullResponseBytes)
		}
	}
}

// processResponse parses the raw byte slice of a response and dispatches it.
func (c *ONQLClient) processResponse(data []byte) {
	// Trim the EOM character from the end.
	trimmedData := strings.TrimSuffix(string(data), string(EOM))
	parts := strings.Split(trimmedData, string(DELIMITER))

	if len(parts) != 3 {
		log.Printf("Received malformed response: %s", trimmedData)
		return
	}

	response := &Response{
		RequestID: parts[0],
		Source:    parts[1],
		Payload:   parts[2],
	}

	c.mu.Lock()
	future, ok := c.pendingRequests[response.RequestID]
	c.mu.Unlock()

	if ok {
		// Send the response to the waiting channel.
		// This is non-blocking because the channel has a buffer of 1.
		future <- response
	} else {
		log.Printf("Received response for unknown or already handled request ID: %s", response.RequestID)
	}
}

// failAllPendingRequests is called when the connection is lost to notify
// all waiting goroutines that their requests have failed.
func (c *ONQLClient) failAllPendingRequests(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for rid, future := range c.pendingRequests {
		// Sending nil indicates an error occurred. The SendRequest function will handle it.
		future <- nil
		delete(c.pendingRequests, rid)
	}
}

// SendRequest sends a request to the server and waits for a response.
// It is safe for concurrent use. A timeout can be specified.
func (c *ONQLClient) SendRequest(keyword, payload string, timeout time.Duration) (*Response, error) {
	requestID, err := generateRequestID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate request ID: %w", err)
	}

	// Create a buffered channel to receive the response.
	// The buffer prevents the reader goroutine from blocking if this function times out.
	future := make(chan *Response, 1)

	c.mu.Lock()
	c.pendingRequests[requestID] = future
	c.mu.Unlock()

	// Ensure the request is removed from the map when the function exits.
	defer func() {
		c.mu.Lock()
		delete(c.pendingRequests, requestID)
		close(future)
		c.mu.Unlock()
	}()

	// Construct and send the message.
	message := fmt.Sprintf("%s%c%s%c%s%c", requestID, DELIMITER, keyword, DELIMITER, payload, EOM)
	_, err = c.conn.Write([]byte(message))
	if err != nil {
		return nil, fmt.Errorf("failed to write request to connection: %w", err)
	}

	// Wait for the response or timeout.
	select {
	case response := <-future:
		if response == nil {
			return nil, errors.New("request failed due to connection error")
		}
		return response, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("request %s timed out after %v", requestID, timeout)
	}
}

// Close gracefully shuts down the client connection and stops the reader goroutine.
func (c *ONQLClient) Close() error {
	// Signal the reader loop to shut down.
	close(c.shutdown)

	// Close the connection. This will unblock the reader if it's waiting for data.
	err := c.conn.Close()

	// Wait for the reader goroutine to finish cleaning up.
	c.wg.Wait()

	log.Println("Connection closed.")
	return err
}

// generateRequestID creates a short, random, hexadecimal string to use as a request ID.
func generateRequestID() (string, error) {
	bytes := make([]byte, 4) // 4 bytes = 8 hex characters
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
